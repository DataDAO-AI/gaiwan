# models.py
"""Data models for Twitter archive processing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Set, List
import re
from time import strptime, mktime
import os

@dataclass
class TweetMetadata:
    """Metadata extracted from tweet entities."""
    mentioned_users: Set[str] = field(default_factory=set)
    hashtags: Set[str] = field(default_factory=set)
    urls: Set[str] = field(default_factory=set)  # Expanded URLs
    quoted_tweet_id: Optional[str] = None
    is_retweet: bool = False
    retweet_of_id: Optional[str] = None
    media: List[dict] = field(default_factory=list)  # Raw media entities

    @classmethod
    def extract_from_text_and_entities(cls, text: str, entities: dict) -> 'TweetMetadata':
        """Extract metadata from both tweet text and entities."""
        metadata = cls()
        
        # Extract from text first
        if text:
            # Check for retweet pattern
            if re.search(r'RT\s*@\w+[:\s]', text, re.IGNORECASE):
                metadata.is_retweet = True
                rt_match = re.search(r'RT\s*@(\w+)[:\s]', text, re.IGNORECASE)
                if rt_match:
                    metadata.retweet_of_id = rt_match.group(1)

            # Check for quoted tweet
            if '/status/' in text:
                quoted_match = re.search(r'/status/(\d+)', text)
                if quoted_match:
                    metadata.quoted_tweet_id = quoted_match.group(1)

        # Extract from entities
        if entities:
            # User mentions
            for mention in entities.get('user_mentions', []):
                metadata.mentioned_users.add(mention['screen_name'].lower())

            # Hashtags
            for tag in entities.get('hashtags', []):
                metadata.hashtags.add(tag['text'].lower())

            # URLs
            for url in entities.get('urls', []):
                expanded_url = url.get('expanded_url')
                if expanded_url:
                    metadata.urls.add(expanded_url.split('?')[0])  # Remove query params

            # Media
            metadata.media.extend(entities.get('media', []))

        return metadata

    @classmethod
    def extract_from_note_core(cls, core: dict) -> 'TweetMetadata':
        """Extract metadata from note tweet core data."""
        metadata = cls()
        
        # Extract mentions
        for mention in core.get('mentions', []):
            metadata.mentioned_users.add(mention['screenName'].lower())

        # Extract hashtags
        for tag in core.get('hashtags', []):
            metadata.hashtags.add(tag['text'].lower())

        # Extract URLs
        for url in core.get('urls', []):
            expanded_url = url.get('expanded_url')
            if expanded_url:
                metadata.urls.add(expanded_url.split('?')[0])

        return metadata

@dataclass
class CanonicalTweet:
    """Normalized tweet representation."""
    id: str  # id_str from tweet or noteTweetId
    text: str  # full_text from tweet or text from noteTweet.core
    screen_name: str  # user.screen_name or derived from archive
    created_at: datetime  # parsed from created_at or createdAt
    user_id: Optional[str] = None  # user.id_str when available
    reply_to_tweet_id: Optional[str] = None
    metadata: TweetMetadata = field(default_factory=TweetMetadata)
    quoted_tweet_id: Optional[str] = None
    liked_by: Set[str] = field(default_factory=set)  # Set of screen_names
    source_type: str = "tweet"  # "tweet", "community_tweet", "note_tweet", or "like"
    media_urls: Set[str] = field(default_factory=set)
    media_files: Set[str] = field(default_factory=set)
    community_id: Optional[str] = None  # Only for community tweets

    @classmethod
    def from_tweet_data(cls, tweet_data: dict, default_screen_name: str) -> 'CanonicalTweet':
        """Create from regular or community tweet data."""
        if 'tweet' in tweet_data:
            tweet_data = tweet_data['tweet']

        user_data = tweet_data.get('user', {})
        screen_name = user_data.get('screen_name', default_screen_name).lower()
        
        return cls(
            id=tweet_data['id_str'],
            text=tweet_data.get('full_text', tweet_data.get('text', '')),
            screen_name=screen_name,
            created_at=parse_twitter_timestamp(tweet_data['created_at']),
            user_id=user_data.get('id_str'),
            reply_to_tweet_id=tweet_data.get('in_reply_to_status_id_str'),
            metadata=TweetMetadata.extract_from_text_and_entities(
                tweet_data.get('full_text', ''),
                tweet_data.get('entities', {})
            ),
            community_id=tweet_data.get('community_id_str'),
            source_type='community_tweet' if 'community_id_str' in tweet_data else 'tweet'
        )

    @classmethod
    def from_note_data(cls, note_data: dict, default_screen_name: str) -> 'CanonicalTweet':
        """Create from note tweet data."""
        core = note_data.get('core', {})
        return cls(
            id=note_data['noteTweetId'],
            text=core.get('text', ''),
            screen_name=default_screen_name.lower(),
            created_at=parse_twitter_timestamp(note_data['createdAt']),
            metadata=TweetMetadata.extract_from_note_core(core),
            source_type='note_tweet'
        )

    @classmethod
    def from_like_data(cls, like_data: dict, user_id: str) -> 'CanonicalTweet':
        """Create from like data."""
        # The schema shows likes are nested under a 'like' key
        if 'like' in like_data:
            like_data = like_data['like']
        
        # Schema defines exactly these fields:
        tweet_id = like_data['tweetId']
        text = like_data['fullText']
        expanded_url = like_data['expandedUrl']
        
        # Create timestamp from URL if possible
        created_at = None
        if expanded_url:
            url_match = re.search(r'/status/\d+/(\d{4}/\d{2}/\d{2})', expanded_url)
            if url_match:
                try:
                    created_at = datetime.strptime(
                        url_match.group(1), 
                        '%Y/%m/%d'
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

        return cls(
            id=tweet_id,
            text=text,
            screen_name=user_id.lower(),
            created_at=created_at or datetime.now(timezone.utc),
            source_type="like"
        )

    @classmethod
    def from_archive_tweet(cls, tweet_data: dict, media_dir: str) -> 'CanonicalTweet':
        """Enhanced factory method for archive tweets"""
        # Basic tweet parsing
        tweet = cls.from_tweet_data(tweet_data)
        
        # Add media handling
        if 'extended_entities' in tweet_data:
            for media in tweet_data['extended_entities'].get('media', []):
                tweet.media_urls.add(media['media_url'])
                
                # Handle local files
                media_file = os.path.join(media_dir, f"{tweet.id}_{media['id']}")
                if os.path.exists(media_file):
                    tweet.media_files.add(media_file)
                    
        return tweet

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "screen_name": self.screen_name,
            "user_id": self.user_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "text": self.text,
            "reply_to_tweet_id": self.reply_to_tweet_id,
            "liked_by": list(self.liked_by),
            "source_type": self.source_type,
            "metadata": {
                "mentioned_users": list(self.metadata.mentioned_users),
                "hashtags": list(self.metadata.hashtags),
                "urls": list(self.metadata.urls),
                "quoted_tweet_id": self.metadata.quoted_tweet_id,
                "is_retweet": self.metadata.is_retweet,
                "retweet_of_id": self.metadata.retweet_of_id
            },
            "media_urls": list(self.media_urls),
            "media_files": list(self.media_files),
            "community_id": self.community_id
        }

@dataclass
class MixPRConfig:
    """Configuration for MixPR retrieval."""
    local_alpha: float = 0.6
    similarity_threshold: float = 0.27
    max_iterations: int = 18
    min_df: int = 2
    max_df: float = 0.95
    batch_size: int = 1000
    graph_weight: float = 0.3  # Weight of graph relationships vs text similarity
    reply_weight: float = 1.0  # Weight of reply edges
    quote_weight: float = 0.8  # Weight of quote tweet edges
    user_similarity_weight: float = 0.4  # Weight for user similarity edges
    sibling_weight: float = 0.5  # Weight for sibling tweets (replies to same parent)
    conversation_weight: float = 0.4  # Weight for conversation context vs basic similarity

@dataclass
class RetrievalResult:
    """Represents a retrieval result with relevance score."""
    tweet: CanonicalTweet
    score: float

@dataclass
class UserSimilarityConfig:
    """Configuration for user similarity calculations."""
    min_tweets_per_user: int = 5
    min_likes_per_user: int = 3
    mention_weight: float = 0.7
    reply_weight: float = 0.8
    quote_weight: float = 0.6
    like_weight: float = 0.7
    retweet_weight: float = 0.8
    conversation_weight: float = 0.9
    temporal_weight: float = 0.5
    mutual_follow_weight: float = 0.8
    ncd_threshold: float = 0.7

def parse_twitter_timestamp(ts: str) -> Optional[datetime]:
    """Convert Twitter's timestamp format to datetime."""
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except ValueError:
        try:
            time_struct = strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
            return datetime.fromtimestamp(mktime(time_struct), timezone.utc)
        except ValueError:
            return None
