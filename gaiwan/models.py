# models.py
"""Data models for Twitter archive processing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Set
import re
from time import strptime, mktime
import os

@dataclass
class TweetMetadata:
    """Metadata extracted from tweet content."""
    mentioned_users: Set[str] = field(default_factory=set)
    hashtags: Set[str] = field(default_factory=set)
    urls: Set[str] = field(default_factory=set)
    quoted_tweet_id: Optional[str] = None
    is_retweet: bool = False
    retweet_of_id: Optional[str] = None

    @classmethod
    def extract_from_text(cls, text: str) -> 'TweetMetadata':
        """Extract metadata from tweet text using pattern matching."""
        metadata = cls()
        
        # Check for retweet pattern more flexibly
        if re.search(r'RT\s*@\w+[:\s]', text, re.IGNORECASE):  # Added re.IGNORECASE flag
            metadata.is_retweet = True
            rt_match = re.search(r'RT\s*@(\w+)[:\s]', text, re.IGNORECASE)
            if rt_match:
                metadata.retweet_of_id = rt_match.group(1)
        
        metadata.mentioned_users.update(
            username.lower() for username in re.findall(r'@(\w+)', text)
        )
        metadata.hashtags.update(
            tag.lower() for tag in re.findall(r'#(\w+)', text)
        )
        metadata.urls.update(
            url.split('?')[0] for url in re.findall(r'https?://[^\s]+', text)
        )

        if '/status/' in text:
            quoted_match = re.search(r'/status/(\d+)', text)
            if quoted_match:
                metadata.quoted_tweet_id = quoted_match.group(1)

        return metadata

@dataclass
class CanonicalTweet:
    """Normalized tweet representation."""
    id: str
    text: str
    author_id: str
    created_at: datetime
    reply_to_tweet_id: Optional[str] = None
    metadata: TweetMetadata = field(default_factory=TweetMetadata)
    quoted_tweet_id: Optional[str] = None
    liked_by: Set[str] = field(default_factory=set)
    source_type: str = "tweet"
    media_urls: Set[str] = field(default_factory=set)
    media_files: Set[str] = field(default_factory=set)

    @classmethod
    def from_tweet_data(cls, tweet_data: dict, user_id: str) -> 'CanonicalTweet':
        """Create from raw tweet data."""
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

        return cls(
            id=tweet_data['id_str'],
            author_id=user_id,
            created_at=parse_twitter_timestamp(tweet_data['created_at']),
            text=tweet_data['full_text'],
            reply_to_tweet_id=tweet_data.get('in_reply_to_status_id_str'),
            metadata=TweetMetadata.extract_from_text(tweet_data['full_text'])
        )

    @classmethod
    def from_like_data(cls, like_data: dict, user_id: str) -> 'CanonicalTweet':
        """Create from like data."""
        tweet_id = like_data.get('tweetId') or like_data.get('tweet_id')
        text = like_data.get('fullText', like_data.get('full_text', ''))

        created_at = None
        if 'expandedUrl' in like_data:
            url_match = re.search(
                r'/status/\d+/(\d{4}/\d{2}/\d{2})',
                like_data['expandedUrl']
            )
            if url_match:
                try:
                    created_at = datetime.strptime(
                        url_match.group(1),
                        '%Y/%m/%d'
                    ).replace(tzinfo=datetime.UTC)
                except ValueError:
                    pass

        return cls(
            id=tweet_id,
            text=text,
            created_at=created_at,
            liked_by={user_id},
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
            "author_id": self.author_id,
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
            }
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
