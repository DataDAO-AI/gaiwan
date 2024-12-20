# models.py
"""Data models for Twitter archive processing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Set
import re
from time import strptime, mktime

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

        if text.startswith('RT @'):
            metadata.is_retweet = True
            rt_match = re.search(r'RT @(\w+):', text)
            if rt_match:
                metadata.retweet_of_id = rt_match.group(1).lower()

        return metadata

@dataclass
class CanonicalTweet:
    """Normalized tweet representation."""
    id: str
    text: str
    author_id: Optional[str] = None
    created_at: Optional[datetime] = None
    reply_to_tweet_id: Optional[str] = None
    liked_by: Set[str] = field(default_factory=set)
    source_type: str = "tweet"
    metadata: TweetMetadata = field(default_factory=TweetMetadata)

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
                    return datetime.fromtimestamp(mktime(time_struct), timezone.UTC)
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

@dataclass
class RetrievalResult:
    """Represents a retrieval result with relevance score."""
    tweet: CanonicalTweet
    score: float
