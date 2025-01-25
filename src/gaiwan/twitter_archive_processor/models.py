from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set, Dict, Any

@dataclass
class TweetMetadata:
    """Contains metadata about a tweet."""
    tweet_type: str
    raw_data: Dict[str, Any]
    urls: Set[str]
    mentioned_users: Set[str] = None
    hashtags: Set[str] = None
    quoted_tweet_id: Optional[str] = None
    is_retweet: bool = False
    retweet_of_id: Optional[str] = None

@dataclass
class CanonicalTweet:
    """Standard format for tweets across the application."""
    id: str
    text: str
    author_id: str
    created_at: datetime
    reply_to_tweet_id: Optional[str]
    metadata: TweetMetadata
    liked_by: Set[str] 