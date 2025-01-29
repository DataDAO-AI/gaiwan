from dataclasses import dataclass
from typing import Dict, Set, Any

@dataclass
class TweetMetadata:
    """Contains metadata about a tweet."""
    tweet_type: str
    raw_data: Dict[str, Any]
    urls: Set[str]
    mentioned_users: Set[str] = None
    hashtags: Set[str] = None
    quoted_tweet_id: str = None
    is_retweet: bool = False
    retweet_of_id: str = None 