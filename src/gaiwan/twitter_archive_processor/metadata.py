from dataclasses import dataclass
from typing import Dict, Set, Any

@dataclass
class TweetMetadata:
    """Contains metadata about a tweet."""
    tweet_type: str
    raw_data: Dict[str, Any]
    urls: Set[str] 