from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Set

from ..metadata import TweetMetadata

@dataclass
class BaseTweet(ABC):
    """Base class for all tweet types."""
    id: str
    text: str
    created_at: Optional[datetime]
    media: List[Dict]
    parent_id: Optional[str]
    metadata: TweetMetadata

    @abstractmethod
    def clean_text(self) -> str:
        """Clean the tweet text."""
        pass

    @abstractmethod
    def get_urls(self) -> Set[str]:
        """Extract URLs from the tweet."""
        pass

    @abstractmethod
    def get_mentions(self) -> Set[str]:
        """Extract user mentions from the tweet."""
        pass

    @abstractmethod
    def get_hashtags(self) -> Set[str]:
        """Extract hashtags from the tweet."""
        pass 