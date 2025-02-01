from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Set, ClassVar

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

    @classmethod
    @abstractmethod
    def from_raw_data(cls, data: Dict) -> 'BaseTweet':
        """Create a tweet from raw Twitter API data."""
        pass

    def clean_text(self) -> str:
        """Remove mentions, URLs, and hashtags from text."""
        text = self.text
        
        # Remove @mentions
        text = ' '.join(word for word in text.split() if not word.startswith('@'))
        
        # Remove URLs
        text = ' '.join(word for word in text.split() if not word.startswith('http'))
        
        # Remove hashtags
        text = ' '.join(word for word in text.split() if not word.startswith('#'))
        
        return ' '.join(text.split())

    def get_urls(self) -> Set[str]:
        """Extract URLs from tweet metadata."""
        if 'entities' in self.metadata.raw_data:
            return {url['expanded_url'] for url in self.metadata.raw_data['entities'].get('urls', [])}
        return set()

    def get_mentions(self) -> Set[str]:
        """Extract mentions from tweet metadata."""
        if 'entities' in self.metadata.raw_data:
            return {mention['screen_name'] for mention in self.metadata.raw_data['entities'].get('user_mentions', [])}
        return set()

    def get_hashtags(self) -> Set[str]:
        """Extract hashtags from tweet metadata."""
        if 'entities' in self.metadata.raw_data:
            return {hashtag['text'] for hashtag in self.metadata.raw_data['entities'].get('hashtags', [])}
        return set() 