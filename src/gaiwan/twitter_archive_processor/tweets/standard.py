from datetime import datetime
from typing import List, Optional, Dict, Set
from .base import BaseTweet
from ..metadata import TweetMetadata

class StandardTweet(BaseTweet):
    def __init__(
        self,
        id: str,
        text: str,
        created_at: Optional[datetime],
        media: List[Dict],
        parent_id: Optional[str],
        metadata: TweetMetadata
    ):
        self.id = id
        self.text = text
        self.created_at = created_at
        self.media = media
        self.parent_id = parent_id
        self.metadata = metadata

    def clean_text(self) -> str:
        """Remove mentions, URLs, and hashtags from text."""
        text = self.text
        
        # Remove @mentions
        text = ' '.join(word for word in text.split() if not word.startswith('@'))
        
        # Remove URLs
        text = ' '.join(word for word in text.split() if not word.startswith('http'))
        
        # Remove hashtags
        text = ' '.join(word for word in text.split() if not word.startswith('#'))
        
        return ' '.join(text.split())  # Clean up extra spaces 

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