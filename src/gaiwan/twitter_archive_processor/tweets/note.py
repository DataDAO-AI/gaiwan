from datetime import datetime
from typing import List, Optional, Dict, Set
from .base import BaseTweet
from ..core.metadata import TweetMetadata

class NoteTweet(BaseTweet):
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
        
        return ' '.join(text.split())

    @classmethod
    def from_raw_data(cls, data: Dict) -> 'NoteTweet':
        """Create a NoteTweet from raw Twitter API data."""
        created_at = None
        if 'createdAt' in data:
            try:
                created_at = datetime.strptime(
                    data['createdAt'],
                    '%Y-%m-%dT%H:%M:%S.%fZ'
                )
            except ValueError:
                pass

        return cls(
            id=data.get('noteTweetId'),
            text=data.get('core', {}).get('text', ''),
            created_at=created_at,
            media=[],  # Note tweets typically don't have media
            parent_id=None,
            metadata=TweetMetadata(
                tweet_type='note',
                raw_data=data,
                urls=set()
            )
        )

    def get_urls(self) -> Set[str]:
        """Get URLs from the note tweet."""
        urls = set()
        core_data = self.metadata.raw_data.get('core', {})
        for url in core_data.get('urls', []):
            if 'expanded_url' in url:
                urls.add(url['expanded_url'])
        return urls

    def get_mentions(self) -> Set[str]:
        """Get user mentions from the note tweet."""
        mentions = set()
        core_data = self.metadata.raw_data.get('core', {})
        for mention in core_data.get('mentions', []):
            if 'screenName' in mention:
                mentions.add(mention['screenName'])
        return mentions

    def get_hashtags(self) -> Set[str]:
        """Get hashtags from the note tweet."""
        hashtags = set()
        core_data = self.metadata.raw_data.get('core', {})
        for hashtag in core_data.get('hashtags', []):
            if isinstance(hashtag, str):
                hashtags.add(hashtag)
            elif isinstance(hashtag, dict) and 'text' in hashtag:
                hashtags.add(hashtag['text'])
        return hashtags 