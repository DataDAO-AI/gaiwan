from datetime import datetime
from typing import Optional, List, Dict, Set
import re

from .base import BaseTweet
from ..metadata import TweetMetadata

class StandardTweet(BaseTweet):
    """Regular tweet with full functionality."""
    
    def clean_text(self) -> str:
        """Clean the tweet text by removing URLs, mentions, etc."""
        text = self.text
        
        # Remove URLs
        text = re.sub(r'https?://\S+', '', text)
        # Remove mentions
        text = re.sub(r'@\w+', '', text)
        # Remove hashtags
        text = re.sub(r'#\w+', '', text)
        # Clean up whitespace
        text = ' '.join(text.split())
        
        return text.strip()

    def get_urls(self) -> Set[str]:
        """Extract URLs from tweet text and entities."""
        urls = set()
        if 'entities' in self.metadata.raw_data:
            entities = self.metadata.raw_data['entities']
            if 'urls' in entities:
                for url in entities['urls']:
                    if 'expanded_url' in url:
                        urls.add(url['expanded_url'])
        return urls

    def get_mentions(self) -> Set[str]:
        """Extract user mentions from tweet."""
        mentions = set()
        if 'entities' in self.metadata.raw_data:
            entities = self.metadata.raw_data['entities']
            if 'user_mentions' in entities:
                for mention in entities['user_mentions']:
                    if 'screen_name' in mention:
                        mentions.add(mention['screen_name'])
        return mentions

    def get_hashtags(self) -> Set[str]:
        """Extract hashtags from tweet."""
        hashtags = set()
        if 'entities' in self.metadata.raw_data:
            entities = self.metadata.raw_data['entities']
            if 'hashtags' in entities:
                for hashtag in entities['hashtags']:
                    if 'text' in hashtag:
                        hashtags.add(hashtag['text'])
        return hashtags

class NoteTweet(BaseTweet):
    """Twitter Notes with their specific structure."""
    
    def clean_text(self) -> str:
        """Clean the note text."""
        return self.text.strip()

    def get_urls(self) -> Set[str]:
        """Extract URLs from note."""
        urls = set()
        if 'core' in self.metadata.raw_data:
            core = self.metadata.raw_data['core']
            if 'urls' in core:
                for url in core['urls']:
                    if 'expanded_url' in url:
                        urls.add(url['expanded_url'])
        return urls

    def get_mentions(self) -> Set[str]:
        """Extract mentions from note."""
        mentions = set()
        if 'core' in self.metadata.raw_data:
            core = self.metadata.raw_data['core']
            if 'mentions' in core:
                for mention in core['mentions']:
                    mentions.add(mention['screenName'])
        return mentions

    def get_hashtags(self) -> Set[str]:
        """Extract hashtags from note."""
        hashtags = set()
        if 'core' in self.metadata.raw_data:
            core = self.metadata.raw_data['core']
            if 'hashtags' in core:
                hashtags.update(core['hashtags'])
        return hashtags 