from pathlib import Path
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

from .tweets.factory import TweetFactory
from .tweets.base import BaseTweet
from .url_analysis import URLAnalyzer
from .metadata import TweetMetadata
from .conversation import ConversationThread
from .export import MarkdownExporter

logger = logging.getLogger(__name__)

class Archive:
    """Represents a Twitter archive with methods for analysis and processing."""
    
    def __init__(self, archive_path: Path):
        self.archive_path = archive_path
        self.username: Optional[str] = None
        self.tweets: List[BaseTweet] = []  # Now using BaseTweet
        self.url_analyzer = URLAnalyzer(archive_path)
        self.metadata: Dict = {}
        
    def load(self) -> None:
        """Load and parse the archive file."""
        try:
            with open(self.archive_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract account info
            if 'account' in data:
                account = data['account'][0]['account']
                self.username = account['username']
                self.metadata['account'] = account
            
            # Process different tweet types using factory
            for tweet_type, key in [
                ('tweet', 'tweets'),
                ('community', 'community-tweet'),
                ('note', 'note-tweet'),
                ('like', 'like')
            ]:
                self._process_tweets(data.get(key, []), tweet_type)
            
            logger.info(f"Loaded {len(self.tweets)} tweets from {self.username}'s archive")
            
        except Exception as e:
            logger.error(f"Error loading archive {self.archive_path}: {e}")
            raise
    
    def _process_tweets(self, tweets_data: List[Dict], tweet_type: str) -> None:
        """Process tweets of a specific type using the factory."""
        for tweet_data in tweets_data:
            # Extract the actual tweet data based on type
            if tweet_type in ('tweet', 'community'):
                data = tweet_data.get('tweet', {})
            elif tweet_type == 'note':
                note_data = tweet_data.get('noteTweet', {})
                # For note tweets, handle the different structure
                data = {
                    'noteTweetId': note_data.get('noteTweetId'),
                    'core': note_data.get('core', {}),
                    'createdAt': note_data.get('createdAt')
                }
            else:  # like
                data = tweet_data.get('like', {})
                # For likes, ensure we have the right text field
                if 'fullText' in data:
                    data['text'] = data['fullText']
            
            if data:
                tweet = TweetFactory.create_tweet(data, tweet_type)
                if tweet:
                    self.tweets.append(tweet)
    
    def analyze_urls(self) -> pd.DataFrame:
        """Analyze URLs in the archive using URLAnalyzer."""
        return self.url_analyzer.analyze_archive(self.archive_path)
    
    def get_conversation_threads(self) -> List[ConversationThread]:
        """Extract conversation threads from tweets."""
        threads = []
        replies = {}
        
        # First pass: organize tweets by parent ID
        for tweet in self.tweets:
            if tweet.parent_id:
                if tweet.parent_id not in replies:
                    replies[tweet.parent_id] = []
                replies[tweet.parent_id].append(tweet)
        
        # Second pass: create threads
        for tweet in self.tweets:
            if not tweet.parent_id and tweet.id in replies:
                # This is a root tweet with replies
                thread = ConversationThread(root_tweet=tweet)
                for reply in replies[tweet.id]:
                    thread.add_reply(reply)
                threads.append(thread)
        
        return sorted(threads, key=lambda t: t.created_at)
    
    def export(self, format: str, output_path: Path) -> None:
        """Export the archive in various formats."""
        exporters = {
            'markdown': MarkdownExporter()
            # Add other exporters here
        }
        
        if format not in exporters:
            raise ValueError(f"Unsupported export format: {format}")
        
        exporter = exporters[format]
        exporter.export_tweets(self.tweets, output_path)