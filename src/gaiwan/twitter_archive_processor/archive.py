from pathlib import Path
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

from .tweets.factory import TweetFactory
from .tweets.base import BaseTweet
from .url_analysis.analyzer import URLAnalyzer
from .metadata import TweetMetadata
from .conversation import ConversationThread
from .export.base import Exporter

logger = logging.getLogger(__name__)

class Archive:
    """Represents a Twitter archive with methods for analysis and processing."""
    
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.archive_path = file_path  # Add this for backward compatibility
        self.username = None
        self.tweets = []
        self.metadata = {}
        self.url_analyzer = URLAnalyzer(archive_dir=None)
        
    def load(self) -> None:
        """Load archive data from file."""
        try:
            with open(self.file_path) as f:
                data = json.load(f)
                
            # Load account info
            if 'account' in data and data['account']:
                account_data = data['account'][0].get('account', {})
                self.username = account_data.get('username')
                self.metadata['account'] = account_data

            # Load tweets
            for tweet_data in data.get('tweets', []):
                if tweet := TweetFactory.create_tweet(tweet_data, 'tweet'):
                    self.tweets.append(tweet)

            # Load note tweets
            for note_data in data.get('note-tweet', []):
                if note := TweetFactory.create_tweet(note_data, 'note'):
                    self.tweets.append(note)

            # Load likes
            for like_data in data.get('like', []):
                if like := TweetFactory.create_tweet(like_data, 'like'):
                    self.tweets.append(like)

        except Exception as e:
            logger.error(f"Failed to load archive {self.file_path}: {e}")
            raise
    
    def analyze_urls(self) -> pd.DataFrame:
        """Analyze URLs in the archive using URLAnalyzer."""
        return self.url_analyzer.analyze_archive(self.file_path)
    
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
    
    def export(self, format: str, output_path: Path, system_message: str = "You are a helpful assistant.") -> None:
        """Export the archive in various formats."""
        from .export.markdown import MarkdownExporter
        from .export.oai import OpenAIExporter
        from .export.chatml import ChatMLExporter
        
        EXPORTERS = {
            'markdown': MarkdownExporter,
            'oai': OpenAIExporter,
            'chatml': ChatMLExporter
        }
        
        if format not in EXPORTERS:
            raise ValueError(f"Unsupported export format: {format}")
        
        exporter = EXPORTERS[format](system_message=system_message)
        if format in ('oai', 'chatml'):
            threads = self.get_conversation_threads()
            if threads:
                for thread in threads:
                    exporter.export_thread(thread, output_path)
            else:
                # If no threads, export individual tweets
                exporter.export_tweets(self.tweets, output_path)
        else:
            exporter.export_tweets(self.tweets, output_path)