"""Markdown exporter implementation."""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, TextIO

from .base import Exporter
from ..tweets.base import BaseTweet
from ..conversation import ConversationThread

logger = logging.getLogger(__name__)

class MarkdownExporter(Exporter):
    """Export tweets to Markdown format."""
    
    def export_tweets(self, tweets: List[BaseTweet], output_path: Path) -> None:
        """Export a list of tweets to markdown."""
        with open(output_path, 'w', encoding='utf-8') as f:
            for tweet in sorted(tweets, key=lambda t: t.created_at or datetime.min.replace(tzinfo=timezone.utc)):
                self._write_tweet(f, tweet)
    
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """Export a conversation thread to markdown."""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"# Thread started on {thread.created_at:%Y-%m-%d %H:%M:%S}\n\n")
            for tweet in thread.all_tweets:
                self._write_tweet(f, tweet)
    
    def _write_tweet(self, f: TextIO, tweet: BaseTweet) -> None:
        """Write a single tweet to the markdown file."""
        if tweet.created_at:
            f.write(f"## {tweet.created_at:%Y-%m-%d %H:%M:%S}\n\n")
        f.write(f"{tweet.text}\n\n")
        for media in tweet.media:
            f.write(f"![{media.get('type', 'media')}]({media.get('media_url', '')})\n\n") 