"""JSONL exporter implementation."""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from .base import Exporter
from ..tweets.base import BaseTweet
from ..conversation import ConversationThread
from ..coretypes import Content

logger = logging.getLogger(__name__)

class JSONLExporter(Exporter):
    """Export tweets to JSONL format."""
    
    def __init__(self, system_message: str = "You are a helpful assistant."):
        self.system_message = system_message
    
    def export_tweets(self, tweets: List[BaseTweet], output_path: Path) -> None:
        """Export tweets to JSONL format."""
        with open(output_path, 'w', encoding='utf-8') as f:
            for tweet in tweets:
                json.dump(self._format_tweet(tweet), f)
                f.write('\n')
    
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """Export a conversation thread to JSONL format."""
        formatted = self._format_conversation([thread.root_tweet] + thread.replies)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(formatted, f)
            f.write('\n')
    
    def _format_tweet(self, tweet: BaseTweet) -> Dict[str, Any]:
        """Format a single tweet for JSONL export."""
        return {
            "id": tweet.id,
            "text": tweet.text,
            "created_at": tweet.created_at.isoformat() if tweet.created_at else None,
            "media": tweet.media,
            "metadata": tweet.metadata.raw_data
        }
    
    def _format_conversation(self, tweets: List[Content]) -> Dict[str, Any]:
        """Format a conversation for JSONL export."""
        return {
            "messages": [
                {"role": "system", "content": self.system_message}
            ] + [
                {"role": "user" if i % 2 == 0 else "assistant", "content": t.text}
                for i, t in enumerate(tweets)
            ]
        } 