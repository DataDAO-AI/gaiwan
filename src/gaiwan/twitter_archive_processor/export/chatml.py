"""ChatML exporter implementation."""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from .oai import OpenAIExporter
from ..tweets.base import BaseTweet
from ..conversation import ConversationThread
from ..coretypes import Content

logger = logging.getLogger(__name__)

class ChatMLExporter(OpenAIExporter):
    """Export tweets to ChatML format with alternating roles."""
    
    def __init__(self, system_message: str = "You are a helpful assistant."):
        self.system_message = system_message
    
    def export_tweets(self, tweets: List[BaseTweet], output_path: Path) -> None:
        """Export tweets to ChatML format."""
        messages = self._format_as_messages(tweets)
        self._write_messages(messages, output_path)
    
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """Export a conversation thread to ChatML format."""
        messages = self._format_thread_as_messages(thread)
        self._write_messages(messages, output_path)
    
    def _format_as_messages(self, tweets: List[BaseTweet]) -> List[Dict[str, str]]:
        """Format tweets as ChatML messages."""
        return [
            {"role": "system", "content": self.system_message}
        ] + [
            {"role": "user", "content": self._format_tweet_content(tweet)}
            for tweet in tweets
        ]
    
    def _format_thread_as_messages(self, thread: ConversationThread) -> List[Dict[str, str]]:
        """Format thread as alternating user/assistant messages."""
        messages = [{"role": "system", "content": self.system_message}]
        for i, tweet in enumerate([thread.root_tweet] + thread.replies):
            role = "user" if i % 2 == 0 else "assistant"
            messages.append({
                "role": role,
                "content": self._format_tweet_content(tweet)
            })
        return messages
    
    def _format_tweet_content(self, tweet: BaseTweet) -> str:
        """Format tweet content with metadata and media."""
        content = [tweet.text]
        
        # Add media descriptions if present
        for media in tweet.media:
            media_type = media.get('type', 'media')
            media_url = media.get('media_url', '')
            content.append(f"\n[{media_type}]({media_url})")
        
        # Add timestamp if present
        if tweet.created_at:
            content.append(f"\n[Posted on {tweet.created_at:%Y-%m-%d %H:%M:%S}]")
            
        return "".join(content)
    
    def _write_messages(self, messages: List[Dict[str, str]], output_path: Path) -> None:
        """Write messages to file in pretty-printed JSON format."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({"messages": messages}, f, indent=2) 