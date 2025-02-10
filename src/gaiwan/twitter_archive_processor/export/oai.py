from pathlib import Path
import json
import logging
from typing import List, Dict
from ..conversation import ConversationThread
from ..tweets.base import BaseTweet
from .base import Exporter

logger = logging.getLogger(__name__)

class OpenAIExporter(Exporter):
    """Exports conversations in OpenAI format."""
    
    def __init__(self, system_message: str = "You are a helpful assistant."):
        self.system_message = system_message
    
    def export_tweets(self, tweets: List[BaseTweet], output_path: Path) -> None:
        """Export tweets to OpenAI format."""
        messages = self._format_as_messages(tweets)
        self._write_messages(messages, output_path)
    
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """Export a conversation thread to OpenAI format."""
        messages = self._format_thread_as_messages(thread)
        self._write_messages(messages, output_path)
    
    def _format_as_messages(self, tweets: List[BaseTweet]) -> List[Dict[str, str]]:
        """Format tweets as messages."""
        return [
            {"role": "system", "content": self.system_message}
        ] + [
            {"role": "user", "content": self._format_tweet_content(tweet)}
            for tweet in tweets
        ]
    
    def _format_thread_as_messages(self, thread: ConversationThread) -> List[Dict[str, str]]:
        """Format thread as messages."""
        return [
            {"role": "system", "content": self.system_message},
            *[{"role": "user", "content": tweet.clean_text()} 
              for tweet in thread.all_tweets]
        ]
    
    def _format_tweet_content(self, tweet: BaseTweet) -> str:
        """Format tweet content with metadata."""
        return tweet.clean_text()
    
    def _write_messages(self, messages: List[Dict[str, str]], output_path: Path) -> None:
        """Write messages to file in JSONL format."""
        with open(output_path, 'a') as f:
            f.write(json.dumps({"messages": messages}) + '\n')

    def export_conversations(
        self,
        threads: List[ConversationThread],
        output_path: Path,
        system_message: str
    ) -> None:
        """Export conversation threads as OpenAI JSONL format."""
        try:
            with open(output_path, 'w') as f:
                for thread in threads:
                    conversation = {
                        'messages': [
                            {'role': 'system', 'content': system_message},
                            *[{'role': 'user', 'content': tweet.clean_text()} 
                              for tweet in thread.all_tweets]
                        ]
                    }
                    f.write(json.dumps(conversation) + '\n')
            
            logger.info(f"Exported {len(threads)} conversations to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export conversations: {e}")
            raise 