"""Base class for exporters."""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from ..tweets.base import BaseTweet
from ..core.conversation import ConversationThread

class Exporter(ABC):
    """Base class for archive exporters."""
    
    def __init__(self, system_message: str = None):
        self.system_message = system_message
    
    @abstractmethod
    def export_tweets(self, tweets: List[BaseTweet], output_path: Path) -> None:
        """Export tweets to the specified format."""
        pass
    
    @abstractmethod
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """Export a conversation thread to the specified format."""
        pass 