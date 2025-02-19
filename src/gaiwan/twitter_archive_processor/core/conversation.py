from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timezone

from ..tweets.base import BaseTweet

@dataclass
class ConversationThread:
    """Represents a thread of related tweets."""
    root_tweet: BaseTweet
    replies: List[BaseTweet] = field(default_factory=list)
    created_at: datetime = field(init=False)
    
    def __post_init__(self):
        self.created_at = self.root_tweet.created_at or datetime.min.replace(tzinfo=timezone.utc)
    
    def add_reply(self, tweet: BaseTweet) -> None:
        """Add a reply to the thread."""
        self.replies.append(tweet)
        self.replies.sort(key=lambda t: t.created_at or datetime.min.replace(tzinfo=timezone.utc))
    
    @property
    def all_tweets(self) -> List[BaseTweet]:
        """Get all tweets in the thread in chronological order."""
        return [self.root_tweet] + self.replies
    
    @property
    def length(self) -> int:
        """Get the number of tweets in the thread."""
        return len(self.replies) + 1 