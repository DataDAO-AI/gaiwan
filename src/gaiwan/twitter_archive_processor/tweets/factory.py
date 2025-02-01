from datetime import datetime, timezone
from typing import Dict, Optional, Type

from .base import BaseTweet
from .types import StandardTweet, NoteTweet
from ..metadata import TweetMetadata

class TweetFactory:
    """Factory for creating different types of tweets."""
    
    _tweet_types = {
        'tweet': StandardTweet,
        'note': NoteTweet,
        'like': StandardTweet
    }
    
    @staticmethod
    def create_tweet(data: Dict, tweet_type: str) -> Optional[BaseTweet]:
        """Create a tweet of the appropriate type."""
        if tweet_type not in TweetFactory._tweet_types:
            raise ValueError(f"Unknown tweet type: {tweet_type}")
            
        tweet_class = TweetFactory._tweet_types[tweet_type]
        
        if tweet_type == 'note':
            if 'noteTweet' in data:
                data = data['noteTweet']  # Extract the note tweet data
            return tweet_class.from_raw_data(data)
        elif tweet_type == 'like':
            if 'like' in data:
                return StandardTweet(
                    id=data['like']['tweetId'],
                    text=data['like']['fullText'],
                    created_at=None,
                    media=[],
                    parent_id=None,
                    metadata=TweetMetadata(
                        tweet_type='like',
                        raw_data=data['like'],
                        urls=set()
                    )
                )
            return None
        else:
            if 'tweet' in data:
                data = data['tweet']  # Extract the tweet data
            return tweet_class.from_raw_data(data) 