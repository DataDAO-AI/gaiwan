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
    
    @classmethod
    def create_tweet(cls, tweet_data: Dict, tweet_type: str) -> Optional[BaseTweet]:
        """Create a tweet of the appropriate type."""
        if tweet_type not in cls._tweet_types:
            raise ValueError(f"Unknown tweet type: {tweet_type}")
            
        tweet_class = cls._tweet_types[tweet_type]
        
        # Extract common fields
        if tweet_type == 'note':
            tweet_id = tweet_data.get('noteTweetId')
            text = tweet_data.get('core', {}).get('text', '')
            created_at = tweet_data.get('createdAt')
        elif tweet_type == 'like':
            tweet_id = tweet_data.get('id_str')
            text = tweet_data.get('full_text')
            created_at = None  # Likes might not have timestamps
        else:
            tweet_id = tweet_data.get('id_str') or tweet_data.get('tweetId')
            text = tweet_data.get('full_text') or tweet_data.get('text', '')
            created_at = tweet_data.get('created_at')
        
        if not tweet_id:
            return None
            
        # Parse timestamp
        if created_at:
            try:
                if tweet_type == 'note':
                    # Note tweets use a different timestamp format
                    timestamp = datetime.strptime(
                        created_at,
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ).replace(tzinfo=timezone.utc)
                else:
                    timestamp = datetime.strptime(
                        created_at,
                        "%a %b %d %H:%M:%S %z %Y"
                    )
            except ValueError:
                timestamp = None
        else:
            timestamp = None
        
        # Create metadata
        metadata = TweetMetadata(
            tweet_type=tweet_type,
            raw_data=tweet_data,
            urls=set()  # Will be populated by the tweet object
        )
        
        return tweet_class(
            id=tweet_id,
            text=text,
            created_at=timestamp,
            media=tweet_data.get('extended_entities', {}).get('media', []),
            parent_id=tweet_data.get('in_reply_to_status_id_str'),
            metadata=metadata
        ) 

    @staticmethod
    def create_tweet(data: Dict, tweet_type: str) -> Optional[BaseTweet]:
        """Create a tweet of the appropriate type."""
        if tweet_type not in TweetFactory._tweet_types:
            raise ValueError(f"Unknown tweet type: {tweet_type}")
            
        tweet_class = TweetFactory._tweet_types[tweet_type]
        
        if tweet_type == 'note':
            if 'noteTweet' in data:
                return tweet_class.from_raw_data(data['noteTweet'])
            return None
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
            if not data.get('id_str'):
                return None
            return tweet_class.from_raw_data(data) 