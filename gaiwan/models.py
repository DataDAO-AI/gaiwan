# models.py
"""Data models for Twitter archive processing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any
import re
from time import strptime, mktime
import os
import pytz
import logging
from .config import MixPRConfig, UserSimilarityConfig  # Add if needed by any code in models.py

logger = logging.getLogger(__name__)

@dataclass
class TweetMetadata:
    """Metadata extracted from tweet entities."""
    mentioned_users: Set[str] = field(default_factory=set)
    hashtags: Set[str] = field(default_factory=set)
    urls: Set[str] = field(default_factory=set)
    media_urls: List[str] = field(default_factory=list)

    @classmethod
    def from_entities(cls, entities: dict) -> 'TweetMetadata':
        """Create metadata from tweet entities following the schema."""
        metadata = cls()
        
        # Extract user mentions
        for mention in entities.get('user_mentions', []):
            if 'screen_name' in mention:
                metadata.mentioned_users.add(mention['screen_name'].lower())

        # Extract hashtags
        for tag in entities.get('hashtags', []):
            if 'text' in tag:
                metadata.hashtags.add(tag['text'].lower())

        # Extract URLs
        for url in entities.get('urls', []):
            if 'expanded_url' in url:
                metadata.urls.add(url['expanded_url'])

        # Extract media URLs
        for media in entities.get('media', []):
            if 'media_url_https' in media:
                metadata.media_urls.append(media['media_url_https'])

        return metadata

class CanonicalTweet:
    """Tweet model following the schema definition."""
    __slots__ = (
        'id', 'created_at', 'text', 'entities', 'possibly_sensitive',
        'favorited', 'retweeted', 'retweet_count', 'favorite_count',
        'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name',
        'screen_name', 'source_type', 'quoted_tweet_id', 'community_id',
        '_metadata', '_media_urls', 'is_retweet', 'liked_by'
    )
    
    def __init__(
        self,
        id: str,
        created_at: datetime,
        text: str,
        entities: dict,
        possibly_sensitive: Optional[bool] = None,
        favorited: Optional[bool] = None,
        retweeted: Optional[bool] = None,
        retweet_count: Optional[int] = None,
        favorite_count: Optional[int] = None,
        in_reply_to_status_id: Optional[str] = None,
        in_reply_to_user_id: Optional[str] = None,
        in_reply_to_screen_name: Optional[str] = None,
        screen_name: Optional[str] = None,
        source_type: str = "tweet",
        quoted_tweet_id: Optional[str] = None,
        community_id: Optional[str] = None,
    ):
        # Required fields from schema
        self.id = id
        self.created_at = created_at
        self.text = text
        self.entities = entities
        
        # Optional fields from schema
        self.possibly_sensitive = possibly_sensitive
        self.favorited = favorited
        self.retweeted = retweeted
        self.retweet_count = retweet_count
        self.favorite_count = favorite_count
        self.in_reply_to_status_id = in_reply_to_status_id
        self.in_reply_to_user_id = in_reply_to_user_id
        self.in_reply_to_screen_name = in_reply_to_screen_name
        self.screen_name = screen_name
        self.source_type = source_type
        self.quoted_tweet_id = quoted_tweet_id
        self.community_id = community_id

        # Initialize metadata first
        self._metadata = TweetMetadata.from_entities(entities)
        self._media_urls = self._metadata.media_urls  # Use metadata's media URLs
        self.is_retweet = bool(self.retweeted or text.startswith('RT @'))
        self.liked_by = set()  # For tracking who liked this tweet

    @property
    def metadata(self) -> TweetMetadata:
        """Get tweet metadata."""
        return self._metadata

    @property
    def media_urls(self) -> List[str]:
        """Get media URLs from metadata."""
        return self._media_urls

    @property
    def mentioned_users(self) -> Set[str]:
        """Get mentioned users from metadata."""
        return self._metadata.mentioned_users

    @property
    def reply_to_tweet_id(self) -> Optional[str]:
        """Alias for in_reply_to_status_id."""
        return self.in_reply_to_status_id

    @property
    def source(self) -> Optional[str]:
        """Backward compatibility for source."""
        return None  # Not in schema, return None

    @classmethod
    def from_dict(cls, data: dict, source_type: str = "tweet") -> Optional['CanonicalTweet']:
        """Create tweet from dict following the schema."""
        try:
            # Required fields from schema
            if not all(k in data for k in ['id_str', 'created_at', 'full_text', 'entities']):
                return None

            return cls(
                id=data['id_str'],
                created_at=parse_twitter_timestamp(data['created_at']),
                text=data['full_text'],
                entities=data.get('entities', {}),
                possibly_sensitive=data.get('possibly_sensitive'),
                favorited=data.get('favorited'),
                retweeted=data.get('retweeted'),
                retweet_count=int(data['retweet_count']) if 'retweet_count' in data else None,
                favorite_count=int(data['favorite_count']) if 'favorite_count' in data else None,
                in_reply_to_status_id=data.get('in_reply_to_status_id_str'),
                in_reply_to_user_id=data.get('in_reply_to_user_id_str'),
                in_reply_to_screen_name=data.get('in_reply_to_screen_name'),
                screen_name=data.get('user', {}).get('screen_name'),
                source_type=source_type,
                quoted_tweet_id=data.get('quoted_status_id_str'),
                community_id=data.get('community_id_str')
            )
        except Exception as e:
            logger.error(f"Error creating tweet from data: {e}")
            return None

    @classmethod
    def from_tweet_data(cls, data: dict, source_type: str = "tweet") -> Optional['CanonicalTweet']:
        """Create from tweet data following the schema."""
        try:
            # Required fields from schema
            if not all(k in data for k in ['id_str', 'created_at', 'full_text']):
                return None

            # Convert possibly_sensitive to proper boolean
            possibly_sensitive = None
            if 'possibly_sensitive' in data:
                if isinstance(data['possibly_sensitive'], bool):
                    possibly_sensitive = data['possibly_sensitive']
                else:
                    possibly_sensitive = str(data['possibly_sensitive']).lower() == 'true'

            return cls(
                id=data['id_str'],
                created_at=parse_twitter_timestamp(data['created_at']),
                text=data['full_text'],
                entities=data.get('entities', {}),
                possibly_sensitive=possibly_sensitive,  # Use converted value
                favorited=data.get('favorited'),
                retweeted=data.get('retweeted'),
                retweet_count=int(data['retweet_count']) if 'retweet_count' in data else None,
                favorite_count=int(data['favorite_count']) if 'favorite_count' in data else None,
                in_reply_to_status_id=data.get('in_reply_to_status_id_str'),
                in_reply_to_user_id=data.get('in_reply_to_user_id_str'),
                in_reply_to_screen_name=data.get('in_reply_to_screen_name'),
                screen_name=data.get('user', {}).get('screen_name'),
                source_type=source_type,
                quoted_tweet_id=data.get('quoted_status_id_str'),
                community_id=data.get('community_id_str')
            )
        except Exception as e:
            logger.error(f"Error creating tweet from data: {e}")
            return None

    @classmethod
    def from_note_data(cls, data: dict, username: str = None) -> Optional['CanonicalTweet']:
        """Create from note tweet data following the schema."""
        try:
            note = data.get('noteTweet', {})
            core = note.get('core', {})
            
            return cls(
                id=note.get('noteTweetId'),
                created_at=parse_twitter_timestamp(note.get('createdAt')),
                text=core.get('text', ''),
                entities=core.get('entities', {}),
                screen_name=username,
                source_type='note'
            )
        except Exception as e:
            logger.error(f"Error creating tweet from note data: {e}")
            return None

@dataclass
class RetrievalResult:
    """Represents a retrieval result with relevance score."""
    tweet: CanonicalTweet
    score: float

def parse_twitter_timestamp(ts: str) -> Optional[datetime]:
    """Convert Twitter's timestamp format to datetime."""
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except ValueError:
        try:
            time_struct = strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
            return datetime.fromtimestamp(mktime(time_struct), timezone.utc)
        except ValueError:
            return None
