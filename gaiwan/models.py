# models.py
"""Data models for Twitter archive processing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any, ClassVar
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
    
    # Add class-level constants for required fields
    REQUIRED_FIELDS: ClassVar[Set[str]] = {
        'id', 'created_at', 'text', 'entities'
    }
    
    # Add timestamp format patterns
    TIMESTAMP_FORMATS: ClassVar[List[str]] = [
        # ISO format with timezone
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        # Twitter's historical format
        "%a %b %d %H:%M:%S +0000 %Y"
    ]

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
    def parse_timestamp(cls, ts: str) -> Optional[datetime]:
        """Parse timestamp from multiple possible formats.
        
        Args:
            ts: Timestamp string in various possible formats
            
        Returns:
            Parsed datetime in UTC, or None if parsing fails
        """
        if not ts:
            return None
            
        # Handle Z suffix
        if ts.endswith('Z'):
            ts = ts[:-1] + '+00:00'
            
        # Try each format
        for fmt in cls.TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(ts, fmt)
                # Ensure UTC timezone
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
                
        logger.warning(f"Could not parse timestamp: {ts}")
        return None

    @classmethod
    def from_dict(cls, data: Dict[str, Any], source_type: str = "tweet") -> Optional['CanonicalTweet']:
        """Create tweet from dict following the schema.
        
        Args:
            data: Raw tweet data dictionary
            source_type: Type of tweet source ("tweet", "community_tweet", "note")
            
        Returns:
            CanonicalTweet instance or None if invalid
        """
        try:
            # Handle nested tweet structure first
            if 'tweet' in data:
                data = data['tweet']

            # Extract core fields with proper fallbacks
            tweet_id = cls._get_required_field(data, 'id')
            created_at = cls.parse_timestamp(cls._get_required_field(data, 'created_at'))
            text = cls._get_required_field(data, 'text')
            entities = cls._get_required_field(data, 'entities')

            if not all([tweet_id, created_at, text]):
                logger.warning(f"Missing required fields in {source_type} data")
                return None

            # Convert numeric fields
            try:
                retweet_count = int(data['retweet_count']) if 'retweet_count' in data else None
                favorite_count = int(data['favorite_count']) if 'favorite_count' in data else None
            except (ValueError, TypeError):
                retweet_count = None
                favorite_count = None

            # Convert boolean fields
            possibly_sensitive = data.get('possibly_sensitive')
            if isinstance(possibly_sensitive, str):
                possibly_sensitive = possibly_sensitive.lower() == 'true'

            return cls(
                id=tweet_id,
                created_at=created_at,
                text=text,
                entities=entities or {},
                possibly_sensitive=possibly_sensitive,
                favorited=data.get('favorited'),
                retweeted=data.get('retweeted'),
                retweet_count=retweet_count,
                favorite_count=favorite_count,
                in_reply_to_status_id=data.get('in_reply_to_status_id_str'),
                in_reply_to_user_id=data.get('in_reply_to_user_id_str'),
                in_reply_to_screen_name=data.get('in_reply_to_screen_name'),
                screen_name=data.get('user', {}).get('screen_name'),
                source_type=source_type,
                quoted_tweet_id=data.get('quoted_status_id_str'),
                community_id=data.get('community_id_str')
            )
        except Exception as e:
            logger.error(f"Error creating {source_type} from data: {e}")
            return None

    @classmethod 
    def from_note_data(cls, data: Dict[str, Any], username: Optional[str] = None) -> Optional['CanonicalTweet']:
        """Create from note tweet data following the schema.
        
        Args:
            data: Raw note tweet data
            username: Optional username for the tweet
            
        Returns:
            CanonicalTweet instance or None if invalid
        """
        try:
            note = data.get('noteTweet', {})
            core = note.get('core', {})
            
            if not all([note.get('noteTweetId'), note.get('createdAt'), core.get('text')]):
                return None
                
            return cls(
                id=note['noteTweetId'],
                created_at=cls.parse_timestamp(note['createdAt']),
                text=core['text'],
                entities=core.get('entities', {}),
                screen_name=username,
                source_type='note'
            )
        except Exception as e:
            logger.error(f"Error creating tweet from note data: {e}")
            return None

    @staticmethod
    def _get_required_field(data: Dict[str, Any], field: str) -> Any:
        """Get required field value with proper fallbacks."""
        # Handle field aliases
        if field == 'id':
            return data.get('id_str') or data.get('id')
        if field == 'text':
            return data.get('full_text') or data.get('text')
        if field == 'created_at':
            return data.get('created_at')
        if field == 'entities':
            return data.get('entities', {})
        
        return data.get(field)

    @classmethod
    def from_tweet_data(cls, data: Dict[str, Any], source_type: str = "tweet") -> Optional['CanonicalTweet']:
        """Alias for from_dict for backward compatibility."""
        return cls.from_dict(data, source_type)

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
