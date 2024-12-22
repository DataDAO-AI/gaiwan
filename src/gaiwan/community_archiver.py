"""Process Twitter archives into normalized tweet and reply data."""

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set, Dict, Any, ClassVar, Tuple
from dataclasses import dataclass, field
import json

import requests

# Constants
SUPABASE_URL = "https://fabxmporizzqflnftavs.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZhYnh"
    "tcG9yaXp6cWZsbmZ0YXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjIyNDQ5MTIsImV4cCI6Mj"
    "AzNzgyMDkxMn0.UIEJiUNkLsW28tBHmG-RQDW-I5JNlJLt62CSk9D_qG8"
)
REQUEST_TIMEOUT = 30
BATCH_SIZE = 1000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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
        self.id = id
        self.created_at = created_at
        self.text = text
        self.entities = entities
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

        # Initialize metadata
        self._metadata = TweetMetadata.from_entities(entities)
        self.is_retweet = bool(self.retweeted or text.startswith('RT @'))
        self.liked_by = set()

    @property
    def metadata(self) -> TweetMetadata:
        """Get tweet metadata."""
        return self._metadata

    @property
    def reply_to_tweet_id(self) -> Optional[str]:
        """Alias for in_reply_to_status_id for compatibility."""
        return self.in_reply_to_status_id

    @property
    def reply_to_user_id(self) -> Optional[str]:
        """Alias for in_reply_to_user_id for compatibility."""
        return self.in_reply_to_user_id

    @property
    def reply_to_screen_name(self) -> Optional[str]:
        """Alias for in_reply_to_screen_name for compatibility."""
        return self.in_reply_to_screen_name

    @classmethod
    def from_dict(cls, data: Dict[str, Any], source_type: str = "tweet") -> Optional['CanonicalTweet']:
        """Create tweet from dictionary data."""
        try:
            # Handle nested tweet structure
            if 'tweet' in data:
                data = data['tweet']

            # Parse timestamp
            created_at = cls.parse_timestamp(data.get('created_at'))
            if not created_at:
                return None

            return cls(
                id=data.get('id_str') or str(data.get('id')),
                created_at=created_at,
                text=data.get('full_text') or data.get('text', ''),
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
    def from_note_data(cls, data: Dict[str, Any], username: Optional[str] = None) -> Optional['CanonicalTweet']:
        """Create from note tweet data."""
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
                screen_name=username or note.get('screenName'),
                source_type='note'
            )
        except Exception as e:
            logger.error(f"Error creating tweet from note data: {e}")
            return None

    @classmethod
    def from_tweet_data(cls, data: Dict[str, Any], source_type: str = "tweet") -> Optional['CanonicalTweet']:
        """Alias for from_dict for backward compatibility."""
        return cls.from_dict(data, source_type)

    @classmethod
    def parse_timestamp(cls, ts: str) -> Optional[datetime]:
        """Convert Twitter's timestamp format to datetime.

        Args:
            ts: Twitter format timestamp string

        Returns:
            Parsed datetime or None if parsing fails
        """
        if not ts:
            return None
            
        try:
            # Handle Z suffix
            if ts.endswith('Z'):
                ts = ts[:-1] + '+00:00'
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except ValueError:
            try:
                time_struct = time.strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
                return datetime.fromtimestamp(time.mktime(time_struct)).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("Could not parse timestamp: %s", ts)
                return None

class BatchWriter:
    """Efficient batch writer for large datasets."""

    def __init__(self, filepath: Path, batch_size: int = BATCH_SIZE):
        """Initialize batch writer.

        Args:
            filepath: Path to output file
            batch_size: Number of items to write at once
        """
        self.filepath = filepath
        self.batch_size = batch_size
        self.batch = []

    def add(self, item: dict) -> None:
        """Add item to batch, flushing if batch is full.

        Args:
            item: Dictionary to serialize and write
        """
        self.batch.append(item)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Write current batch to file and clear buffer."""
        if not self.batch:
            return

        mode = 'ab' if self.filepath.exists() else 'wb'
        with self.filepath.open(mode) as f:
            for item in self.batch:
                f.write(json.dumps(item).encode() + b'\n')
        self.batch.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

class ArchiveProcessor:
    """Processes Twitter archives into normalized tweet and reply data."""

    def __init__(self, output_dir: Path):
        """Initialize processor.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = output_dir
        self.tweets_file = output_dir / "canonical_tweets.jsonl"
        self.replies_file = output_dir / "reply_edges.jsonl"
        self.processed_archives_file = output_dir / "processed_archives.txt"
        self.processed_archives = set()

    def _load_processed_archives(self) -> Set[str]:
        """Load set of already processed archive filenames."""
        if self.processed_archives_file.exists():
            return set(
                line.strip()
                for line in self.processed_archives_file.open()
                if line.strip()
            )
        return set()

    def _mark_archive_processed(self, path: Path, metadata: Dict) -> None:
        """Mark archive as processed with metadata.
        
        Args:
            path: Path to processed archive
            metadata: Archive metadata including last_modified, size, etc
        """
        with self.processed_archives_file.open('a') as f:
            record = {
                'filename': path.name,
                'processed_at': datetime.now().isoformat(),
                'metadata': metadata
            }
            f.write(json.dumps(record) + '\n')
        self.processed_archives.add(path.name)

    def should_process_archive(self, path: Path, current_metadata: Dict) -> bool:
        """Check if archive needs processing based on metadata changes."""
        if path.name not in self.processed_archives:
            return True
            
        # Check if archive has been modified since last processing
        stored_metadata = self._get_stored_metadata(path.name)
        if not stored_metadata:
            return True
            
        return (
            stored_metadata.get('etag') != current_metadata.get('etag') or
            stored_metadata.get('last_modified') != current_metadata.get('last_modified')
        )

    def process_file(self, archive_path: Path) -> List[CanonicalTweet]:
        """Process a single archive file."""
        try:
            username = archive_path.stem.split('_')[0]
            logger.info(f"Processing archive for {username}")
            
            # Load new archive data
            with open(archive_path) as f:
                new_data = json.load(f)
            
            # Check for existing archive data
            existing_path = self.output_dir / f"{username}_processed.json"
            if existing_path.exists():
                logger.info(f"Found existing processed archive for {username}")
                with open(existing_path) as f:
                    old_data = json.load(f)
                # Merge archives
                data = self.merge_archives(old_data, new_data)
            else:
                data = new_data
            
            # Save merged data
            with open(existing_path, 'w') as f:
                json.dump(data, f)
            
            tweets = []
            # Process tweets in batches
            def process_batch(items, source_type):
                batch_tweets = []
                for item in items:
                    if len(batch_tweets) >= BATCH_SIZE:
                        tweets.extend(batch_tweets)
                        batch_tweets = []
                        
                    if source_type == 'community' and 'tweet' in item:
                        tweet = CanonicalTweet.from_tweet_data(
                            item['tweet'], 
                            source_type='community'
                        )
                    elif source_type == 'note' and 'noteTweet' in item:
                        tweet = CanonicalTweet.from_note_data(item, username)
                    else:
                        continue
                        
                    if tweet and tweet.id:
                        batch_tweets.append(tweet)
                
                tweets.extend(batch_tweets)  # Add remaining tweets
            
            # Process each type in batches
            if 'community-tweet' in data:
                process_batch(data['community-tweet'], 'community')
            if 'note-tweet' in data:
                process_batch(data['note-tweet'], 'note')
            if 'tweets' in data:
                process_batch(data['tweets'], 'tweet')
                
            return tweets
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {str(e)}")
            return []

    def _normalize_tweet_data(self, data: dict) -> Optional[dict]:
        """Normalize tweet data to match schema."""
        try:
            # Handle nested tweet structure
            if isinstance(data, dict):
                if 'tweet' in data:
                    return self._normalize_tweet_data(data['tweet'])
                if 'noteTweet' in data:
                    note = data['noteTweet']
                    return {
                        'id_str': note.get('noteTweetId'),
                        'created_at': note.get('createdAt'),
                        'full_text': note.get('core', {}).get('text', ''),
                        'entities': note.get('core', {}).get('entities', {}),
                        'user': {'screen_name': note.get('screenName', 'visakanv')}
                    }
                # Add screen_name if not present
                if 'user' not in data:
                    data = {**data, 'user': {'screen_name': 'visakanv'}}
                return data
        except Exception as e:
            logger.error(f"Error normalizing tweet data: {e}")
            return None

    def process_archive(self, archive_path: Path) -> List[CanonicalTweet]:
        """Process archive following the schema structure."""
        tweets = []
        
        try:
            # If it's a JSON file, process it directly
            if archive_path.suffix == '.json':
                logger.info(f"Processing JSON archive: {archive_path}")
                with open(archive_path) as f:
                    data = json.load(f)
                    logger.debug(f"Archive data keys: {list(data.keys())}")
                    
                    # Process main tweets
                    if 'tweets' in data:
                        tweet_list = data['tweets']
                        logger.info(f"Found {len(tweet_list)} tweets in archive")
                        
                        for tweet_wrapper in tweet_list:
                            if 'tweet' in tweet_wrapper:
                                tweet_data = tweet_wrapper['tweet']
                                normalized_data = self._normalize_tweet_data(tweet_data)
                                if normalized_data:
                                    tweet = CanonicalTweet.from_dict(normalized_data)
                                    if tweet and tweet.id:
                                        tweets.append(tweet)
                
                    # Process community tweets
                    if 'community-tweet' in data:
                        community_tweets = data['community-tweet']
                        logger.info(f"Found {len(community_tweets)} community tweets")
                        
                        for tweet_wrapper in community_tweets:
                            if 'tweet' in tweet_wrapper:
                                tweet_data = tweet_wrapper['tweet']
                                normalized_data = self._normalize_tweet_data(tweet_data)
                                if normalized_data:
                                    tweet = CanonicalTweet.from_dict(normalized_data, source_type='community_tweet')
                                    if tweet and tweet.id:
                                        tweets.append(tweet)
                
                    # Process note tweets
                    if 'note-tweet' in data:
                        note_tweets = data['note-tweet']
                        logger.info(f"Found {len(note_tweets)} note tweets")
                        
                        for note_wrapper in note_tweets:
                            if 'noteTweet' in note_wrapper:
                                tweet = CanonicalTweet.from_note_data(note_wrapper)
                                if tweet and tweet.id:
                                    tweets.append(tweet)
                
                    logger.info(f"Successfully processed {len(tweets)} total tweets")
                    return tweets
                
            # Otherwise look for tweet.js
            tweet_js = archive_path / "tweet.js"
            if tweet_js.exists():
                logger.info(f"Processing tweet.js file: {tweet_js}")
                with open(tweet_js) as f:
                    content = f.read()
                    if content.startswith('window.YTD.tweet.part0 = '):
                        content = content.replace('window.YTD.tweet.part0 = ', '')
                    
                    data = json.loads(content)
                    tweet_list = data.get('tweet', []) if isinstance(data, dict) else data
                    
                    for raw_tweet in tweet_list:
                        normalized_data = self._normalize_tweet_data(raw_tweet)
                        if normalized_data:
                            tweet = CanonicalTweet.from_dict(normalized_data)
                            if tweet and tweet.id:
                                tweets.append(tweet)
            
            return tweets
            
        except Exception as e:
            logger.error(f"Error processing archive {archive_path}: {e}")
            return []

    def process_note_tweets(self, note_tweets_data, username):
        """Process note tweets from archive data."""
        for idx, note_tweet in enumerate(note_tweets_data):
            try:
                note_data = note_tweet.get('noteTweet', {})
                updated_at = CanonicalTweet.parse_twitter_datetime(note_data['updatedAt'])
                # ... rest of the processing ...
                
            except Exception as e:
                logger.error(
                    f"Error processing note tweet {idx} for {username}: {str(e)}\n"
                    f"Note data: {json.dumps(note_tweet, indent=2)[:500]}..."
                )

    def _process_note_tweets(self, note_tweets: List[Dict], username: str) -> List[Dict]:
        """Process note tweets from the archive."""
        processed_notes = []
        for idx, note_tweet in enumerate(note_tweets):
            try:
                # Process note tweet
                processed_note = self._process_single_note_tweet(note_tweet)
                if processed_note:
                    processed_notes.append(processed_note)
            except Exception as e:
                logger.error(
                    f"Error processing note tweet {idx} for {username}: {str(e)}\n"
                    f"Note data: {json.dumps(note_tweet, indent=2)[:500]}..."
                )
                continue
        return processed_notes

    def process_tweet(self, tweet_data: dict, source_type: str) -> Optional[CanonicalTweet]:
        """Process any tweet type into canonical form.
        
        Args:
            tweet_data: Raw tweet data
            source_type: Type of tweet ("tweet", "community_tweet", "note")
            
        Returns:
            CanonicalTweet instance or None if invalid
        """
        try:
            if source_type == "note":
                # For note tweets, we need to handle the noteTweet wrapper
                if 'noteTweet' not in tweet_data:
                    logger.warning("Missing noteTweet wrapper in note tweet data")
                    return None
                return CanonicalTweet.from_note_data(tweet_data)
            else:
                # Regular and community tweets use from_dict
                return CanonicalTweet.from_dict(tweet_data, source_type=source_type)
        except Exception as e:
            logger.error(f"Error processing {source_type}: {str(e)}")
            return None

    def _create_tweet_from_data(self, tweet_data: dict) -> Optional[CanonicalTweet]:
        """Create CanonicalTweet from raw tweet data."""
        try:
            # Ensure required fields exist
            if not all(k in tweet_data for k in ['id_str', 'created_at', 'full_text', 'entities']):
                logger.warning(f"Missing required fields in tweet data: {tweet_data}")
                return None
            
            return CanonicalTweet(
                id=tweet_data['id_str'],
                created_at=parse_twitter_timestamp(tweet_data['created_at']),
                text=tweet_data['full_text'],
                entities=tweet_data.get('entities', {}),
                possibly_sensitive=tweet_data.get('possibly_sensitive'),
                favorited=tweet_data.get('favorited'),
                retweeted=tweet_data.get('retweeted'),
                retweet_count=int(tweet_data['retweet_count']) if 'retweet_count' in tweet_data else None,
                favorite_count=int(tweet_data['favorite_count']) if 'favorite_count' in tweet_data else None,
                reply_to_tweet_id=tweet_data.get('in_reply_to_status_id_str'),
                reply_to_user_id=tweet_data.get('in_reply_to_user_id_str'),
                reply_to_screen_name=tweet_data.get('in_reply_to_screen_name'),
                screen_name=tweet_data.get('user', {}).get('screen_name'),
                is_retweet='retweeted_status' in tweet_data,
                retweet_of_id=tweet_data.get('retweeted_status', {}).get('id_str'),
                quoted_tweet_id=tweet_data.get('quoted_status_id_str')
            )
        except Exception as e:
            logger.error(f"Error creating tweet from data: {e}")
            return None

    def _get_stored_metadata(self, filename: str) -> Optional[Dict]:
        """Get stored metadata for an archive.
        
        Args:
            filename: Name of archive file
            
        Returns:
            Stored metadata dict or None if not found
        """
        if not self.processed_archives_file.exists():
            return None
        
        with self.processed_archives_file.open() as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if record['filename'] == filename:
                        return record['metadata']
                except json.JSONDecodeError:
                    continue
        return None

    def merge_archives(self, old_data: Dict, new_data: Dict) -> Dict:
        """Merge two archive datasets preserving unique content.
        
        Args:
            old_data: Existing archive data
            new_data: New archive data to merge
            
        Returns:
            Merged archive data
        """
        merged = old_data.copy()
        
        # Helper to merge lists of items by ID field
        def merge_by_id(old_items: List[Dict], new_items: List[Dict], id_field: str) -> List[Dict]:
            merged_items = {item[id_field]: item for item in old_items}
            # Update/add new items
            for item in new_items:
                if item[id_field] not in merged_items:
                    merged_items[item[id_field]] = item
                else:
                    # If item exists in both, prefer new version
                    merged_items[item[id_field]] = item
            return list(merged_items.values())
        
        # Merge tweets
        if 'tweets' in new_data:
            merged['tweets'] = merge_by_id(
                merged.get('tweets', []),
                new_data['tweets'],
                'id_str'
            )
        
        # Merge community tweets
        if 'community-tweet' in new_data:
            merged['community-tweet'] = merge_by_id(
                merged.get('community-tweet', []),
                new_data['community-tweet'],
                'tweet.id_str'
            )
        
        # Merge note tweets
        if 'note-tweet' in new_data:
            merged['note-tweet'] = merge_by_id(
                merged.get('note-tweet', []),
                new_data['note-tweet'],
                'noteTweet.noteTweetId'
            )
        
        # Merge likes (preserve all unique likes)
        if 'like' in new_data:
            old_likes = {like['like']['tweetId'] for like in merged.get('like', [])}
            merged['like'] = merged.get('like', []) + [
                like for like in new_data['like']
                if like['like']['tweetId'] not in old_likes
            ]
        
        # Merge followers (preserve all unique followers)
        if 'follower' in new_data:
            old_followers = {f['follower']['accountId'] for f in merged.get('follower', [])}
            merged['follower'] = merged.get('follower', []) + [
                f for f in new_data['follower']
                if f['follower']['accountId'] not in old_followers
            ]
        
        # Merge following (preserve all unique following)
        if 'following' in new_data:
            old_following = {f['following']['accountId'] for f in merged.get('following', [])}
            merged['following'] = merged.get('following', []) + [
                f for f in new_data['following']
                if f['following']['accountId'] not in old_following
            ]
        
        # Update profile if present in new data
        if 'profile' in new_data:
            merged['profile'] = new_data['profile']
        
        return merged

def parse_twitter_timestamp(ts: str) -> Optional[datetime]:
    """Convert Twitter's timestamp format to datetime.

    Args:
        ts: Twitter format timestamp string

    Returns:
        Parsed datetime or None if parsing fails
    """
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except ValueError:
        try:
            time_struct = time.strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
            return datetime.fromtimestamp(time.mktime(time_struct), timezone.UTC)
        except ValueError:
            logger.warning("Could not parse timestamp: %s", ts)
            return None

def get_all_accounts() -> List[dict]:
    """Fetch list of all accounts from Supabase.

    Returns:
        List of account dictionaries
    """
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/account",
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error("Failed to fetch accounts: %s", str(e))
        return []

def download_archive(username: str, output_dir: Path) -> Tuple[Optional[Path], Optional[Dict]]:
    """Download a Twitter archive if it doesn't exist locally or has changed.

    Args:
        username: Twitter username
        output_dir: Directory to save archive

    Returns:
        Tuple of (Path to archive or None, metadata dict or None)
    """
    username = username.lower()
    output_file = output_dir / f"{username}_archive.json"
    
    # Get current metadata
    metadata = get_archive_metadata(username)
    if not metadata:
        logger.error("Could not fetch metadata for %s", username)
        return None, None

    # Check if we need to download
    if output_file.exists():
        logger.info("Archive exists for %s, checking if updated", username)
        stored_metadata = _get_stored_metadata(output_file.name)
        if stored_metadata:
            if (stored_metadata.get('etag') == metadata.get('etag') and
                stored_metadata.get('last_modified') == metadata.get('last_modified')):
                logger.info("Archive for %s is up to date", username)
                return output_file, metadata

    # Download new/updated archive
    url = f"{SUPABASE_URL}/storage/v1/object/public/archives/{username}/archive.json"
    try:
        logger.info("Downloading archive for %s", username)
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(response.content)

        logger.info("Successfully downloaded archive for %s", username)
        return output_file, metadata

    except requests.RequestException as e:
        logger.error("Failed to download archive for %s: %s", username, str(e))
        return None, None

def get_archive_metadata(username: str) -> Optional[Dict]:
    """Fetch metadata about an archive from Supabase.
    
    Args:
        username: Twitter username
        
    Returns:
        Dict with metadata including last_modified, size, etc or None if not found
    """
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    
    try:
        response = requests.head(
            f"{SUPABASE_URL}/storage/v1/object/public/archives/{username}/archive.json",
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.ok:
            return {
                'last_modified': response.headers.get('last-modified'),
                'size': response.headers.get('content-length'),
                'etag': response.headers.get('etag')
            }
        return None
    except requests.RequestException as e:
        logger.error(f"Failed to fetch archive metadata for {username}: {str(e)}")
        return None

def main():
    """Process multiple archives into normalized tweet data."""
    parser = argparse.ArgumentParser()
    parser.add_argument('archive_dir', type=Path, help="Directory to store archives")
    parser.add_argument('--usernames', nargs='*', help="Specific usernames to process")
    parser.add_argument(
        '--username-file',
        type=Path,
        help="File containing usernames, one per line"
    )
    parser.add_argument('--all', action='store_true', help="Process all accounts")
    parser.add_argument(
        '--force-reprocess',
        action='store_true',
        help="Reprocess already processed archives"
    )
    args = parser.parse_args()

    usernames = set(args.usernames or [])
    if args.username_file and args.username_file.exists():
        usernames.update(
            line.strip() for line in args.username_file.open()
            if line.strip()
        )

    if args.all or not usernames:
        logger.info("Fetching list of all accounts...")
        accounts = get_all_accounts()
        usernames.update(acc['username'] for acc in accounts)
        logger.info("Found %d accounts to process", len(usernames))

    processor = ArchiveProcessor(args.archive_dir)

    if args.force_reprocess:
        processor.processed_archives.clear()

    for username in usernames:
        archive_path, metadata = download_archive(username, args.archive_dir)
        if archive_path and metadata:
            if processor.should_process_archive(archive_path, metadata):
                tweets = processor.process_file(archive_path)
                if tweets:
                    processor._mark_archive_processed(archive_path, metadata)

    # Process any existing archives not covered above
    existing_archives = set(args.archive_dir.glob('*_archive.json'))
    for path in existing_archives:
        username = path.stem.split('_')[0]
        metadata = get_archive_metadata(username)
        if metadata and processor.should_process_archive(path, metadata):
            tweets = processor.process_file(path)
            if tweets:
                processor._mark_archive_processed(path, metadata)

if __name__ == '__main__':
    main()
