"""Process Twitter archives into normalized tweet and reply data."""

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set, Dict, Any
import json
import os

import requests
import pytz

from gaiwan.models import CanonicalTweet
from gaiwan.stats_collector import StatsManager

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
        self.stats_manager = StatsManager(output_dir)

    def _load_processed_archives(self) -> Set[str]:
        """Load set of already processed archive filenames.

        Returns:
            Set of processed archive filenames
        """
        if self.processed_archives_file.exists():
            return set(
                line.strip()
                for line in self.processed_archives_file.open()
                if line.strip()
            )
        return set()

    def _mark_archive_processed(self, path: Path) -> None:
        """Mark archive as processed.

        Args:
            path: Path to processed archive
        """
        with self.processed_archives_file.open('a') as f:
            f.write(f"{path.name}\n")
        self.processed_archives.add(path.name)

    def should_process_archive(self, path: Path) -> bool:
        """Check if archive needs processing.

        Args:
            path: Path to archive file

        Returns:
            True if archive should be processed
        """
        return path.name not in self.processed_archives

    def process_file(self, archive_path: Path) -> List[CanonicalTweet]:
        """Process a single archive file."""
        try:
            # Extract username from filename
            username = archive_path.stem.split('_')[0]
            logger.info(f"Processing archive for {username}")
            
            with open(archive_path) as f:
                data = json.load(f)
                
            tweets = []
            
            # Handle community tweets
            if 'community-tweet' in data:
                for tweet_wrapper in data['community-tweet']:
                    if 'tweet' in tweet_wrapper:
                        tweet = CanonicalTweet.from_tweet_data(
                            tweet_wrapper['tweet'], 
                            source_type='community'
                        )
                        if tweet and tweet.id:  # Ensure tweet has an ID
                            tweets.append(tweet)
                            
            # Handle note tweets
            if 'note-tweet' in data:
                for note_wrapper in data['note-tweet']:
                    if 'noteTweet' in note_wrapper:
                        tweet = CanonicalTweet.from_note_data(note_wrapper, username)
                        if tweet and tweet.id:  # Ensure tweet has an ID
                            tweets.append(tweet)
                            
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
        for note_tweet in note_tweets_data:
            try:
                note_data = note_tweet.get('noteTweet', {})
                updated_at = CanonicalTweet.parse_twitter_datetime(note_data['updatedAt'])
                
                # Now both datetimes are timezone-aware for comparison
                # ... rest of the processing ...
                
            except Exception as e:
                self.logger.error(f"Error processing note tweet {i} for {username}: {str(e)}\nNote data: {json.dumps(note_tweet, indent=2)[:500]}...")

    def _process_note_tweets(self, note_tweets: List[Dict], username: str) -> List[Dict]:
        """Process note tweets from the archive."""
        processed_notes = []
        for i, note_tweet in enumerate(note_tweets):
            try:
                # Process note tweet
                processed_note = self._process_single_note_tweet(note_tweet)
                if processed_note:
                    processed_notes.append(processed_note)
            except Exception as e:
                self.logger.error(f"Error processing note tweet {i} for {username}: {str(e)}\nNote data: {json.dumps(note_tweet, indent=2)[:500]}...")
                continue
        return processed_notes

    def process_tweet(self, tweet_data: dict, source_type: str) -> Optional[CanonicalTweet]:
        """Process any tweet type into canonical form."""
        try:
            if source_type == "community_tweet":
                # Keep community_tweet as source_type
                return CanonicalTweet.from_tweet_data(tweet_data, source_type="community_tweet")
            elif source_type == "note":
                return CanonicalTweet.from_note_data(tweet_data)
            else:
                return CanonicalTweet.from_tweet_data(tweet_data, source_type="tweet")
        except Exception as e:
            logger.error(f"Error processing {source_type}: {str(e)}\nTweet data: {json.dumps(tweet_data, indent=2)[:500]}...")
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

def download_archive(username: str, output_dir: Path) -> Optional[Path]:
    """Download a Twitter archive if it doesn't exist locally.

    Args:
        username: Twitter username
        output_dir: Directory to save archive

    Returns:
        Path to downloaded archive or None if download fails
    """
    username = username.lower()
    output_file = output_dir / f"{username}_archive.json"

    if output_file.exists():
        logger.info("Archive for %s already exists, skipping download", username)
        return output_file

    url = f"{SUPABASE_URL}/storage/v1/object/public/archives/{username}/archive.json"

    try:
        logger.info("Downloading archive for %s", username)
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(response.content)

        logger.info("Successfully downloaded archive for %s", username)
        return output_file

    except requests.RequestException as e:
        logger.error("Failed to download archive for %s: %s", username, str(e))
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
        archive_path = download_archive(username, args.archive_dir)
        if archive_path:
            processor.process_file(archive_path)

    existing_archives = set(args.archive_dir.glob('*_archive.json'))
    for path in existing_archives:
        processor.process_file(path)

if __name__ == '__main__':
    main()
