"""Process Twitter archives into normalized tweet and reply data."""

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set
import json

import requests

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
            stats = {
                'tweets': 0,
                'community_tweets': 0,
                'note_tweets': 0,
                'likes': 0
            }
            
            # Handle regular tweets
            if 'tweet' in data:
                tweet_list = data['tweet']
                if isinstance(tweet_list, dict):
                    tweet_list = [tweet_list]
                    
                for tweet_data in tweet_list:
                    tweet = CanonicalTweet.from_tweet_data(tweet_data, username)
                    tweets.append(tweet)
                    self.stats_manager.process_tweet(tweet)
                    if tweet.source_type == 'community_tweet':
                        stats['community_tweets'] += 1
                    else:
                        stats['tweets'] += 1
                    
            # Handle note tweets
            if 'noteTweet' in data:
                note_list = data['noteTweet']
                if isinstance(note_list, dict):
                    note_list = [note_list]
                    
                for note_data in note_list:
                    tweet = CanonicalTweet.from_note_data(note_data, username)
                    tweets.append(tweet)
                    self.stats_manager.process_tweet(tweet)
                    stats['note_tweets'] += 1
                    
            # Handle likes
            if 'like' in data:
                like_list = data['like']
                if isinstance(like_list, dict):
                    like_list = [like_list]
                    
                for like_entry in like_list:
                    if 'like' in like_entry:
                        tweet = CanonicalTweet.from_like_data(like_entry, username)
                        tweets.append(tweet)
                        self.stats_manager.process_tweet(tweet)
                        stats['likes'] += 1
            
            # Log summary statistics
            logger.info(
                f"Processed {username} archive: "
                f"{stats['tweets']} tweets, "
                f"{stats['community_tweets']} community tweets, "
                f"{stats['note_tweets']} note tweets, "
                f"{stats['likes']} likes"
            )
            
            # Save stats after processing all tweets
            self.stats_manager.save_stats(username)
            self._mark_archive_processed(archive_path)
            return tweets
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return []

    def process_archive(self, archive_path: Path) -> List[CanonicalTweet]:
        """Process a Twitter archive directory."""
        if archive_path in self.processed_archives:
            logger.warning(f"Archive {archive_path} has already been processed")
            return []

        tweets = []
        tweet_js = archive_path / "tweet.js"
        
        if not tweet_js.exists():
            logger.error(f"No tweet.js found in {archive_path}")
            return []

        try:
            with open(tweet_js) as f:
                content = f.read()
                logger.debug(f"Raw content: {content[:100]}...")  
                
                # Remove JS variable assignment more carefully
                if content.startswith("window.YTD.tweet.part0 = "):
                    content = content[len("window.YTD.tweet.part0 = "):]
                    logger.debug("Removed JS prefix")
                else:
                    logger.debug("No JS prefix found")
                    
                data = json.loads(content)
                logger.debug(f"Parsed data type: {type(data)}")
                logger.debug(f"Parsed data: {data}")

            # Handle both direct list and dict with 'tweet' key
            if isinstance(data, dict) and 'tweet' in data:
                tweet_list = data['tweet']
            elif isinstance(data, list):
                tweet_list = data
            else:
                logger.error(f"Unexpected data structure: {type(data)}")
                return []

            # Try to get username from the first tweet
            try:
                first_tweet = tweet_list[0]
                if isinstance(first_tweet, dict) and 'tweet' in first_tweet:
                    first_tweet = first_tweet['tweet']
                username = first_tweet['user']['screen_name'].lower()
            except (IndexError, KeyError):
                username = archive_path.stem.replace('_archive', '').lower()
                logger.warning(f"Could not find username in {archive_path}, using filename: {username}")

            for tweet_data in data:
                if not isinstance(tweet_data, dict) or 'tweet' not in tweet_data:
                    continue
                tweet = CanonicalTweet.from_tweet_data(tweet_data['tweet'], username)
                tweets.append(tweet)

            self.processed_archives.add(archive_path)
            return tweets

        except Exception as e:
            logger.error(f"Error processing archive {archive_path}: {e}")
            return []

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
