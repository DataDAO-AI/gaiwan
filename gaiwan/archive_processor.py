# archive_processor.py
"""Process Twitter archives into normalized tweet and reply data."""

import argparse
import logging
import time
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Set, List, Optional
from urllib.parse import urlparse, unquote

import orjson
import requests

from models import CanonicalTweet, TweetMetadata
from stats_collector import StatsManager

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
        self.filepath = filepath
        self.batch_size = batch_size
        self.batch = []

    def add(self, item: dict) -> None:
        self.batch.append(item)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.batch:
            return

        mode = 'ab' if self.filepath.exists() else 'wb'
        with self.filepath.open(mode) as f:
            for item in self.batch:
                f.write(orjson.dumps(item))
                f.write(b'\n')
        self.batch.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

class ArchiveProcessor:
    """Processes Twitter archives into normalized tweet and reply data."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.tweets_file = output_dir / "canonical_tweets.jsonl"
        self.replies_file = output_dir / "reply_edges.jsonl"
        self.processed_archives_file = output_dir / "processed_archives.txt"
        self.processed_archives = self._load_processed_archives()
        self.stats_manager = StatsManager(output_dir)

    def _load_processed_archives(self) -> Set[str]:
        if self.processed_archives_file.exists():
            return set(
                line.strip()
                for line in self.processed_archives_file.open()
                if line.strip()
            )
        return set()

    def _mark_archive_processed(self, path: Path) -> None:
        with self.processed_archives_file.open('a') as f:
            f.write(f"{path.name}\n")
        self.processed_archives.add(path.name)

    def should_process_archive(self, path: Path) -> bool:
        return path.name not in self.processed_archives

    def process_file(self, path: Path) -> None:
        """Process a single archive file."""
        if not self.should_process_archive(path):
            logger.info("Skipping already processed archive: %s", path.name)
            return

        try:
            with path.open('rb') as f:
                data = orjson.loads(f.read())

            try:
                user_id = data['account'][0]['account']['accountId']
            except (KeyError, IndexError) as e:
                logger.error("Could not find user ID in %s: %s", path, str(e))
                return

            tweets_writer = BatchWriter(self.tweets_file)
            replies_writer = BatchWriter(self.replies_file)
            processed_tweets = []

            # Process regular tweets
            for tweet_container in data.get('tweets', []):
                tweet_data = tweet_container.get('tweet', {})
                if tweet_data:
                    tweet = CanonicalTweet.from_tweet_data(tweet_data, user_id)
                    tweets_writer.add(tweet.to_dict())
                    processed_tweets.append(tweet)

                    if tweet.reply_to_tweet_id:
                        replies_writer.add({
                            "parent_id": tweet.reply_to_tweet_id,
                            "child_id": tweet.id
                        })

            # Process community tweets
            for tweet_container in data.get('community-tweet', []):
                tweet_data = tweet_container.get('tweet', {})
                if tweet_data:
                    tweet = CanonicalTweet.from_tweet_data(tweet_data, user_id)
                    tweets_writer.add(tweet.to_dict())
                    processed_tweets.append(tweet)

                    if tweet.reply_to_tweet_id:
                        replies_writer.add({
                            "parent_id": tweet.reply_to_tweet_id,
                            "child_id": tweet.id
                        })

            # Process likes
            for like_container in data.get('like', []):
                like_data = like_container.get('like', {})
                if like_data:
                    tweet = CanonicalTweet.from_like_data(like_data, user_id)
                    tweets_writer.add(tweet.to_dict())
                    processed_tweets.append(tweet)

            tweets_writer.flush()
            replies_writer.flush()

            # Generate stats for this archive
            self.stats_manager.process_archive(path, processed_tweets)

            self._mark_archive_processed(path)
            logger.info("Successfully processed archive: %s", path.name)

        except Exception as e:
            logger.error("Error processing %s: %s", path, str(e))

def parse_twitter_timestamp(ts: str) -> Optional[datetime]:
    """Convert Twitter's timestamp format to datetime."""
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except ValueError:
        try:
            time_struct = time.strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
            return datetime.fromtimestamp(time.mktime(time_struct), UTC)
        except ValueError:
            logger.warning("Could not parse timestamp: %s", ts)
            return None

def get_all_accounts() -> List[dict]:
    """Fetch list of all accounts from Supabase."""
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
    """Download a Twitter archive if it doesn't exist locally."""
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
