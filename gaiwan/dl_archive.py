"""Twitter archive downloader for community-archive.org.

This script downloads and merges Twitter archives from community-archive.org,
tracking the latest state of each user's archive and avoiding duplicate downloads.
"""

from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Dict

import requests

# Constants
SUPABASE_URL = "https://fabxmporizzqflnftavs.supabase.co"
ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZhYn"
    "htcG9yaXp6cWZsbmZ0YXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjIyNDQ5MTIsImV4cCI6"
    "MjAzNzgyMDkxMn0.UIEJiUNkLsW28tBHmG-RQDW-I5JNlJLt62CSk9D_qG8"
)
OUTPUT_DIR = Path("twitter_archives")
METADATA_FILE = OUTPUT_DIR / "metadata.json"
REQUEST_TIMEOUT = 30  # seconds

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("archive_fetcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ArchiveManager:
    """Manages the downloading, merging, and tracking of Twitter archives."""

    def __init__(self):
        """Initialize the archive manager and load existing metadata."""
        self.metadata = self.load_metadata()

    def load_metadata(self) -> Dict:
        """Load metadata from file or create new if doesn't exist."""
        if METADATA_FILE.exists():
            try:
                with open(METADATA_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Corrupt metadata file, creating new")
                return {}
        return {}

    def save_metadata(self):
        """Save metadata to file."""
        with open(METADATA_FILE, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2)

    def get_latest_dates(self, archive_data: Dict) -> Dict[str, str]:
        """Get latest dates for each section of the archive."""
        latest_dates = {}

        if "tweets" in archive_data:
            tweet_dates = [
                tweet["tweet"].get("created_at")
                for tweet in archive_data["tweets"]
                if "tweet" in tweet and tweet["tweet"].get("created_at")
            ]
            if tweet_dates:
                latest_dates["tweets"] = max(tweet_dates)

        if "like" in archive_data:
            like_dates = [
                like["like"].get("created_at")
                for like in archive_data["like"]
                if "like" in like and like["like"].get("created_at")
            ]
            if like_dates:
                latest_dates["likes"] = max(like_dates)

        logger.debug(
            "Found dates - Tweets: %d, Likes: %d",
            len(tweet_dates) if "tweets" in archive_data else 0,
            len(like_dates) if "like" in archive_data else 0
        )

        return latest_dates
    def should_update(self, username: str, new_dates: Dict[str, str]) -> bool:
        """Check if we should update based on latest dates."""
        if username not in self.metadata:
            return True

        if not new_dates:
            logger.warning("No valid dates found in new archive for %s", username)
            return False

        current = self.metadata[username].get("latest_dates", {})

        for section, new_date in new_dates.items():
            current_date = current.get(section)
            if current_date is None or new_date > current_date:
                logger.info(
                    "Update needed for %s - %s: %s -> %s",
                    username, section, current_date, new_date
                )
                return True

        return False

    def merge_archives(self, old_data: Dict, new_data: Dict) -> Dict:
        """Merge two archives, keeping all data from both."""
        merged = old_data.copy()

        sections = [
            "tweets", "like", "following", "follower",
            "community-tweet", "note-tweet"
        ]

        for section in sections:
            if section in new_data:
                if section not in merged:
                    merged[section] = []

                existing_ids = set()
                section_key = section.replace("-", "")
                for item in merged[section]:
                    if section_key in item and "id_str" in item[section_key]:
                        existing_ids.add(item[section_key]["id_str"])

                for item in new_data[section]:
                    if (
                        section_key in item
                        and "id_str" in item[section_key]
                        and item[section_key]["id_str"] not in existing_ids
                    ):
                        merged[section].append(item)

        merged["account"] = new_data.get("account", merged.get("account", {}))
        merged["profile"] = new_data.get("profile", merged.get("profile", {}))

        for section in sections:
            if section in merged:
                logger.debug("  %s: %d items", section, len(merged[section]))

        return merged

    def get_existing_archive(self, username: str) -> Dict:
        """Get the most recent existing archive for a user."""
        if username not in self.metadata:
            return {}

        archive_file = self.metadata[username].get("latest_file")
        if not archive_file:
            return {}

        try:
            with open(OUTPUT_DIR / archive_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error("Error reading existing archive for %s: %s", username, e)
            return {}


def setup_directory():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Ensuring output directory exists: %s", OUTPUT_DIR)


def get_users() -> list:
    """Get list of users from Supabase."""
    headers = {
        "apikey": ANON_KEY,
        "Authorization": f"Bearer {ANON_KEY}"
    }

    logger.info("Fetching user list from Supabase")
    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/account",
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        users = response.json()
        logger.info("Successfully retrieved %d users", len(users))
        return users
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching users: %s", e)
        return []


def get_archive(username: str) -> Dict:
    """Get archive for a specific user."""
    url = f"{SUPABASE_URL}/storage/v1/object/public/archives/{username.lower()}/archive.json"
    logger.info("Fetching archive for user: %s", username)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        archive_data = response.json()

        logger.debug("Archive structure for %s:", username)
        logger.debug("Top-level keys: %s", list(archive_data.keys()))

        sections = [
            "tweets", "like", "following", "follower",
            "community-tweet", "note-tweet"
        ]
        for section in sections:
            if section in archive_data:
                section_data = archive_data[section]
                logger.debug("%s type: %s", section, type(section_data))
                logger.debug("%s length: %d", section, len(section_data))
                if section_data:
                    sample = section_data[0]
                    logger.debug(
                        "Sample %s keys: %s",
                        section,
                        list(sample.keys()) if sample else "empty"
                    )

        return archive_data
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching archive for %s: %s", username, e)
        return None


def save_archive(username: str, archive_data: Dict, timestamp: datetime) -> str:
    """Save archive to file and return filename."""
    filename = f"{username}-{timestamp.isoformat()}.json"
    filepath = OUTPUT_DIR / filename

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, ensure_ascii=False, indent=2)
        logger.info("Successfully saved archive for %s to %s", username, filepath)
        return filename
    except IOError as e:
        logger.error("Error saving archive for %s: %s", username, e)
        return None


def main():
    """Main execution function."""
    logger.info("Starting archive fetch process")
    setup_directory()

    archive_manager = ArchiveManager()
    users = get_users()

    if not users:
        logger.error("No users found, exiting")
        return

    for user in users:
        username = user["username"]
        new_archive = get_archive(username)

        if not new_archive:
            continue

        latest_dates = archive_manager.get_latest_dates(new_archive)

        if not archive_manager.should_update(username, latest_dates):
            logger.info("Skipping %s - no new content", username)
            continue

        existing_archive = archive_manager.get_existing_archive(username)
        merged_archive = archive_manager.merge_archives(existing_archive, new_archive)

        timestamp = datetime.now()
        filename = save_archive(username, merged_archive, timestamp)

        if filename:
            archive_manager.metadata[username] = {
                "latest_file": filename,
                "latest_dates": latest_dates,
                "stats": {
                    "num_tweets": len(merged_archive.get("tweets", [])),
                    "num_likes": len(merged_archive.get("like", [])),
                    "num_following": len(merged_archive.get("following", [])),
                    "num_followers": len(merged_archive.get("follower", []))
                },
                "last_updated": timestamp.isoformat()
            }
            archive_manager.save_metadata()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # pylint: disable=broad-except
        logger.critical("Unexpected error: %s", e, exc_info=True)
