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

                # Create set of existing IDs
                existing_ids = set()
                section_key = section.replace("-", "")
                for item in merged[section]:
                    if section_key in item and "id_str" in item[section_key]:
                        existing_ids.add(item[section_key]["id_str"])

                # Add new items that don't exist
                for item in new_data[section]:
                    if (section_key in item and
                        "id_str" in item[section_key] and
                        item[section_key]["id_str"] not in existing_ids):
                        merged[section].append(item)
                        existing_ids.add(item[section_key]["id_str"])

        # Update profile and account info
        if "profile" in new_data:
            merged["profile"] = new_data["profile"]
        if "account" in new_data:
            merged["account"] = new_data["account"]

        # Log section sizes
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

        # Log raw response details
        logger.debug("Response status: %d", response.status_code)
        logger.debug("Response headers: %s", dict(response.headers))
        logger.debug("Response size: %d bytes", len(response.content))

        # Parse the JSON response
        archive_data = response.json()

        # Log the raw structure
        logger.debug("Raw archive structure:")
        logger.debug("Top-level keys: %s", list(archive_data.keys()))

        # Debug first items of each section
        for section in ["tweets", "like", "following", "follower", "community-tweet", "note-tweet"]:
            if section in archive_data and archive_data[section]:
                first_item = archive_data[section][0]
                logger.debug("First %s item: %s", section, json.dumps(first_item, indent=2))

                # If it's an empty list or not a list, log that
                if not isinstance(archive_data[section], list):
                    logger.warning(
                        "%s is not a list, it's a %s",
                        section,
                        type(archive_data[section])
                    )
                elif not archive_data[section]:
                    logger.warning("%s is an empty list", section)

        # Initialize the normalized structure
        result = {
            "tweets": [],
            "like": [],
            "following": [],
            "follower": [],
            "community-tweet": [],
            "note-tweet": [],
            "account": archive_data.get("account", {}),
            "profile": archive_data.get("profile", {})
        }

        # Copy raw arrays directly
        for section in ["tweets", "like", "following", "follower", "community-tweet", "note-tweet"]:
            if section in archive_data and isinstance(archive_data[section], list):
                result[section] = archive_data[section]
                logger.debug(
                    "%s: copied %d items directly",
                    section,
                    len(archive_data[section])
                )
                if archive_data[section]:
                    logger.debug(
                        "Sample %s: %s",
                        section,
                        json.dumps(archive_data[section][0], indent=2)
                    )

        # Log final counts
        logger.debug("Final result counts:")
        for section, data in result.items():
            if isinstance(data, list):
                logger.debug("%s: %d items", section, len(data))
            else:
                logger.debug("%s: %s", section, "present" if data else "empty")

        return result

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching archive for %s: %s", username, e)
        return None
    except json.JSONDecodeError as e:
        logger.error("Error parsing JSON for %s: %s", username, e)
        logger.debug("Raw response content: %s", response.text[:1000])  # First 1000 chars
        return None
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected error processing archive for %s: %s", username, e)
        return None

def save_archive(username: str, archive_data: Dict, timestamp: datetime) -> str:
    """Save archive to file and return filename."""
    # Replace colons with underscores in timestamp for valid filename
    timestamp_str = timestamp.isoformat().replace(':', '_')
    filename = f"{username}-{timestamp_str}.json"
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
