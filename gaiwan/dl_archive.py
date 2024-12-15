import requests
import json
import os
from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, Any

# Constants
SUPABASE_URL = "https://fabxmporizzqflnftavs.supabase.co"
ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZhYnhtcG9yaXp6cWZsbmZ0YXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjIyNDQ5MTIsImV4cCI6MjAzNzgyMDkxMn0.UIEJiUNkLsW28tBHmG-RQDW-I5JNlJLt62CSk9D_qG8"
OUTPUT_DIR = Path("twitter_archives")
METADATA_FILE = OUTPUT_DIR / "metadata.json"

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('archive_fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ArchiveManager:
    def __init__(self):
        self.metadata = self.load_metadata()

    def load_metadata(self) -> Dict:
        """Load metadata from file or create new if doesn't exist"""
        if METADATA_FILE.exists():
            try:
                with open(METADATA_FILE, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Corrupt metadata file, creating new")
                return {}
        return {}

    def save_metadata(self):
        """Save metadata to file"""
        with open(METADATA_FILE, 'w') as f:
            json.dump(self.metadata, f, indent=2)

    def get_latest_dates(self, archive_data: Dict) -> Dict[str, str]:
        """Get latest dates for each section of the archive"""
        latest_dates = {}
        
        # For tweets
        if 'tweets' in archive_data:
            tweet_dates = [
                tweet['tweet'].get('created_at') 
                for tweet in archive_data['tweets']
                if 'tweet' in tweet and tweet['tweet'].get('created_at') is not None
            ]
            if tweet_dates:
                latest_dates['tweets'] = max(tweet_dates)

        # For likes
        if 'like' in archive_data:
            like_dates = [
                like['like'].get('created_at') 
                for like in archive_data['like']
                if 'like' in like and like['like'].get('created_at') is not None
            ]
            if like_dates:
                latest_dates['likes'] = max(like_dates)

        logger.debug(f"Found dates - Tweets: {len(tweet_dates) if 'tweets' in archive_data else 0}, "
                    f"Likes: {len(like_dates) if 'like' in archive_data else 0}")
        
        return latest_dates

    def should_update(self, username: str, new_dates: Dict[str, str]) -> bool:
        """Check if we should update based on latest dates"""
        if username not in self.metadata:
            return True

        if not new_dates:
            logger.warning(f"No valid dates found in new archive for {username}")
            return False

        current = self.metadata[username].get('latest_dates', {})
        
        for section, new_date in new_dates.items():
            current_date = current.get(section)
            if current_date is None or new_date > current_date:
                logger.info(f"Update needed for {username} - {section}: {current_date} -> {new_date}")
                return True
        
        return False

    def merge_archives(self, old_data: Dict, new_data: Dict) -> Dict:
        """Merge two archives, keeping all data from both"""
        merged = old_data.copy()
        
        # For all list sections
        for section in ['tweets', 'like', 'following', 'follower', 'community-tweet', 'note-tweet']:
            if section in new_data:
                if section not in merged:
                    merged[section] = []
                
                # Create a set of existing IDs
                existing_ids = set()
                section_key = section.replace('-', '')  # handle 'community-tweet' -> 'communitytweet'
                for item in merged[section]:
                    if section_key in item and 'id_str' in item[section_key]:
                        existing_ids.add(item[section_key]['id_str'])
                
                # Add only new items
                for item in new_data[section]:
                    if (section_key in item and 
                        'id_str' in item[section_key] and 
                        item[section_key]['id_str'] not in existing_ids):
                        merged[section].append(item)

        # Always take the newest account and profile info
        merged['account'] = new_data.get('account', merged.get('account', {}))
        merged['profile'] = new_data.get('profile', merged.get('profile', {}))
        
        # Log some stats about the merge
        logger.debug(f"Merge stats:")
        for section in ['tweets', 'like', 'following', 'follower', 'community-tweet', 'note-tweet']:
            if section in merged:
                count = len(merged[section])
                logger.debug(f"  {section}: {count} items")
        
        return merged

    def get_existing_archive(self, username: str) -> Dict:
        """Get the most recent existing archive for a user"""
        if username not in self.metadata:
            return {}

        archive_file = self.metadata[username].get('latest_file')
        if not archive_file:
            return {}

        try:
            with open(OUTPUT_DIR / archive_file, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error reading existing archive for {username}: {e}")
            return {}

def setup_directory():
    """Create output directory if it doesn't exist"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Ensuring output directory exists: {OUTPUT_DIR}")

def get_users():
    """Get list of users from Supabase"""
    headers = {
        "apikey": ANON_KEY,
        "Authorization": f"Bearer {ANON_KEY}"
    }
    
    logger.info("Fetching user list from Supabase")
    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/account",
            headers=headers
        )
        response.raise_for_status()
        users = response.json()
        logger.info(f"Successfully retrieved {len(users)} users")
        return users
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching users: {e}")
        return []

def get_archive(username: str) -> Dict:
    """Get archive for a specific user"""
    url = f"{SUPABASE_URL}/storage/v1/object/public/archives/{username.lower()}/archive.json"
    logger.info(f"Fetching archive for user: {username}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        archive_data = response.json()
        
        # Debug logging
        logger.debug(f"Archive structure for {username}:")
        logger.debug(f"Top-level keys: {list(archive_data.keys())}")
        
        for section in ['tweets', 'like', 'following', 'follower', 'community-tweet', 'note-tweet']:
            if section in archive_data:
                section_data = archive_data[section]
                logger.debug(f"{section} type: {type(section_data)}")
                logger.debug(f"{section} length: {len(section_data)}")
                if section_data:
                    sample = section_data[0]
                    logger.debug(f"Sample {section} keys: {list(sample.keys()) if sample else 'empty'}")
        
        return archive_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching archive for {username}: {e}")
        return None

def save_archive(username: str, archive_data: Dict, timestamp: datetime) -> str:
    """Save archive to file and return filename"""
    filename = f"{username}-{timestamp.isoformat()}.json"
    filepath = OUTPUT_DIR / filename
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(archive_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Successfully saved archive for {username} to {filepath}")
        return filename
    except IOError as e:
        logger.error(f"Error saving archive for {username}: {e}")
        return None

def main():
    logger.info("Starting archive fetch process")
    setup_directory()
    
    archive_manager = ArchiveManager()
    users = get_users()
    
    if not users:
        logger.error("No users found, exiting")
        return
    
    for user in users:
        username = user['username']
        new_archive = get_archive(username)
        
        if not new_archive:
            continue

        latest_dates = archive_manager.get_latest_dates(new_archive)
        
        if not archive_manager.should_update(username, latest_dates):
            logger.info(f"Skipping {username} - no new content")
            continue

        existing_archive = archive_manager.get_existing_archive(username)
        merged_archive = archive_manager.merge_archives(existing_archive, new_archive)
        
        timestamp = datetime.now()
        filename = save_archive(username, merged_archive, timestamp)
        
        if filename:
            archive_manager.metadata[username] = {
                'latest_file': filename,
                'latest_dates': latest_dates,
                'stats': {
                    'num_tweets': len(merged_archive.get('tweets', [])),
                    'num_likes': len(merged_archive.get('like', [])),
                    'num_following': len(merged_archive.get('following', [])),
                    'num_followers': len(merged_archive.get('follower', []))
                },
                'last_updated': timestamp.isoformat()
            }
            archive_manager.save_metadata()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)