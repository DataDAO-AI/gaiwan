"""Download and merge Twitter archives from Supabase storage."""

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

# Constants
SUPABASE_URL = "https://fabxmporizzqflnftavs.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZhYnh"
    "tcG9yaXp6cWZsbmZ0YXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjIyNDQ5MTIsImV4cCI6Mj"
    "AzNzgyMDkxMn0.UIEJiUNkLsW28tBHmG-RQDW-I5JNlJLt62CSk9D_qG8"
)
REQUEST_TIMEOUT = 10  # Reduced timeout
MAX_WORKERS = 4  # Parallel downloads

logger = logging.getLogger(__name__)

def get_archive_metadata(username: str) -> Optional[Dict]:
    """Fetch metadata about an archive from Supabase."""
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

def download_archive(username: str, output_dir: Path) -> Tuple[Optional[Path], Optional[Dict]]:
    """Download a Twitter archive if it doesn't exist locally or has changed."""
    username = username.lower()
    output_file = output_dir / f"{username}_archive.json"
    
    try:
        # Get current metadata
        metadata = get_archive_metadata(username)
        if not metadata:
            logger.warning(f"No archive found for {username}")
            return None, None

        # Check if we need to download
        if output_file.exists():
            with open(output_file) as f:
                try:
                    stored_metadata = json.load(f)['_metadata']
                    if stored_metadata.get('etag') == metadata.get('etag'):
                        logger.info(f"Archive for {username} is up to date")
                        return output_file, metadata
                except (json.JSONDecodeError, KeyError):
                    pass

        # Download new/updated archive
        url = f"{SUPABASE_URL}/storage/v1/object/public/archives/{username}/archive.json"
        logger.info(f"Downloading archive for {username} ({metadata.get('size', '?')} bytes)")
        
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # Add metadata to archive
        data = response.json()
        data['_metadata'] = metadata
        
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(data, f)

        logger.info(f"Successfully downloaded archive for {username}")
        return output_file, metadata

    except requests.RequestException as e:
        logger.error(f"Failed to download archive for {username}: {str(e)}")
        return None, None

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
        logger.error(f"Failed to fetch accounts: {str(e)}")
        return []

def download_archives(usernames: List[str], output_dir: Path):
    """Download multiple archives in parallel with progress bar."""
    logger.info(f"Preparing to download {len(usernames)} archives")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Start downloads
        future_to_username = {
            executor.submit(download_archive, username, output_dir): username
            for username in usernames
        }
        
        # Track progress
        with tqdm(total=len(usernames), desc="Downloading archives") as pbar:
            for future in as_completed(future_to_username):
                username = future_to_username[future]
                try:
                    result = future.result()
                    if result[0]:  # If download successful
                        pbar.write(f"✓ {username}")
                    else:
                        pbar.write(f"✗ {username}")
                except Exception as e:
                    pbar.write(f"Error downloading {username}: {str(e)}")
                pbar.update(1)

def main():
    """Download Twitter archives from Supabase."""
    parser = argparse.ArgumentParser(description="Download Twitter archives from Supabase")
    parser.add_argument('archive_dir', type=Path, help="Directory to store archives")
    parser.add_argument('--usernames', nargs='*', help="Specific usernames to download")
    parser.add_argument(
        '--username-file',
        type=Path,
        help="File containing usernames, one per line"
    )
    parser.add_argument('--all', action='store_true', help="Download all accounts")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Collect usernames
    usernames = set(args.usernames or [])
    if args.username_file and args.username_file.exists():
        usernames.update(
            line.strip() for line in args.username_file.open()
            if line.strip()
        )

    if args.all or not usernames:
        accounts = get_all_accounts()
        usernames.update(acc['username'] for acc in accounts)
        logger.info(f"Found {len(usernames)} accounts to process")

    # Download archives
    download_archives(list(usernames), args.archive_dir)

if __name__ == '__main__':
    main()
