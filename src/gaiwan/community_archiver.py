"""Download and merge Twitter archives from Supabase storage."""

import argparse
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from tqdm import tqdm
import orjson  # Much faster than json

# Constants
SUPABASE_URL = "https://fabxmporizzqflnftavs.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZhYnh"
    "tcG9yaXp6cWZsbmZ0YXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjIyNDQ5MTIsImV4cCI6Mj"
    "AzNzgyMDkxMn0.UIEJiUNkLsW28tBHmG-RQDW-I5JNlJLt62CSk9D_qG8"
)
REQUEST_TIMEOUT = 10
MAX_WORKERS = 4

logger = logging.getLogger(__name__)

def get_archive_metadata(username: str) -> Optional[Dict]:
    """Fetch metadata about an archive from Supabase."""
    # Try both with and without underscore prefix
    urls_to_try = [
        f"{SUPABASE_URL}/storage/v1/object/public/archives/_{username}/archive.json",
        f"{SUPABASE_URL}/storage/v1/object/public/archives/{username}/archive.json"
    ]
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    for url in urls_to_try:
        try:
            logger.debug(f"Fetching metadata from {url}")
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            logger.debug(f"Got response: {response.status_code}")
            
            if response.ok:
                content = response.content
                return {
                    'size': str(len(content)),
                    'url': url,
                    'content': content,
                    'etag': response.headers.get('etag', ''),
                    'last_modified': response.headers.get('last-modified', '')
                }
            elif response.status_code != 404:  # Only log non-404 errors
                logger.debug(f"Response body: {response.text}")
                logger.error(f"Got {response.status_code} at {url}")
                
        except requests.RequestException as e:
            logger.error(f"Failed to fetch from {url}: {str(e)}")
    
    return None

def merge_archives(old_data: Dict, new_data: Dict) -> Dict:
    """Merge two archives, preserving all tweets and local modifications."""
    # Create a new archive with old data as base
    merged = old_data.copy()
    
    # Collections to merge (from schema)
    collections = [
        'tweets',              # User's own tweets
        'community_tweets',    # Tweets from communities
        'note_tweets',         # Twitter Notes
        'likes',              # Liked tweets
    ]
    
    # Track what we're merging
    stats = {
        collection: {
            'old_count': len(old_data.get(collection, [])),
            'new_count': len(new_data.get(collection, [])),
            'merged_count': 0
        }
        for collection in collections
    }
    
    # Merge each collection
    for collection in collections:
        # Initialize collection if it doesn't exist
        if collection not in merged:
            merged[collection] = []
            
        # Create index of existing items by ID
        item_index = {
            item['tweet']['id_str']: item  # Tweets are wrapped in a 'tweet' object
            for item in merged[collection]
            if 'tweet' in item and 'id_str' in item['tweet']
        }
        
        # Add any new items not in the index
        for item in new_data.get(collection, []):
            if ('tweet' in item and 'id_str' in item['tweet'] and 
                item['tweet']['id_str'] not in item_index):
                merged[collection].append(item)
                item_index[item['tweet']['id_str']] = item
                
        stats[collection]['merged_count'] = len(merged[collection])
    
    # Update metadata while preserving local modifications
    new_meta = new_data.get('_metadata', {})
    old_meta = merged.get('_metadata', {})
    
    merged['_metadata'] = {
        **new_meta,
        'merged_at': datetime.now(timezone.utc).isoformat(),
        'merge_stats': stats,
        # Preserve any local modifications
        **{k:v for k,v in old_meta.items() if k.startswith('local_')}
    }
    
    return merged

def download_archive(username: str, output_dir: Path) -> Tuple[Optional[Path], Optional[Dict]]:
    """Download and merge a Twitter archive."""
    username = username.lower()
    output_file = output_dir / f"{username}_archive.json"
    
    try:
        # Get current metadata and content
        metadata = get_archive_metadata(username)
        if not metadata:
            return None, None

        # Use the content we already downloaded
        content = metadata.pop('content')
        new_data = orjson.loads(content)
        new_data['_metadata'] = metadata
        
        # If file exists, merge instead of overwrite
        if output_file.exists():
            try:
                with open(output_file, 'rb') as f:
                    old_data = orjson.loads(f.read())
                    
                # Check if we need to merge
                old_size = old_data.get('_metadata', {}).get('size')
                if old_size == metadata['size']:
                    logger.debug(f"Archive for {username} unchanged, skipping")
                    return output_file, metadata
                    
                # Merge archives
                merged_data = merge_archives(old_data, new_data)
                new_data = merged_data
                
            except Exception as e:
                logger.error(f"Failed to merge archive for {username}: {str(e)}")
                # Continue with new data if merge fails
        
        # Write the archive
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'wb') as f:
            f.write(orjson.dumps(new_data))

        return output_file, metadata

    except Exception as e:
        logger.error(f"Failed to download archive for {username}: {str(e)}")
        return None, None

def get_all_accounts() -> List[Dict]:
    """Fetch list of all accounts from Supabase."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    url = f"{SUPABASE_URL}/rest/v1/account?select=username"
    try:
        logger.debug(f"Fetching accounts from {url}")
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        logger.debug(f"Got response: {response.status_code}")
        
        if response.ok:
            return response.json()  # Return usernames exactly as they are
        else:
            logger.error(f"Failed to get accounts: {response.status_code}")
            if not response.ok:
                logger.debug(f"Response body: {response.text}")
            return []
            
    except requests.RequestException as e:
        logger.error(f"Failed to get accounts: {str(e)}")
        return []

def read_archive_metadata(archive_file: Path) -> Tuple[str, Optional[Dict]]:
    """Read just the metadata from an archive file using binary search."""
    try:
        username = archive_file.name.replace('_archive.json', '').lower()
        
        with open(archive_file, 'rb') as f:
            # Read in 8KB chunks from the end until we find "_metadata"
            chunk_size = 8192
            f.seek(0, 2)  # Seek to end
            file_size = f.tell()
            
            # Start from end and work backwards
            pos = max(0, file_size - chunk_size)
            while pos >= 0:
                f.seek(pos)
                chunk = f.read(min(chunk_size, file_size - pos))
                meta_pos = chunk.find(b'"_metadata"')
                if meta_pos != -1:
                    # Found metadata marker, now find the complete object
                    abs_pos = pos + meta_pos
                    f.seek(abs_pos)
                    data = f.read(2048)  # Metadata should be smaller than this
                    
                    # Find the start of the object (after the colon)
                    start = data.find(b':') + 1
                    # Find the end of the object (matching closing brace)
                    depth = 0
                    for i, c in enumerate(data[start:]):
                        if c == b'{'[0]:
                            depth += 1
                        elif c == b'}'[0]:
                            depth -= 1
                            if depth == 0:
                                end = start + i + 1
                                metadata = orjson.loads(data[start:end])
                                return username, metadata
                    
                pos = max(0, pos - chunk_size)
                if pos == 0:
                    break
                    
            return username, None
            
    except Exception as e:
        logger.warning(f"Error reading {archive_file}: {str(e)}")
        return username, None

def get_existing_archives(output_dir: Path) -> set[str]:
    """Get set of usernames that have existing archives."""
    try:
        archive_files = list(output_dir.glob("*_archive.json"))
        logger.info(f"Found {len(archive_files)} files matching *_archive.json")
        
        existing = set()
        with ThreadPoolExecutor(max_workers=min(32, len(archive_files))) as executor:
            futures = [executor.submit(read_archive_metadata, f) for f in archive_files]
            
            for future in as_completed(futures):
                username, metadata = future.result()
                if metadata:
                    existing.add(username)
                    logger.debug(f"Loaded metadata for {username}")
            
        return existing
        
    except Exception as e:
        logger.error(f"Error scanning existing archives: {str(e)}")
        return set()

def download_archives(usernames: List[str], output_dir: Path):
    """Download multiple archives in parallel with progress bar."""
    logger.info(f"Checking {len(usernames)} archives...")
    
    # Get existing archives efficiently
    existing = get_existing_archives(output_dir)
    
    # Determine which archives need downloading
    to_download = [u for u in usernames if u.lower() not in existing]
    
    if len(usernames) > len(to_download):
        logger.info(f"{len(usernames) - len(to_download)} archives already up to date")
    
    if to_download:
        logger.info(f"Downloading {len(to_download)} archives...")
        success = []
        failed = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(download_archive, username, output_dir): username 
                for username in to_download
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), 
                             desc="Downloading", unit="archive"):
                username = futures[future]
                try:
                    if future.result():
                        success.append(username)
                    else:
                        failed.append(username)
                except Exception as e:
                    logger.error(f"Failed to download {username}: {str(e)}")
                    failed.append(username)
        
        # Report results
        logger.info(f"\nDownload summary:")
        if success:
            logger.info(f"  - Downloaded {len(success)} archives")
        if failed:
            logger.info(f"  - Failed to download {len(failed)} archives:")
            for username in sorted(failed):
                logger.info(f"    - {username}")

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
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Collect usernames
    usernames = set(u.lower() for u in (args.usernames or []))
    if args.username_file and args.username_file.exists():
        usernames.update(
            line.strip().lower() for line in args.username_file.open()
            if line.strip()
        )

    if args.all or not usernames:
        usernames.update(get_all_accounts())
        logger.info(f"Found {len(usernames)} accounts to process")

    # Download archives
    download_archives(list(usernames), args.archive_dir)

if __name__ == '__main__':
    main()
