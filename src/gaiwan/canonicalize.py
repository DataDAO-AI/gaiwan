"""Canonicalize Twitter archive data into a unified timeline."""

import argparse
import json
import logging
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
import time
import jsonschema
from gaiwan.schema_generator import generate_schema
from tqdm import tqdm
import orjson
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import pyarrow as pa
import pyarrow.parquet as pq
import re
import numpy as np

logger = logging.getLogger(__name__)

# Constants
MAX_WORKERS = min(32, multiprocessing.cpu_count())  # Use actual core count, capped at 32

@dataclass(frozen=True)
class TweetID:
    """Twitter snowflake ID.
    
    Format: 64 bits
    - 42 bits: milliseconds since Twitter epoch (2010-11-04T01:42:54.657Z)
    - 10 bits: machine ID
    - 12 bits: sequence number
    """
    _id: int
    
    def __init__(self, id_val: Union[str, int]):
        """Initialize with proper validation."""
        # Convert to string first to check length
        id_str = str(id_val)
        if not id_str.isdigit() or len(id_str) > 19:  # Max 19 digits for 64-bit int
            raise ValueError(f"Invalid Twitter ID: {id_str}")
        object.__setattr__(self, '_id', int(id_str))
    
    def __hash__(self) -> int:
        return hash(self._id)
    
    @classmethod
    def from_str(cls, id_str: str) -> 'TweetID':
        """Create TweetID from string."""
        return cls(id_str)
        
    @classmethod
    def from_any(cls, id_val: Union[str, int, 'TweetID']) -> 'TweetID':
        """Create TweetID from any valid input type."""
        if isinstance(id_val, TweetID):
            return id_val
        if isinstance(id_val, int):
            return cls(id_val)
        return cls.from_str(str(id_val))
        
    @property
    def timestamp(self) -> datetime:
        """Extract timestamp from snowflake."""
        ms = (self._id >> 22) + 1288834974657
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc)
    
    def __lt__(self, other: 'TweetID') -> bool:
        return self._id < other._id
    
    def __le__(self, other: 'TweetID') -> bool:
        return self._id <= other._id
    
    def __gt__(self, other: 'TweetID') -> bool:
        return self._id > other._id
    
    def __ge__(self, other: 'TweetID') -> bool:
        return self._id >= other._id
    
    def __str__(self) -> str:
        return str(self._id)
    
    def __repr__(self) -> str:
        return f"TweetID({self._id})"

@dataclass
class CanonicalTweet:
    """Normalized tweet format."""
    id: TweetID
    text: str  # Required field, empty string is valid
    _created_at: datetime
    author_username: Optional[str] = None
    retweet_count: Optional[int] = None
    in_reply_to_status_id: Optional[TweetID] = None
    in_reply_to_username: Optional[str] = None
    quoted_tweet_id: Optional[TweetID] = None
    entities: Optional[dict] = None
    likers: Set[str] = field(default_factory=set)
    reply_ids: Set[TweetID] = field(default_factory=set)

    def __post_init__(self):
        """Ensure created_at is set, deriving from ID if needed."""
        if not self._created_at:
            object.__setattr__(self, '_created_at', self.id.timestamp)

    @property
    def created_at(self) -> datetime:
        """Get creation time."""
        return self._created_at

    @classmethod
    def from_any_tweet(cls, data: Dict, username: str) -> Optional['CanonicalTweet']:
        """Create canonical tweet from any tweet type."""
        try:
            quoted_id = None  # Initialize at the top level
            if 'tweet' in data:
                data = data['tweet']
                text = data.get('full_text', data.get('text', ''))
                tweet_id = TweetID.from_str(data['id_str'])
                
                # Extract quoted tweet ID from URLs if not already found through metadata
                is_quote = data.get('is_quote_status', False)
                
                # Check URLs for quoted tweets
                for url in data.get('entities', {}).get('urls', []):
                    expanded_url = url.get('expanded_url', '')
                    # Match twitter.com status URLs
                    if 'twitter.com' in expanded_url and '/status/' in expanded_url:
                        try:
                            # Extract status ID from URL and take only numeric part
                            status_part = expanded_url.split('/status/')[-1].split('/')[0]
                            if match := re.match(r'(\d+)', status_part):
                                status_id = match.group(1)
                                # Only use if it's not a self-quote and is a valid ID
                                if status_id != data['id_str'] and len(status_id) <= 20:
                                    quoted_id = TweetID.from_str(status_id)
                                    break
                        except Exception as e:
                            logger.warning(f"Failed to extract quoted tweet ID from URL {expanded_url}: {e}")

                # If no URL-based quote found, try metadata fields
                if not quoted_id and is_quote:
                    quoted_status_id_str = data.get('quoted_status_id_str')
                    if quoted_status_id_str:
                        quoted_id = TweetID.from_any(quoted_status_id_str)
                    else:
                        logger.warning(f"Quote tweet {data['id_str']} missing quoted_status_id_str")
                
                # Convert entities with proper integer types
                raw_entities = data.get('entities', {})
                entities = {
                    'hashtags': [
                        {
                            'text': str(h['text']),
                            'indices': [int(idx) for idx in h['indices']]
                        }
                        for h in raw_entities.get('hashtags', [])
                    ],
                    'urls': [
                        {
                            'url': str(u['url']),
                            'expanded_url': str(u.get('expanded_url', u['url'])),
                            'display_url': str(u.get('display_url', u['url'])),
                            'indices': [int(idx) for idx in u['indices']]
                        }
                        for u in raw_entities.get('urls', [])
                    ],
                    'user_mentions': [
                        {
                            'screen_name': str(m['screen_name']),
                            'indices': [int(idx) for idx in m['indices']],
                            'id': int(m['id'])
                        }
                        for m in raw_entities.get('user_mentions', [])
                    ],
                    'media': [
                        {
                            'url': str(m['url']),
                            'expanded_url': str(m['expanded_url']),
                            'media_url': str(m['media_url']),
                            'type': str(m['type']),
                            'display_url': str(m['display_url']),
                            'id': int(m['id']),
                            'sizes': {
                                size: {
                                    'w': int(info['w']),
                                    'h': int(info['h']),
                                    'resize': str(info['resize'])
                                }
                                for size, info in m['sizes'].items()
                            }
                        }
                        for m in raw_entities.get('media', [])
                    ]
                }
                
                return cls(
                    id=tweet_id,
                    text=text,
                    _created_at=datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y"),
                    author_username=username,
                    retweet_count=int(data.get('retweet_count', 0)),
                    in_reply_to_status_id=TweetID.from_any(data['in_reply_to_status_id_str']) if data.get('in_reply_to_status_id_str') else None,
                    in_reply_to_username=data.get('in_reply_to_screen_name'),
                    quoted_tweet_id=quoted_id,
                    entities=entities
                )
                
            elif 'noteTweet' in data:
                data = data['noteTweet']
                if 'core' not in data or 'text' not in data['core']:
                    return None
                text = data['core']['text']
                tweet_id = TweetID.from_str(str(data['noteTweetId']))
                
                # Build entities from note data
                entities = {
                    'hashtags': [
                        {
                            'text': str(h['text']),
                            'indices': [int(h['fromIndex']), int(h['toIndex'])]
                        }
                        for h in data['core'].get('hashtags', [])
                    ],
                    'urls': [
                        {
                            'url': str(u['shortUrl']),
                            'expanded_url': str(u.get('expandedUrl', u['shortUrl'])),
                            'display_url': str(u.get('displayUrl', u['shortUrl'])),
                            'indices': [int(u['fromIndex']), int(u['toIndex'])]
                        }
                        for u in data['core'].get('urls', [])
                    ]
                }
                
                return cls(
                    id=tweet_id,
                    text=text,
                    _created_at=datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00')),
                    author_username=username,
                    entities=entities
                )
                
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error converting tweet: {str(e)}", exc_info=True)
            return None

def process_archive(path: Path) -> Dict:
    """Process a single archive file, extracting tweets and profile."""
    with open(path, 'rb') as f:
        data = orjson.loads(f.read())
    
    username = path.stem[:-8] if path.stem.endswith('_archive') else path.stem
    
    result = {
        'tweets': {},      # Just tweets and profile now
        'profile': data.get('profile')
    }
    
    # Process tweets and build reply graph
    for section in ['tweets', 'community-tweet', 'note-tweet']:
        for tweet_data in data.get(section, []):
            tweet = CanonicalTweet.from_any_tweet(tweet_data, username)
            if tweet:
                result['tweets'][tweet.id] = tweet
                if tweet.in_reply_to_status_id:
                    # Add to reply_ids of parent tweet if it exists
                    if tweet.in_reply_to_status_id in result['tweets']:
                        result['tweets'][tweet.in_reply_to_status_id].reply_ids.add(tweet.id)
    
    # Process likes, creating CanonicalTweets for liked tweets we don't have
    for like in data.get('like', []):
        if 'like' in like:
            like_data = like['like']
            if tweet_id := like_data.get('tweetId'):
                tid = TweetID.from_str(tweet_id)
                if tid not in result['tweets']:
                    # Create tweet even if no text - it might have had media or be part of a thread
                    text = like_data.get('fullText', '')  # Default to empty string
                    result['tweets'][tid] = CanonicalTweet(
                        id=tid,
                        text=text,
                        _created_at=tid.timestamp,  # Always derive from ID for likes
                        entities={},  # Empty dict for now
                        author_username=None,  # Unknown for now
                        likers={username}  # Initialize with current liker
                    )
                else:
                    # Add this user as a liker
                    result['tweets'][tid].likers.add(username)
    
    return result

def build_thread_trees(tweets: Dict[TweetID, CanonicalTweet]) -> Dict[TweetID, Set[TweetID]]:
    """Build complete thread trees from tweets."""
    # Build reply graph including both parent->child and child->parent relationships
    reply_graph = {}
    for tweet in tweets.values():
        # Add this tweet's replies
        if tweet.reply_ids:
            reply_graph[tweet.id] = tweet.reply_ids
        
        # Add this tweet as a reply to its parent
        if tweet.in_reply_to_status_id and tweet.in_reply_to_status_id in tweets:
            if tweet.in_reply_to_status_id not in reply_graph:
                reply_graph[tweet.in_reply_to_status_id] = set()
            reply_graph[tweet.in_reply_to_status_id].add(tweet.id)
    
    # Find root tweets (have no parent)
    roots = {
        tweet.id 
        for tweet in tweets.values() 
        if not tweet.in_reply_to_status_id or tweet.in_reply_to_status_id not in tweets
    }
    
    # Build complete trees
    thread_trees = {}
    for root in roots:
        # Get all descendants using BFS
        descendants = set()
        to_process = {root}
        while to_process:
            current = to_process.pop()
            if current in reply_graph:
                replies = reply_graph[current]
                descendants.update(replies)
                to_process.update(replies)
        
        if descendants:  # Only store non-empty trees
            thread_trees[root] = descendants
    
    return thread_trees

def write_parquet_output(output_dir: Path, data: Dict, name: str, batch_size: int = 100_000):
    """Write data to Parquet files in batches."""
    logger.info("Converting data for Parquet format...")
    
    # Define schemas
    tweets_schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),  # ID is never null
        pa.field('text', pa.string(), nullable=False),  # Text is never null
        pa.field('created_at', pa.string(), nullable=False),  # Always available from ID
        pa.field('author_username', pa.string(), nullable=True),  # Optional
        pa.field('retweet_count', pa.int32(), nullable=True),  # Optional
        pa.field('in_reply_to_status_id', pa.int64(), nullable=True),
        pa.field('in_reply_to_username', pa.string(), nullable=True),
        pa.field('quoted_tweet_id', pa.int64(), nullable=True),
        pa.field('entities', pa.string(), nullable=True),  # Optional
        pa.field('likers', pa.list_(pa.string())),
        pa.field('reply_ids', pa.list_(pa.int64()))
    ])
    
    thread_schema = pa.schema([
        pa.field('root_id', pa.int64(), nullable=False),  # Root tweet ID
        pa.field('descendant_ids', pa.list_(pa.int64())),  # All tweets in thread below root
        pa.field('quoted_ids', pa.list_(pa.int64())),  # Tweets quoted by this thread
        pa.field('quoting_ids', pa.list_(pa.int64()))  # Tweets that quote this thread
    ])
    
    # Process tweets in batches
    tweets_data = []
    batch_num = 0
    for i, tweet in enumerate(data['tweets'].values()):
        tweets_data.append({
            'id': tweet.id._id,
            'text': tweet.text,
            'created_at': tweet.created_at.isoformat(),
            'author_username': tweet.author_username,
            'retweet_count': tweet.retweet_count,
            'in_reply_to_status_id': tweet.in_reply_to_status_id._id if tweet.in_reply_to_status_id else None,
            'in_reply_to_username': tweet.in_reply_to_username,
            'quoted_tweet_id': tweet.quoted_tweet_id._id if tweet.quoted_tweet_id else None,
            'entities': orjson.dumps(tweet.entities).decode('utf-8') if tweet.entities else None,
            'likers': sorted(list(tweet.likers)) or [],
            'reply_ids': [rid._id for rid in sorted(tweet.reply_ids)] or []
        })
        
        if len(tweets_data) >= batch_size:
            logger.info(f"Writing batch {batch_num} ({len(tweets_data):,} tweets)...")
            tweets_table = pa.Table.from_pylist(tweets_data, schema=tweets_schema)
            pq.write_table(
                tweets_table,
                output_dir / 'tweets' / f'{name}.{batch_num}.parquet',
                compression='ZSTD',
                compression_level=9
            )
            tweets_data = []
            batch_num += 1
    
    # Write final batch if any
    if tweets_data:
        logger.info(f"Writing final batch ({len(tweets_data):,} tweets)...")
        tweets_table = pa.Table.from_pylist(tweets_data, schema=tweets_schema)
        pq.write_table(
            tweets_table,
            output_dir / 'tweets' / f'{name}.{batch_num}.parquet',
            compression='ZSTD',
            compression_level=9
        )
    
    # Build thread trees while we have the tweets in memory
    logger.info("Building thread trees...")
    thread_trees = build_thread_trees(data['tweets'])
    
    # Write thread trees
    logger.info(f"Writing {len(thread_trees):,} thread trees...")
    thread_data = []
    for root, descendants in thread_trees.items():
        # Get all tweets in thread
        thread_tweets = {root} | descendants
        
        # Find quoted tweets (tweets quoted by any tweet in thread)
        quoted_ids = {
            tweet.quoted_tweet_id._id
            for tweet in (data['tweets'][tid] for tid in thread_tweets)
            if tweet.quoted_tweet_id
        }
        
        # Find quoting tweets (tweets that quote any tweet in thread)
        quoting_ids = {
            tid._id
            for tid, tweet in data['tweets'].items()
            if tweet.quoted_tweet_id and tweet.quoted_tweet_id in thread_tweets
        }
        
        thread_data.append({
            'root_id': root._id,
            'descendant_ids': [tid._id for tid in sorted(descendants)],
            'quoted_ids': sorted(quoted_ids),
            'quoting_ids': sorted(quoting_ids)
        })
    thread_table = pa.Table.from_pylist(thread_data, schema=thread_schema)
    pq.write_table(
        thread_table,
        output_dir / 'trees' / f'{name}.parquet',
        compression='ZSTD',
        compression_level=9
    )

def ensure_output_structure(base_dir: Path, name: str) -> Dict[str, Path]:
    """Create and return output directory structure."""
    # Ensure base directory exists
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create standard subdirectories
    paths = {}
    for subdir in ['tweets', 'profiles', 'trees', 'schemas']:  # Added schemas
        dir_path = base_dir / subdir
        dir_path.mkdir(parents=True, exist_ok=True)
        paths[subdir] = dir_path
    
    return paths

def canonicalize_archive(archive_dir: Path, output_dir: Path, schema_file: Optional[Path] = None, 
                        validate: bool = False, format: str = 'parquet', 
                        sample_size: Optional[int] = None, name: str = 'archive'):
    """Process all archives into a unified timeline."""
    
    # Set up output structure
    paths = ensure_output_structure(output_dir, name)
    
    # Main data structures
    tweets: Dict[TweetID, CanonicalTweet] = {}
    profiles: Dict[str, Dict] = {}
    
    # Process archives in parallel
    archives = list(archive_dir.glob("*_archive.json"))
    if sample_size:
        archives = random.sample(archives, min(sample_size, len(archives)))
    logger.info(f"Processing {len(archives)} archives...")
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_archive, path): path for path in archives}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing archives"):
            path = futures[future]
            try:
                result = future.result()
                username = path.stem[:-8] if path.stem.endswith('_archive') else path.stem
                
                # Merge tweets and update reply structure
                for tweet in result['tweets'].values():
                    if tweet.id not in tweets:
                        tweets[tweet.id] = tweet
                    else:
                        # Update existing tweet with any new information
                        existing = tweets[tweet.id]
                        existing.likers.update(tweet.likers)
                        if tweet.author_username and not existing.author_username:
                            existing.author_username = tweet.author_username
                        if tweet.entities and not existing.entities:
                            existing.entities = tweet.entities
                        # Merge reply_ids
                        existing.reply_ids.update(tweet.reply_ids)
                
                # Store profile
                if result['profile']:
                    profiles[username] = result['profile']
                    
            except Exception as e:
                logger.error(f"Error processing archive {path}: {e}")
    
    # Try to derive author usernames from replies where possible
    for tweet in tweets.values():
        if tweet.in_reply_to_status_id and tweet.in_reply_to_username:
            if tweet.in_reply_to_status_id in tweets:
                parent = tweets[tweet.in_reply_to_status_id]
                if not parent.author_username:
                    parent.author_username = tweet.in_reply_to_username
    
    if format == 'parquet':
        write_parquet_output(output_dir, {
            'tweets': tweets,
            'profiles': profiles
        }, name=name)
    else:
        # Write JSON to output directory
        json_path = paths['tweets'] / f'{name}.json'
        output = {
            "tweets": [
                {
                    'id': tweet.id._id,  # Integer
                    'text': tweet.text,
                    'created_at': tweet.created_at.isoformat(),
                    'author_username': tweet.author_username,
                    'retweet_count': tweet.retweet_count,
                    'in_reply_to_status_id': tweet.in_reply_to_status_id._id if tweet.in_reply_to_status_id else None,
                    'in_reply_to_username': tweet.in_reply_to_username,
                    'quoted_tweet_id': tweet.quoted_tweet_id._id if tweet.quoted_tweet_id else None,
                    'entities': orjson.dumps(tweet.entities).decode('utf-8') if tweet.entities else None,
                    'likers': sorted(tweet.likers),
                    'reply_ids': [rid._id for rid in sorted(tweet.reply_ids)]
                }
                for tweet in sorted(tweets.values(), key=lambda t: t.created_at, reverse=True)
            ],
            "profiles": [
                {
                    'username': username,
                    'bio': profile[0]['profile']['description']['bio'],
                    'website': profile[0]['profile']['description']['website'],
                    'location': profile[0]['profile']['description']['location'],
                    'avatar_url': profile[0]['profile']['avatarMediaUrl'],
                    'header_url': profile[0]['profile'].get('headerMediaUrl', '')
                }
                for username, profile in profiles.items()
            ]
        }
        with open(json_path, 'wb') as f:
            output_bytes = orjson.dumps(output, default=str, option=orjson.OPT_INDENT_2)
            f.write(output_bytes)
    
    # Generate schema if requested
    if validate:
        schema_file = schema_file or (paths['schemas'] / f'{name}.schema.json')
        logger.info(f"Generating schema for canonical format...")
        if format == 'json':
            generate_schema([json_path], schema_file)
        else:
            # Generate schema from all Parquet files
            parquet_files = [
                paths[subdir] / f'{name}.parquet'
                for subdir in ['tweets', 'profiles', 'trees']
            ]
            generate_schema(parquet_files, schema_file)
    
    # Print statistics
    logger.info("\nFinal statistics:")
    logger.info(f"  Tweets: {len(tweets):,}")
    logger.info(f"  Profiles: {len(profiles):,}")
    logger.info(f"  Thread trees: {len(build_thread_trees(tweets)):,}")

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Canonicalize Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_dir', type=Path, help="Directory for output files")
    parser.add_argument('--name', type=str, default='archive',
                       help="Base name for output files (default: archive)")
    parser.add_argument('--schema', type=Path, help="Optional schema file to use")
    parser.add_argument('--debug', action='store_true', 
                       help="Enable debug logging and output JSON for schema validation")
    parser.add_argument('--json', action='store_true',
                       help="Output in JSON format instead of Parquet")
    parser.add_argument('--sample', type=int, metavar='N',
                       help="Process only N random archives (for testing)")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Process archives
    canonicalize_archive(args.archive_dir, args.output_dir, 
                        args.schema, 
                        validate=args.debug,  # Always validate in debug mode
                        format='json' if (args.json or args.debug) else 'parquet',
                        sample_size=args.sample,
                        name=args.name)
    

if __name__ == '__main__':
    main() 