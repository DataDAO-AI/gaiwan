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
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import pyarrow as pa
import pyarrow.parquet as pq

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
    
    def __hash__(self) -> int:
        return hash(self._id)
    
    @classmethod
    def from_str(cls, id_str: str) -> 'TweetID':
        return cls(int(id_str))
        
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
    created_at: datetime
    text: str
    entities: dict
    author_username: str
    retweet_count: int = 0
    in_reply_to_status_id: Optional[TweetID] = None
    in_reply_to_username: Optional[str] = None
    quoted_tweet_id: Optional[TweetID] = None
    likers: Set[str] = field(default_factory=set)

    @classmethod
    def from_any_tweet(cls, data: Dict, username: str) -> Optional['CanonicalTweet']:
        """Create canonical tweet from any tweet type."""
        try:
            quoted_id = None  # Initialize at the top level
            if 'tweet' in data:
                data = data['tweet']
                created_at = datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y")
                text = data.get('full_text', data.get('text', ''))
                tweet_id = TweetID.from_str(data['id_str'])
                
                # Debug quote tweet handling
                is_quote = data.get('is_quote_status', False)
                if is_quote and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Tweet {data['id_str']} is a quote tweet:")
                    logger.debug(f"  Raw tweet data: {json.dumps({
                        'is_quote_status': data.get('is_quote_status'),
                        'quoted_status_id_str': data.get('quoted_status_id_str'),
                        'quoted_status': bool(data.get('quoted_status')),
                        'text': data.get('text', '')[:50] + '...'  # First 50 chars
                    }, indent=2)}")
                if is_quote:
                    quoted_id_str = data.get('quoted_status_id_str')
                    if quoted_id_str:
                        quoted_id = TweetID.from_any(quoted_id_str)
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
                            'expanded_url': str(u.get('expanded_url', u['url'])),  # Fall back to short URL
                            'display_url': str(u.get('display_url', u['url'])),    # Fall back to short URL
                            'indices': [int(idx) for idx in u['indices']]
                        }
                        for u in raw_entities.get('urls', [])
                    ],
                    'user_mentions': [
                        {
                            'screen_name': str(m['screen_name']),
                            'indices': [int(idx) for idx in m['indices']],
                            'id': int(m['id'])  # Convert to integer
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
                            'id': int(m['id']),  # Convert to integer
                            'sizes': {
                                size: {
                                    'w': int(info['w']),  # Convert to integer
                                    'h': int(info['h']),  # Convert to integer
                                    'resize': str(info['resize'])
                                }
                                for size, info in m['sizes'].items()
                            }
                        }
                        for m in raw_entities.get('media', [])
                    ]
                }
                retweet_count = int(data.get('retweet_count', 0))
                
            elif 'noteTweet' in data:
                data = data['noteTweet']
                created_at = datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
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
                        for m in data.get('media', [])
                    ]
                }
                retweet_count = 0
                
            else:
                return None
                
            if not text:
                return None
                
            return cls(
                id=TweetID.from_any(tweet_id),
                created_at=created_at,
                text=str(text),
                entities=entities,
                author_username=str(username),
                retweet_count=retweet_count,
                in_reply_to_status_id=TweetID.from_any(data['in_reply_to_status_id_str']) if data.get('in_reply_to_status_id_str') else None,
                in_reply_to_username=data.get('in_reply_to_screen_name'),
                quoted_tweet_id=quoted_id
            )
            
        except Exception as e:
            logger.error(f"Error converting tweet: {str(e)}", exc_info=True)
            return None

@dataclass
class OrphanedTweet:
    """Information about tweets we only know through likes/replies."""
    id: TweetID
    text: Optional[str] = None
    url: Optional[str] = None
    likers: Set[str] = field(default_factory=set)
    reply_ids: Set[TweetID] = field(default_factory=set)
    
    def to_dict(self) -> Dict:
        return {
            'tweet_id': self.id,
            'text': self.text,
            'url': self.url,
            'likers': sorted(self.likers),
            'reply_ids': sorted(self.reply_ids)
        }

def process_archive(path: Path) -> Dict:
    """Process a single archive file, extracting tweets, likes and reply structure."""
    with open(path, 'rb') as f:
        data = orjson.loads(f.read())
    
    username = path.stem[:-8] if path.stem.endswith('_archive') else path.stem
    
    result = {
        'tweets': {},      
        'likes': set(),    
        'replies': set(),  
        'profile': data.get('profile'),
        'like_texts': {},  # Add these
        'like_urls': {}    # Add these
    }
    
    # Process tweets and build reply graph
    for section in ['tweets', 'community-tweet', 'note-tweet']:
        for tweet_data in data.get(section, []):
            tweet = CanonicalTweet.from_any_tweet(tweet_data, username)
            if tweet:
                result['tweets'][tweet.id] = tweet
                if tweet.in_reply_to_status_id:
                    result['replies'].add((tweet.in_reply_to_status_id, tweet.id))
    
    # Process likes in same pass
    for like in data.get('like', []):
        if 'like' in like:
            like_data = like['like']
            if tweet_id := like_data.get('tweetId'):
                tid = TweetID.from_str(tweet_id)  # Convert to TweetID
                result['likes'].add((tid, username))
                # Store text and URL if available
                if text := like_data.get('fullText'):
                    result['like_texts'][tid] = text
                if url := like_data.get('expandedUrl'):
                    result['like_urls'][tid] = url
    
    return result

def build_thread_trees(reply_graph: Dict[TweetID, Set[TweetID]]) -> Dict[TweetID, Set[TweetID]]:
    """Build complete thread trees from reply graph.
    Returns a mapping of root tweet ID -> set of all descendant tweet IDs."""
    # Find root tweets (no parent)
    all_replies = {reply for replies in reply_graph.values() for reply in replies}
    roots = {tweet_id for tweet_id in reply_graph if tweet_id not in all_replies}
    
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

def write_parquet_output(base_path: Path, data: Dict, name: str = 'archive'):
    """Write output in Parquet format with optimized compression."""
    logger.info("Converting data for Parquet format...")
    
    # Ensure output directories exist
    output_dir = base_path / 'output'
    for subdir in ['tweets', 'profiles', 'trees', 'orphaned', 'schemas']:
        (output_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    # Convert data to lists first
    tweets_data = [
        {
            'id': tweet.id._id,  # Store as int64
            'created_at': tweet.created_at.isoformat(),
            'text': tweet.text,
            'author_username': tweet.author_username,
            'retweet_count': tweet.retweet_count,
            'in_reply_to_status_id': tweet.in_reply_to_status_id._id if tweet.in_reply_to_status_id else None,
            'in_reply_to_username': tweet.in_reply_to_username or None,
            'quoted_tweet_id': tweet.quoted_tweet_id._id if tweet.quoted_tweet_id else None,
            'entities': orjson.dumps(tweet.entities).decode('utf-8'),
            'likers': sorted(list(data['likes'].get(tweet.id, set()))),
            'reply_ids': sorted(rid._id for rid in data['reply_graph'].get(tweet.id, set()))
        }
        for tweet in data['tweets'].values()
    ]
    
    # Convert orphaned tweets
    orphaned_data = [
        {
            'tweet_id': tweet.id._id,
            'text': tweet.text,
            'url': tweet.url,
            'likers': list(tweet.likers),
            'reply_ids': [rid._id for rid in tweet.reply_ids]
        }
        for tweet in data['orphaned'].values()
    ]
    
    # Convert profiles - flatten structure
    profile_data = [
        {
            'username': username,
            'bio': profile[0]['profile']['description']['bio'],
            'website': profile[0]['profile']['description']['website'],
            'location': profile[0]['profile']['description']['location'],
            'avatar_url': profile[0]['profile']['avatarMediaUrl'],
            'header_url': profile[0]['profile'].get('headerMediaUrl', '')
        }
        for username, profile in data['profiles'].items()
    ]
    
    # Build thread trees - simple format for fast lookup
    thread_data = [
        {
            'root_id': root_id._id,  # Store as integer
            'descendant_ids': sorted(d._id for d in descendants)  # Store as integers
        }
        for root_id, descendants in build_thread_trees(data['reply_graph']).items()
    ]
    
    # Write main tables
    logger.info(f"Writing {len(tweets_data):,} tweets...")
    tweets_table = pa.Table.from_pylist(tweets_data)
    pq.write_table(
        tweets_table, 
        output_dir / 'tweets' / f'{name}.parquet',
        compression='ZSTD',
        compression_level=9
    )
    
    logger.info(f"Writing {len(orphaned_data):,} orphaned tweets...")
    orphaned_table = pa.Table.from_pylist(orphaned_data)
    pq.write_table(
        orphaned_table,
        output_dir / 'orphaned' / f'{name}.parquet',
        compression='ZSTD',
        compression_level=9
    )
    
    logger.info(f"Writing {len(profile_data):,} profiles...")
    profiles_table = pa.Table.from_pylist(profile_data)
    pq.write_table(
        profiles_table,
        output_dir / 'profiles' / f'{name}.parquet',
        compression='ZSTD',
        compression_level=9
    )
    
    # Write thread lookup table
    logger.info(f"Writing {len(thread_data):,} thread trees...")
    thread_table = pa.Table.from_pylist(thread_data)
    pq.write_table(
        thread_table,
        output_dir / 'trees' / f'{name}.parquet',
        compression='ZSTD',
        compression_level=9
    )

def ensure_output_structure(base_dir: Path, name: str) -> Dict[str, Path]:
    """Create and return output directory structure.
    
    Returns:
        Dict mapping purpose to Path, e.g., 'tweets' -> Path('output/tweets')
    """
    # Ensure base directory exists
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create standard subdirectories
    paths = {}
    for subdir in ['tweets', 'profiles', 'trees', 'orphaned', 'schemas']:
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
    likes: Dict[TweetID, Set[str]] = {}
    reply_graph: Dict[TweetID, Set[TweetID]] = {}
    orphaned: Dict[TweetID, OrphanedTweet] = {}
    profiles: Dict[str, Dict] = {}
    
    # Process archives in parallel
    archives = list(archive_dir.glob("*_archive.json"))
    if sample_size:
        import random
        archives = random.sample(archives, min(sample_size, len(archives)))
        logger.info(f"Using sample of {len(archives)} archives")
    logger.info(f"Processing {len(archives)} archives...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_archive, path): path for path in archives}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing archives"):
            path = futures[future]  # Get path before trying result
            try:
                result = future.result()
                username = path.stem[:-8] if path.stem.endswith('_archive') else path.stem
                
                # Merge tweets
                tweets.update(result['tweets'])
                
                # Merge likes and track orphaned tweets
                for tweet_id, liker in result['likes']:
                    # Add to likes index
                    if tweet_id not in likes:
                        likes[tweet_id] = set()
                    likes[tweet_id].add(liker)
                    
                    # Track orphaned tweets
                    if tweet_id not in tweets:
                        if tweet_id not in orphaned:
                            orphaned[tweet_id] = OrphanedTweet(
                                id=TweetID.from_any(tweet_id),
                                text=result['like_texts'].get(tweet_id),
                                url=result['like_urls'].get(tweet_id)
                            )
                        orphaned[tweet_id].likers.add(liker)
                
                # Merge reply structure and track orphaned tweets
                for parent_id, reply_id in result['replies']:
                    # Add to reply graph
                    if parent_id not in reply_graph:
                        reply_graph[parent_id] = set()
                    reply_graph[parent_id].add(reply_id)
                    
                    # Track orphaned tweets
                    if parent_id not in tweets and parent_id not in orphaned:
                        orphaned[parent_id] = OrphanedTweet(id=parent_id)
                    if parent_id in orphaned:
                        orphaned[parent_id].reply_ids.add(reply_id)
                
                # Store profile
                if result['profile']:
                    profiles[username] = result['profile']
                    
            except Exception as e:
                logger.error(f"Error processing archive {path}: {e}")
    
    if format == 'parquet':
        write_parquet_output(output_dir, {
            'tweets': tweets,
            'orphaned': orphaned,
            'profiles': profiles,
            'likes': likes,
            'reply_graph': reply_graph
        }, name=name)
    else:
        # Write JSON to output directory
        json_path = paths['tweets'] / f'{name}.json'
        output = {
            "tweets": [
                {
                    **asdict(tweet),
                    'id': tweet.id._id,  # Integer
                    'in_reply_to_status_id': tweet.in_reply_to_status_id._id if tweet.in_reply_to_status_id else None,
                    'quoted_tweet_id': tweet.quoted_tweet_id._id if tweet.quoted_tweet_id else None,
                    'likers': sorted(likes.get(tweet.id, set())),
                    'reply_ids': [rid._id for rid in sorted(reply_graph.get(tweet.id, set()))]  # Array of integers
                }
                for tweet in sorted(tweets.values(), key=lambda t: t.created_at, reverse=True)
            ],
            "orphaned_tweets": [
                {
                    'tweet_id': orphan.id._id,  # Integer
                    'text': orphan.text,
                    'url': orphan.url,
                    'likers': sorted(orphan.likers),
                    'reply_ids': [rid._id for rid in sorted(orphan.reply_ids)]  # Array of integers
                }
                for orphan in sorted(orphaned.values(), key=lambda o: len(o.likers), reverse=True)
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
                for subdir in ['tweets', 'orphaned', 'profiles', 'trees']
            ]
            generate_schema(parquet_files, schema_file)
    
    # Print statistics
    logger.info("\nFinal statistics:")
    logger.info(f"  Tweets: {len(tweets):,}")
    logger.info(f"  Orphaned tweets: {len(orphaned):,}")
    logger.info(f"  Total likes: {sum(len(l) for l in likes.values()):,}")
    logger.info(f"  Reply connections: {sum(len(r) for r in reply_graph.values()):,}")
    logger.info(f"  Quote tweets: {sum(1 for t in tweets.values() if t.quoted_tweet_id is not None):,}")
    logger.info(f"  Profiles: {len(profiles):,}")
    logger.info(f"  Thread trees: {len(build_thread_trees(reply_graph)):,}")

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
    parser.add_argument('--analyze', action='store_true',
                       help="Run thread analysis after processing")
    parser.add_argument('--inspect', action='store_true',
                       help="Inspect raw archive contents for debugging")
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
    
    # Run analysis if requested
    if args.analyze:
        from gaiwan.analyze import analyze_thread_patterns, analyze_reconstruction_confidence
        logger.info("\nAnalyzing thread patterns...")
        
        threads = analyze_thread_patterns(
            paths['tweets'] / f'{args.name}.parquet',
            paths['orphaned'] / f'{args.name}.parquet'
        )
        patterns = analyze_reconstruction_confidence(threads)
        
        # Show findings
        logger.info(f"Found {len(patterns)} reconstructible tweets")
        high_conf = sum(1 for p in patterns.values() 
                       if sum(p.confidence_factors.values())/len(p.confidence_factors) > 0.8)
        logger.info(f"High confidence reconstructions: {high_conf}")
    
    if args.inspect:
        archives = list(args.archive_dir.glob("*_archive.json"))
        if args.sample:
            archives = random.sample(archives, min(args.sample, len(archives)))
        for path in archives:
            inspect_archive(path)
        return

def inspect_archive(path: Path):
    """Debug helper to examine raw archive contents."""
    with open(path, 'rb') as f:
        data = orjson.loads(f.read())
    
    quote_count = 0
    total_tweets = 0
    for section in ['tweets', 'community-tweet', 'note-tweet']:
        for tweet in data.get(section, []):
            total_tweets += 1
            if 'tweet' in tweet:
                tweet = tweet['tweet']
                # Check retweet fields
                is_retweet = tweet.get('retweeted', False)
                retweet_count = tweet.get('retweet_count', '0')
                
                if is_retweet:
                    logger.debug(f"Quote-related tweet found in {path}:")
                    logger.debug(f"  id: {tweet['id_str']}")
                    logger.debug(f"  is_retweet: {is_retweet}")
                    logger.debug(f"  retweet_count: {retweet_count}")
                if tweet.get('quoted_status_id_str'):
                    quote_count += 1
                    logger.debug(f"Found quote tweet: {tweet['id_str']} -> {tweet['quoted_status_id_str']}")
    
    logger.info(f"Archive {path.name}: {total_tweets} tweets, {quote_count} quotes")

if __name__ == '__main__':
    main() 