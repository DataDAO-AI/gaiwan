"""
Efficient Twitter archive processor with bidirectional thread reconstruction.
Handles various Twitter archive formats and large datasets.
"""

import argparse
import json
import logging
from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import os
import tempfile
import shutil
import re
from datetime import datetime, timezone
import pprint
import random
import pickle
import hashlib

import duckdb
import pandas as pd

# Disable the Google API warning
os.environ["GAIWAN_DISABLE_YOUTUBE_API"] = "1"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Constants for performance tuning
MAX_WORKERS = min(8, multiprocessing.cpu_count())
BATCH_SIZE = 500000  # Process in manageable batches
CHECKPOINT_DIR = "./checkpoints"  # Directory for checkpoints

def get_archive_hash(file_path):
    """Generate a hash of the archive filename to use for checkpointing."""
    return hashlib.md5(str(file_path).encode()).hexdigest()

def load_processed_archives():
    """Load the set of already processed archives from checkpoint."""
    checkpoint_file = os.path.join(CHECKPOINT_DIR, "processed_archives.pkl")
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'rb') as f:
            return pickle.load(f)
    return set()

def save_processed_archives(processed_archives):
    """Save the set of processed archives to checkpoint."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    checkpoint_file = os.path.join(CHECKPOINT_DIR, "processed_archives.pkl")
    with open(checkpoint_file, 'wb') as f:
        pickle.dump(processed_archives, f)

def initialize_db(temp_dir=None):
    """Create a DuckDB instance with configurable temp directory and optimized settings."""
    # Create connection with explicit temp directory if provided
    if temp_dir:
        con = duckdb.connect(database=':memory:')
        con.execute(f"PRAGMA temp_directory='{temp_dir}'")
    else:
        con = duckdb.connect(database=':memory:')
    
    # Performance optimizations - use moderate defaults to avoid issues
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='4GB'")  # Conservative limit to avoid memory pressure
    
    # Better compression algorithm (ZSTD is generally a good balance)
    con.execute("PRAGMA force_compression='ZSTD'")  # Specify a proper compression algorithm
    
    # Create schema
    create_tables(con)
    
    return con

def create_tables(con):
    """Create necessary tables in DuckDB."""
    # Raw tweets table
    con.execute("""
    CREATE TABLE source_tweets (
        id VARCHAR,
        user_id VARCHAR,
        user_screen_name VARCHAR,
        user_name VARCHAR,
        in_reply_to_status_id VARCHAR,
        in_reply_to_user_id VARCHAR,
        in_reply_to_screen_name VARCHAR,
        retweet_count INTEGER,
        favorite_count INTEGER,
        full_text VARCHAR,
        lang VARCHAR,
        source VARCHAR,
        created_at TIMESTAMP,
        favorited BOOLEAN,
        retweeted BOOLEAN,
        possibly_sensitive BOOLEAN,
        urls VARCHAR[],
        media VARCHAR[],
        hashtags VARCHAR[],
        user_mentions VARCHAR[],
        tweet_type VARCHAR,
        archive_file VARCHAR,
        is_reply BOOLEAN
    )
    """)
    
    # Create full indexes instead of partial indexes for better compatibility
    con.execute("CREATE INDEX tweet_id_idx ON source_tweets(id)")
    con.execute("CREATE INDEX reply_indicator_idx ON source_tweets(is_reply)")
    con.execute("CREATE INDEX reply_to_full_idx ON source_tweets(in_reply_to_status_id)")

def inspect_archive_format(file_path):
    """Analyze the structure of a Twitter archive file to understand its format."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                # First try parsing as pure JSON
                data = json.load(f)
                
                # Log the top-level keys to understand structure
                if isinstance(data, dict):
                    logger.info(f"Archive format for {file_path.name}: {list(data.keys())}")
                    
                    # Examine regular tweet structure
                    if 'tweets' in data and isinstance(data['tweets'], list) and len(data['tweets']) > 0:
                        tweet_container = data['tweets'][0]
                        logger.info(f"Regular tweet container keys: {list(tweet_container.keys())}")
                        
                        # Go one level deeper into the actual tweet
                        if 'tweet' in tweet_container and isinstance(tweet_container['tweet'], dict):
                            tweet = tweet_container['tweet']
                            logger.info(f"Regular tweet object keys: {list(tweet.keys())}")
                            
                            # Examine timestamp format
                            if 'created_at' in tweet:
                                logger.info(f"Regular tweet timestamp format: {tweet['created_at']}")
                            
                            # Check type of ID for data consistency
                            if 'id_str' in tweet:
                                logger.info(f"ID type: {type(tweet['id_str']).__name__}")
                            
                            # Check reply IDs to understand threading
                            if 'in_reply_to_status_id_str' in tweet:
                                logger.info(f"Reply ID type: {type(tweet['in_reply_to_status_id_str']).__name__}")
                    
                    # Examine community tweet structure
                    if 'community-tweet' in data and isinstance(data['community-tweet'], list) and len(data['community-tweet']) > 0:
                        tweet_container = data['community-tweet'][0]
                        logger.info(f"Community tweet container keys: {list(tweet_container.keys())}")
                        
                        # Go one level deeper into the actual tweet
                        if 'tweet' in tweet_container and isinstance(tweet_container['tweet'], dict):
                            tweet = tweet_container['tweet']
                            logger.info(f"Community tweet object keys: {list(tweet.keys())}")
                            
                            # Examine timestamp format
                            if 'created_at' in tweet:
                                logger.info(f"Community tweet timestamp format: {tweet['created_at']}")
                    
                    # Examine note tweet structure
                    if 'note-tweet' in data and isinstance(data['note-tweet'], list) and len(data['note-tweet']) > 0:
                        tweet_container = data['note-tweet'][0]
                        logger.info(f"Note tweet container keys: {list(tweet_container.keys())}")
                        
                        # Go one level deeper into the actual note tweet
                        if 'noteTweet' in tweet_container and isinstance(tweet_container['noteTweet'], dict):
                            note_tweet = tweet_container['noteTweet']
                            logger.info(f"Note tweet object keys: {list(note_tweet.keys())}")
                            
                            # Examine timestamp format - note tweets often use 'createdAt' instead of 'created_at'
                            if 'createdAt' in note_tweet:
                                logger.info(f"Note tweet timestamp format: {note_tweet['createdAt']}")
                            elif 'created_at' in note_tweet:
                                logger.info(f"Note tweet timestamp format: {note_tweet['created_at']}")
                    
                    # Examine like structure
                    if 'like' in data and isinstance(data['like'], list) and len(data['like']) > 0:
                        like_container = data['like'][0]
                        logger.info(f"Like container keys: {list(like_container.keys())}")
                        
                        # Go one level deeper into the actual like
                        if 'like' in like_container and isinstance(like_container['like'], dict):
                            like = like_container['like']
                            logger.info(f"Like object keys: {list(like.keys())}")
                
            except json.JSONDecodeError:
                logger.warning(f"Could not parse {file_path.name} as JSON")
                
    except Exception as e:
        logger.error(f"Error inspecting {file_path.name}: {e}")

def more_detailed_archive_inspection(archive_files):
    """Deeper inspection of archive formats across multiple files."""
    logger.info("Performing detailed inspection of all archive files...")
    
    # Structure counts for statistics
    tweet_formats = {}
    like_formats = {}
    community_formats = {}
    note_formats = {}
    
    # Timestamp format examination
    regular_timestamp_samples = set()
    note_timestamp_samples = set()
    
    for file_path in archive_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    
                    # Track structure
                    if not isinstance(data, dict):
                        continue
                    
                    # Check for regular tweets structure and timestamps
                    if 'tweets' in data and isinstance(data['tweets'], list) and len(data['tweets']) > 0:
                        tweet_container = data['tweets'][0]
                        # Get the keys in the tweet container
                        keys = tuple(sorted(tweet_container.keys()))
                        tweet_formats[keys] = tweet_formats.get(keys, 0) + 1
                        
                        # Examine timestamp format
                        if 'tweet' in tweet_container and 'created_at' in tweet_container['tweet']:
                            timestamp = tweet_container['tweet']['created_at']
                            if len(regular_timestamp_samples) < 5:  # Collect a few samples
                                regular_timestamp_samples.add(timestamp)
                    
                    # Check for community tweets
                    if 'community-tweet' in data and isinstance(data['community-tweet'], list) and len(data['community-tweet']) > 0:
                        tweet_container = data['community-tweet'][0]
                        keys = tuple(sorted(tweet_container.keys()))
                        community_formats[keys] = community_formats.get(keys, 0) + 1
                        
                        # Examine a community tweet in detail
                        if len(community_formats) == 1:  # Just log the first format found
                            logger.info(f"Community tweet from {file_path.name}: {tweet_container}")
                    
                    # Check for note tweets and their timestamp format
                    if 'note-tweet' in data and isinstance(data['note-tweet'], list) and len(data['note-tweet']) > 0:
                        tweet_container = data['note-tweet'][0]
                        keys = tuple(sorted(tweet_container.keys()))
                        note_formats[keys] = note_formats.get(keys, 0) + 1
                        
                        # Examine a note tweet in detail and collect timestamp
                        if 'noteTweet' in tweet_container:
                            note_tweet = tweet_container['noteTweet']
                            logger.info(f"Note tweet structure from {file_path.name}: {note_tweet}")
                            
                            # Check for createdAt timestamp
                            if 'createdAt' in note_tweet:
                                timestamp = note_tweet['createdAt']
                                if len(note_timestamp_samples) < 5:  # Collect a few samples
                                    note_timestamp_samples.add(timestamp)
                                logger.info(f"Note tweet timestamp from {file_path.name}: {timestamp}")
                    
                    # Check for like structure
                    if 'like' in data and isinstance(data['like'], list) and len(data['like']) > 0:
                        like_container = data['like'][0]
                        keys = tuple(sorted(like_container.keys()))
                        like_formats[keys] = like_formats.get(keys, 0) + 1
                        
                        # Examine a like in detail
                        if like_formats and len(like_formats) == 1:  # Just log the first format found
                            logger.info(f"Like object from {file_path.name}: {like_container}")
                
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in {file_path.name}")
        except Exception as e:
            logger.error(f"Error examining {file_path.name}: {e}")
    
    logger.info(f"Found {len(tweet_formats)} different tweet formats:")
    for i, (format_keys, count) in enumerate(sorted(tweet_formats.items(), key=lambda x: x[1], reverse=True), 1):
        logger.info(f"  Format {i} ({count} archives): {format_keys}")
    
    logger.info(f"Found {len(community_formats)} different community tweet formats:")
    for i, (format_keys, count) in enumerate(sorted(community_formats.items(), key=lambda x: x[1], reverse=True), 1):
        logger.info(f"  Format {i} ({count} archives): {format_keys}")
    
    logger.info(f"Found {len(note_formats)} different note tweet formats:")
    for i, (format_keys, count) in enumerate(sorted(note_formats.items(), key=lambda x: x[1], reverse=True), 1):
        logger.info(f"  Format {i} ({count} archives): {format_keys}")
    
    logger.info(f"Found {len(like_formats)} different like formats:")
    for i, (format_keys, count) in enumerate(sorted(like_formats.items(), key=lambda x: x[1], reverse=True), 1):
        logger.info(f"  Format {i} ({count} archives): {format_keys}")
    
    # Report on timestamp formats
    logger.info("Timestamp format samples:")
    logger.info(f"Regular tweet timestamps: {regular_timestamp_samples}")
    logger.info(f"Note tweet timestamps: {note_timestamp_samples}")

def process_archive(file_path, user_cache={}):
    """Process a Twitter archive file and extract tweets, likes, community tweets, and note tweets."""
    logger.info(f"Processing archive: {file_path.name}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract user profile information
        user_info = {}
        if 'profile' in data:
            # Profile could be a list or a dictionary, handle both cases
            profile = data['profile']
            if isinstance(profile, list) and len(profile) > 0:
                # If it's a list, take the first item
                profile = profile[0]
                # It might be another container with a 'profile' key
                if isinstance(profile, dict) and 'profile' in profile:
                    profile = profile['profile']
            
            # Now we should have the actual profile data
            if isinstance(profile, dict):
                # Extract user info from different possible profile formats
                user_info = {
                    'user_id': profile.get('userInformations', {}).get('id', '') or profile.get('id', ''),
                    'user_screen_name': profile.get('userInformations', {}).get('userName', '') or 
                                        profile.get('userName', '') or profile.get('screen_name', ''),
                    'user_name': profile.get('userInformations', {}).get('displayName', '') or 
                                 profile.get('displayName', '') or profile.get('name', '')
                }
                
                # Make sure we don't have empty values for critical fields
                if not user_info['user_screen_name'] and 'handle' in profile:
                    user_info['user_screen_name'] = profile['handle'].lstrip('@')
                
                # Extract username from archive filename if still missing
                if not user_info['user_screen_name'] or user_info['user_screen_name'] == '':
                    # Try to extract username from archive filename (usually username_archive.json)
                    filename = file_path.name
                    if '_archive.json' in filename:
                        extracted_name = filename.replace('_archive.json', '')
                        if extracted_name:
                            user_info['user_screen_name'] = extracted_name
                
                # Ensure user_name has a value if missing
                if not user_info['user_name'] and user_info['user_screen_name']:
                    user_info['user_name'] = user_info['user_screen_name']
            
            # Cache user info for future reference
            if user_info.get('user_id'):
                user_cache[user_info['user_id']] = user_info
        
        tweets = []
        
        # Process regular tweets
        if 'tweets' in data and isinstance(data['tweets'], list):
            for tweet_container in data['tweets']:
                if isinstance(tweet_container, dict) and 'tweet' in tweet_container:
                    tweet_obj = tweet_container['tweet']
                    tweet = process_tweet(tweet_obj, user_info, 'tweet', file_path.name)
                    if tweet:
                        tweets.append(tweet)
        
        # Process community tweets
        if 'community-tweet' in data and isinstance(data['community-tweet'], list):
            for tweet_container in data['community-tweet']:
                if isinstance(tweet_container, dict) and 'tweet' in tweet_container:
                    tweet_obj = tweet_container['tweet']
                    tweet = process_tweet(tweet_obj, user_info, 'community_tweet', file_path.name)
                    if tweet:
                        tweets.append(tweet)
        
        # Process note tweets (different container key: 'noteTweet')
        if 'note-tweet' in data and isinstance(data['note-tweet'], list):
            for tweet_container in data['note-tweet']:
                if isinstance(tweet_container, dict) and 'noteTweet' in tweet_container:
                    note_tweet_obj = tweet_container['noteTweet']
                    # Process note tweets differently due to their structure
                    tweet = process_note_tweet(note_tweet_obj, user_info, file_path.name)
                    if tweet:
                        tweets.append(tweet)
        
        # Process likes
        if 'like' in data and isinstance(data['like'], list):
            for like_container in data['like']:
                if isinstance(like_container, dict) and 'like' in like_container:
                    like_obj = like_container['like']
                    
                    # Extract the URL to add to the urls array instead of a separate field
                    expanded_url = like_obj.get('expandedUrl', '')
                    urls_array = []
                    if expanded_url:
                        urls_array.append(expanded_url)
                    
                    like = {
                        'id': like_obj.get('tweetId', ''),
                        'user_id': user_info.get('user_id', ''),
                        'user_screen_name': user_info.get('user_screen_name', ''),
                        'user_name': user_info.get('user_name', ''),
                        'in_reply_to_status_id': None,
                        'in_reply_to_user_id': None,
                        'in_reply_to_screen_name': None,
                        'retweet_count': 0,
                        'favorite_count': 0,
                        'full_text': like_obj.get('fullText', ''),
                        'lang': None,  # Not available for likes
                        'source': None,  # Not available for likes
                        'created_at': None,  # Not available for likes
                        'favorited': True,  # This is a liked tweet by definition
                        'retweeted': False,
                        'possibly_sensitive': False,
                        'urls': urls_array,  # Add the expanded URL to the urls array
                        'media': [],  # Not directly available
                        'hashtags': [],  # Not directly available
                        'user_mentions': [],  # Not directly available
                        'tweet_type': 'like',
                        'archive_file': file_path.name,
                        'is_reply': False  # Likes aren't replies
                    }
                    tweets.append(like)
                    
        return tweets, user_info
        
    except Exception as e:
        logger.error(f"Error processing archive {file_path.name}: {e}")
        return [], {}

def process_tweet(tweet_obj, user_info, tweet_type, archive_file):
    """Process a tweet object from the archive and extract relevant information."""
    try:
        # Extract URLs, media, hashtags, etc.
        urls = []
        media = []
        hashtags = []
        user_mentions = []
        
        if 'entities' in tweet_obj:
            entities = tweet_obj.get('entities', {})
            
            # Extract URLs
            for url_obj in entities.get('urls', []):
                if 'expanded_url' in url_obj:
                    urls.append(url_obj['expanded_url'])
            
            # Extract hashtags
            for tag in entities.get('hashtags', []):
                if 'text' in tag:
                    hashtags.append(tag['text'])
                    
            # Extract user mentions
            for mention in entities.get('user_mentions', []):
                if 'screen_name' in mention:
                    user_mentions.append(mention['screen_name'])
        
        # Extract media from extended_entities if available
        if 'extended_entities' in tweet_obj and 'media' in tweet_obj['extended_entities']:
            for media_obj in tweet_obj['extended_entities']['media']:
                if 'media_url' in media_obj:
                    media.append(media_obj['media_url'])
        
        # Parse timestamp using our unified parser
        created_at = parse_twitter_timestamp(tweet_obj.get('created_at'))
        
        # Check if this is a reply
        is_reply = tweet_obj.get('in_reply_to_status_id_str') is not None
        
        # Create structured tweet object
        tweet = {
            'id': tweet_obj.get('id_str', ''),
            'user_id': user_info.get('user_id', ''),
            'user_screen_name': user_info.get('user_screen_name', ''),
            'user_name': user_info.get('user_name', ''),
            'in_reply_to_status_id': tweet_obj.get('in_reply_to_status_id_str'),
            'in_reply_to_user_id': tweet_obj.get('in_reply_to_user_id_str'),
            'in_reply_to_screen_name': tweet_obj.get('in_reply_to_screen_name'),
            'retweet_count': tweet_obj.get('retweet_count', 0),
            'favorite_count': tweet_obj.get('favorite_count', 0),
            'full_text': tweet_obj.get('full_text', ''),
            'lang': tweet_obj.get('lang', ''),
            'source': tweet_obj.get('source', ''),
            'created_at': created_at,
            'favorited': tweet_obj.get('favorited', False),
            'retweeted': tweet_obj.get('retweeted', False),
            'possibly_sensitive': tweet_obj.get('possibly_sensitive', False),
            'urls': urls,
            'media': media,
            'hashtags': hashtags,
            'user_mentions': user_mentions,
            'tweet_type': tweet_type,
            'archive_file': archive_file,
            'is_reply': is_reply  # Add the is_reply field to match our schema
        }
        
        # Apply fallback for missing user
        if not tweet['user_screen_name'] or tweet['user_screen_name'] == '':
            # Try to extract from archive filename
            if '_archive.json' in archive_file:
                extracted_name = archive_file.replace('_archive.json', '')
                tweet['user_screen_name'] = extracted_name
        
        return tweet
    except Exception as e:
        logger.error(f"Error processing tweet: {e}")
        return None

def process_note_tweet(note_tweet_obj, user_info, archive_file):
    """Process a note tweet which has a different structure than regular tweets."""
    try:
        # Extract content from the note tweet
        core = note_tweet_obj.get('core', {})
        
        # Extract URLs, mentions, hashtags
        urls = []
        user_mentions = []
        hashtags = []
        
        # Process URLs in note tweets
        for url_obj in core.get('urls', []):
            if 'expandedUrl' in url_obj:
                urls.append(url_obj['expandedUrl'])
        
        # Process mentions in note tweets
        for mention in core.get('mentions', []):
            if 'screenName' in mention:
                user_mentions.append(mention['screenName'])
                
        # Process hashtags in note tweets
        for tag in core.get('hashtags', []):
            if isinstance(tag, dict) and 'text' in tag:
                hashtags.append(tag['text'])
            elif isinstance(tag, str):
                hashtags.append(tag)
        
        # Parse timestamp (note tweets use createdAt in ISO format)
        created_at = parse_twitter_timestamp(note_tweet_obj.get('createdAt'))
        
        # Create structured tweet object
        tweet = {
            'id': note_tweet_obj.get('noteTweetId', ''),
            'user_id': user_info.get('user_id', ''),
            'user_screen_name': user_info.get('user_screen_name', ''),
            'user_name': user_info.get('user_name', ''),
            'in_reply_to_status_id': None,  # Note tweets typically don't have reply info in the same format
            'in_reply_to_user_id': None,
            'in_reply_to_screen_name': None,
            'retweet_count': 0,  # Not directly available in note tweets
            'favorite_count': 0,  # Not directly available in note tweets
            'full_text': core.get('text', ''),
            'lang': None,  # Not directly available in note tweets
            'source': None,  # Not directly available in note tweets
            'created_at': created_at,
            'favorited': False,
            'retweeted': False,
            'possibly_sensitive': False,
            'urls': urls,
            'media': [],  # Media handling would need separate processing
            'hashtags': hashtags,
            'user_mentions': user_mentions,
            'tweet_type': 'note_tweet',
            'archive_file': archive_file,
            'is_reply': False  # Note tweets typically aren't replies
        }
        
        return tweet
    except Exception as e:
        logger.error(f"Error processing note tweet in {archive_file}: {e}")
        return None

def parse_twitter_timestamp(timestamp_str):
    """
    Parse Twitter timestamp which can be in two different formats:
    1. Regular Twitter format: "Wed Oct 10 20:19:24 +0000 2018"
    2. ISO format (used in noteTweets): "2022-08-19T22:22:42.000Z"
    """
    if not timestamp_str:
        return None
        
    # ISO format detection
    if 'T' in timestamp_str and timestamp_str.endswith('Z'):
        try:
            # Convert ISO format to datetime
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except Exception as e:
            logger.warning(f"Error parsing ISO timestamp: {timestamp_str} - {e}")
            return None
    else:
        try:
            # Convert standard Twitter format to datetime
            return datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %z %Y")
        except Exception as e:
            logger.warning(f"Error parsing Twitter timestamp: {timestamp_str} - {e}")
            return None

def format_timestamp(timestamp_str):
    """Convert a Twitter timestamp to ISO format."""
    if not timestamp_str:
        return None
    
    try:
        # Twitter format: "Wed Oct 10 20:19:24 +0000 2018"
        if '+0000' in timestamp_str:
            dt = datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
            return dt.isoformat()
        else:
            # Try standard ISO format
            return pd.to_datetime(timestamp_str).isoformat()
    except:
        return None

def process_archive_batch(archive_files, db_con, processed_archives, output_dir):
    """Process a batch of archives with error handling and checkpointing."""
    total_tweets = 0
    user_cache = {}
    newly_processed = set()
    
    try:
        for file_path in archive_files:
            # Skip already processed archives
            archive_hash = get_archive_hash(file_path)
            if archive_hash in processed_archives:
                logger.info(f"Skipping already processed archive: {file_path.name}")
                continue
                
            try:
                tweets, user_info = process_archive(file_path, user_cache)
                
                if tweets:
                    # Insert in smaller chunks to avoid memory issues
                    chunk_size = 10000
                    for i in range(0, len(tweets), chunk_size):
                        chunk = tweets[i:i+chunk_size]
                        # Use safe way to insert data
                        try:
                            # Convert to pandas dataframe to let DuckDB handle type conversion
                            df = pd.DataFrame(chunk)
                            db_con.execute("INSERT INTO source_tweets SELECT * FROM df")
                            total_tweets += len(chunk)
                        except Exception as e:
                            logger.error(f"Error inserting chunk from {file_path.name}: {e}")
                            # Continue with next chunk rather than failing the whole file
                
                # Mark as processed even if there were partial errors
                newly_processed.add(archive_hash)
                
                # Save intermediate checkpoint periodically
                if len(newly_processed) % 10 == 0:
                    # Save processed archives checkpoint
                    save_processed_archives(processed_archives.union(newly_processed))
                    
                    # Save raw tweets to parquet as checkpoint
                    checkpoint_data(db_con, output_dir, "raw_tweets_checkpoint")
                    
                    logger.info(f"Saved checkpoint after processing {len(newly_processed)} new archives")
                
            except Exception as e:
                logger.error(f"Error processing archive {file_path.name}: {e}")
                # Continue with next file rather than failing the whole batch
        
        # Update overall processed list with newly processed archives
        processed_archives.update(newly_processed)
        save_processed_archives(processed_archives)
        
        logger.info(f"Processed {total_tweets} tweets from {len(newly_processed)} archives")
        return total_tweets
        
    except Exception as e:
        logger.exception(f"Batch processing error: {e}")
        return total_tweets

def checkpoint_data(con, output_dir, prefix):
    """Save intermediate data to parquet files as checkpoints."""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate a timestamp for the checkpoint file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Export the current state to parquet
        checkpoint_file = os.path.join(output_dir, f"{prefix}_{timestamp}.parquet")
        con.execute(f"COPY source_tweets TO '{checkpoint_file}' (FORMAT 'parquet')")
        
        logger.info(f"Saved checkpoint to {checkpoint_file}")
        return checkpoint_file
    except Exception as e:
        logger.error(f"Failed to create checkpoint: {e}")
        return None

def load_checkpoint(con, checkpoint_file):
    """Load data from a checkpoint file."""
    try:
        # Load data from parquet checkpoint
        if os.path.exists(checkpoint_file):
            con.execute(f"INSERT INTO source_tweets SELECT * FROM read_parquet('{checkpoint_file}')")
            logger.info(f"Loaded checkpoint from {checkpoint_file}")
            return True
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
    return False

def multi_stage_process(archive_files, temp_dir, output_dir, batch_size):
    """
    Process archives in multiple stages with checkpointing for resilience.
    
    1. Stage 1: Extract tweets from archives with checkpointing
    2. Stage 2: Export results directly
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    # Load previously processed archives
    processed_archives = load_processed_archives()
    logger.info(f"Found {len(processed_archives)} previously processed archives")
    
    # Initialize the database
    con = initialize_db(temp_dir)
    
    # Stage 1: Process archives and extract tweets
    # Only process archives we haven't seen before
    remaining_archives = [f for f in archive_files if get_archive_hash(f) not in processed_archives]
    
    # Process in smaller chunks to avoid running out of memory/disk
    total_tweets = 0
    archive_count = 0
    
    try:
        # Process each archive individually to minimize memory usage
        for file_path in remaining_archives:
            try:
                archive_count += 1
                logger.info(f"Processing archive {archive_count}/{len(remaining_archives)}: {file_path.name}")
                tweets, _ = process_archive(file_path)
                
                if tweets:
                    # Insert tweets in smaller batches to avoid memory issues
                    for j in range(0, len(tweets), 500):
                        batch = tweets[j:j+500]
                        try:
                            # Convert list of dicts to pandas DataFrame for efficient insertion
                            df = pd.DataFrame(batch)
                            con.execute("INSERT INTO source_tweets SELECT * FROM df")
                            total_tweets += len(batch)
                        except Exception as e:
                            logger.error(f"Error inserting batch from {file_path.name}: {e}")
                
                # Mark this archive as processed
                processed_archives.add(get_archive_hash(file_path))
                
                # Save checkpoint after each archive
                save_processed_archives(processed_archives)
                
                # Save incremental results to parquet after every 5 archives
                if archive_count % 5 == 0:
                    try:
                        checkpoint_path = os.path.join(CHECKPOINT_DIR, f"tweets_checkpoint_{archive_count}.parquet")
                        con.execute(f"COPY source_tweets TO '{checkpoint_path}' (FORMAT PARQUET)")
                        logger.info(f"Saved checkpoint: {checkpoint_path}")
                    except Exception as e:
                        logger.error(f"Failed to save checkpoint: {e}")
                
            except Exception as e:
                logger.error(f"Error processing archive {file_path.name}: {e}")
        
        # Stage 2: Export results directly
        # This avoids complex processing that might cause disk space issues
        logger.info(f"Exporting {total_tweets} processed tweets...")
        
        # Export in manageable chunks by user to avoid memory issues
        result_path = os.path.join(output_dir, "processed_tweets.parquet")
        try:
            con.execute(f"COPY source_tweets TO '{result_path}' (FORMAT PARQUET)")
            logger.info(f"Exported processed tweets to {result_path}")
        except Exception as e:
            logger.error(f"Error exporting tweets: {e}")
            
            # Try exporting smaller chunks
            logger.info("Trying to export in smaller chunks...")
            con.execute(f"""
            CREATE TABLE export_groups AS
            SELECT 
                user_screen_name,
                COUNT(*) as tweet_count
            FROM source_tweets
            GROUP BY user_screen_name
            """)
            
            user_groups = con.execute("SELECT user_screen_name FROM export_groups").fetchall()
            for i, (user,) in enumerate(user_groups):
                try:
                    user_safe = re.sub(r'[^a-zA-Z0-9_]', '_', user)
                    user_path = os.path.join(output_dir, f"tweets_{user_safe}.parquet")
                    con.execute(f"""
                    COPY (
                        SELECT * FROM source_tweets 
                        WHERE user_screen_name = '{user}'
                    ) TO '{user_path}' (FORMAT PARQUET)
                    """)
                    if i % 10 == 0:
                        logger.info(f"Exported {i+1}/{len(user_groups)} user files")
                except Exception as e:
                    logger.error(f"Error exporting tweets for {user}: {e}")
        
        # Export user mapping - improved version that ensures we get all users
        try:
            user_path = os.path.join(output_dir, "users.parquet")
            
            # Create a more robust users export table
            con.execute("""
            CREATE TABLE users_export AS
            SELECT DISTINCT 
                COALESCE(user_id, '') AS user_id,
                COALESCE(user_screen_name, '') AS user_screen_name,
                COALESCE(user_name, '') AS user_name,
                COUNT(*) AS tweet_count
            FROM source_tweets
            WHERE user_screen_name IS NOT NULL AND user_screen_name != ''
            GROUP BY user_id, user_screen_name, user_name
            """)
            
            con.execute(f"COPY users_export TO '{user_path}' (FORMAT PARQUET)")
            
            # Log some stats about the users
            user_count = con.execute("SELECT COUNT(*) FROM users_export").fetchone()[0]
            logger.info(f"Exported {user_count} users to {user_path}")
            
            # Show top users by tweet count
            top_users = con.execute("""
                SELECT user_screen_name, tweet_count
                FROM users_export
                ORDER BY tweet_count DESC
                LIMIT 10
            """).fetchall()
            
            logger.info("Top 10 users by tweet count:")
            for username, count in top_users:
                logger.info(f"  @{username}: {count} tweets")
                
        except Exception as e:
            logger.error(f"Error exporting users: {e}")
    
    except Exception as e:
        logger.error(f"Error in processing: {e}")
        # Try to salvage what we can
        try:
            # Count what we have
            count = con.execute("SELECT COUNT(*) FROM source_tweets").fetchone()[0]
            logger.info(f"Completed processing {count} tweets before error")
            
            # Try simple export
            emergency_path = os.path.join(output_dir, "emergency_tweets.parquet")
            con.execute(f"COPY source_tweets TO '{emergency_path}' (FORMAT PARQUET)")
            logger.info(f"Emergency data export to {emergency_path}")
        except Exception as e2:
            logger.error(f"Failed emergency export: {e2}")
    
    return total_tweets

def table_exists(con, table_name):
    """Check if a table exists in the database."""
    try:
        con.execute(f"SELECT * FROM {table_name} LIMIT 0")
        return True
    except:
        return False

def debug_archive_structure(file_path):
    """Debug a specific archive file to understand its structure."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Analyze top level structure
        logger.info(f"DEBUG - Top level keys in {file_path.name}: {list(data.keys())}")
        
        # Analyze profile structure
        if 'profile' in data:
            profile = data['profile']
            logger.info(f"DEBUG - Profile type: {type(profile).__name__}")
            
            if isinstance(profile, list):
                logger.info(f"DEBUG - Profile list length: {len(profile)}")
                if len(profile) > 0:
                    logger.info(f"DEBUG - First profile item type: {type(profile[0]).__name__}")
                    if isinstance(profile[0], dict):
                        logger.info(f"DEBUG - First profile item keys: {list(profile[0].keys())}")
            elif isinstance(profile, dict):
                logger.info(f"DEBUG - Profile keys: {list(profile.keys())}")
        
        # Analyze tweets structure
        if 'tweets' in data:
            tweets = data['tweets']
            logger.info(f"DEBUG - Tweets type: {type(tweets).__name__}")
            
            if isinstance(tweets, list):
                logger.info(f"DEBUG - Tweets list length: {len(tweets)}")
                if len(tweets) > 0:
                    logger.info(f"DEBUG - First tweet item type: {type(tweets[0]).__name__}")
                    if isinstance(tweets[0], dict):
                        logger.info(f"DEBUG - First tweet item keys: {list(tweets[0].keys())}")
        
        # Analyze other important sections
        for section in ['community-tweet', 'note-tweet', 'like']:
            if section in data:
                section_data = data[section]
                logger.info(f"DEBUG - {section} type: {type(section_data).__name__}")
                
                if isinstance(section_data, list):
                    logger.info(f"DEBUG - {section} list length: {len(section_data)}")
                    if len(section_data) > 0:
                        logger.info(f"DEBUG - First {section} item type: {type(section_data[0]).__name__}")
                        if isinstance(section_data[0], dict):
                            logger.info(f"DEBUG - First {section} item keys: {list(section_data[0].keys())}")
                
    except Exception as e:
        logger.error(f"DEBUG - Error analyzing {file_path.name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Process Twitter archives with bidirectional thread reconstruction.")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archive JSON files")
    parser.add_argument('output_dir', type=Path, help="Directory to save Parquet files")
    parser.add_argument('--temp-dir', type=str, help="Custom temporary directory for storage", default=None)
    parser.add_argument('--batch-size', type=int, help="Tweets per batch", default=BATCH_SIZE)
    parser.add_argument('--inspect', action='store_true', help="Only inspect archive format without processing")
    parser.add_argument('--deep-inspect', action='store_true', help="Perform detailed inspection of all archives")
    parser.add_argument('--samples', type=int, help="Number of archives to sample during inspection", default=20)
    parser.add_argument('--debug-file', type=str, help="Debug a specific archive file", default=None)
    parser.add_argument('--resume', action='store_true', help="Resume processing from checkpoints")
    parser.add_argument('--reset', action='store_true', help="Reset checkpoints and start fresh")
    args = parser.parse_args()

    if not args.archive_dir.is_dir():
        logger.error(f"Archive directory does not exist: {args.archive_dir}")
        return
    
    # Handle reset option
    if args.reset:
        if os.path.exists(CHECKPOINT_DIR):
            shutil.rmtree(CHECKPOINT_DIR)
            logger.info("Checkpoints reset, starting fresh")
    
    # Handle debug mode
    if args.debug_file:
        debug_file = Path(args.archive_dir) / args.debug_file
        if debug_file.exists():
            logger.info(f"Debugging file: {debug_file}")
            debug_archive_structure(debug_file)
            return
        else:
            logger.error(f"Debug file not found: {debug_file}")
            return
    
    # Create a dedicated temp directory for processing
    temp_dir = args.temp_dir
    if not temp_dir:
        temp_dir = tempfile.mkdtemp(prefix="twitter_archive_")
    else:
        temp_dir = Path(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_dir = str(temp_dir.absolute())
    
    logger.info(f"Using temporary directory: {temp_dir}")
    
    try:
        start_time = time.time()
        
        # Find all archive files
        archive_files = list(args.archive_dir.glob('*.json'))
        if not archive_files:
            logger.error("No archive JSON files found.")
            return
        
        logger.info(f"Found {len(archive_files)} archive files")
        
        # Handle inspection modes
        if args.inspect:
            logger.info(f"Inspection mode: examining archive formats (sampling {args.samples} files)")
            # Look at more files for better coverage
            sample_archives = random.sample(archive_files, min(args.samples, len(archive_files)))
            for file in sample_archives:  
                inspect_archive_format(file)
            return
            
        if args.deep_inspect:
            more_detailed_archive_inspection(archive_files)
            return
        
        # Use multi-stage processing with checkpointing
        total_tweets = multi_stage_process(archive_files, temp_dir, args.output_dir, args.batch_size)
        
        total_time = time.time() - start_time
        logger.info(f"Processed {total_tweets} tweets in {total_time:.1f} seconds")
        
    except Exception as e:
        logger.exception(f"Error processing archives: {e}")
    finally:
        # Clean up temp directory if we created one
        if not args.temp_dir and temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                logger.warning(f"Could not remove temporary directory: {temp_dir}")

if __name__ == '__main__':
    main() 