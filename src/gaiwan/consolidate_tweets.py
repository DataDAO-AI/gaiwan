"""
Consolidate duplicate tweets and add timestamp inference.

This script:
1. Deduplicates tweets, prioritizing full tweet versions over likes
2. Preserves all users who liked each tweet in a "liked_by" array
3. Infers timestamps for tweets (especially likes) using Snowflake ID
"""

import argparse
import os
from pathlib import Path
import logging
import time
import sys
from datetime import datetime, timezone
import pandas as pd
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def extract_timestamp_from_id(tweet_id):
    """
    Extract timestamp from Twitter Snowflake ID.
    Twitter Snowflake IDs are 64-bit values where the first 41 bits represent
    milliseconds since Twitter epoch (2010-11-04).
    """
    try:
        # Convert ID to integer if it's a string
        tweet_id = int(tweet_id)
        
        # Extract timestamp bits and shift
        timestamp_ms = (tweet_id >> 22) + 1288834974657  # Twitter epoch offset
        
        # Convert to datetime
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        return timestamp
    except (ValueError, TypeError):
        return None

def register_extract_timestamp(con):
    """Register the extract_timestamp function with DuckDB."""
    con.create_function(
        'extract_timestamp_from_id', 
        extract_timestamp_from_id, 
        ['VARCHAR'], 
        'TIMESTAMP'
    )

def consolidate_tweets(input_file, output_file, batch_size=1000000):
    """
    Consolidate duplicate tweets while preserving like information.
    
    Args:
        input_file: Path to input parquet file
        output_file: Path to output parquet file
        batch_size: Batch size for processing large files
    """
    start_time = time.time()
    logger.info(f"Starting tweet consolidation from {input_file}")
    
    # Create DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Register timestamp extraction function
    register_extract_timestamp(con)
    
    # Load tweets
    logger.info("Loading tweets into DuckDB...")
    con.execute(f"CREATE TABLE tweets AS SELECT * FROM read_parquet('{input_file}')")
    
    # Get count of original tweets
    original_count = con.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
    logger.info(f"Loaded {original_count} tweets")
    
    # Count distinct tweet IDs
    unique_ids = con.execute("SELECT COUNT(DISTINCT id) FROM tweets").fetchone()[0]
    logger.info(f"Found {unique_ids} unique tweet IDs")
    
    # IMPORTANT: Extract all likers from the FULL dataset before deduplication
    logger.info("Extracting all likers from the original dataset...")
    con.execute("""
    CREATE TABLE likes_by_user AS
    SELECT 
        id,
        user_screen_name as liker_screen_name
    FROM tweets 
    WHERE tweet_type = 'like' AND user_screen_name IS NOT NULL
    """)
    
    like_count = con.execute("SELECT COUNT(*) FROM likes_by_user").fetchone()[0]
    unique_liked = con.execute("SELECT COUNT(DISTINCT id) FROM likes_by_user").fetchone()[0]
    logger.info(f"Extracted {like_count} like records for {unique_liked} unique tweets")
    
    # Now deduplicate the tweets for further processing
    logger.info("Deduplicating tweets for processing...")
    con.execute("""
    CREATE TABLE tweets_deduplicated AS
    WITH ranked_duplicates AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY 
                tweet_type != 'like' DESC, -- Prioritize non-like tweets
                created_at IS NOT NULL DESC, -- Prefer tweets with timestamps
                LENGTH(COALESCE(full_text, '')) DESC -- Prefer tweets with more content
            ) as rank
        FROM tweets
    )
    SELECT * EXCLUDE(rank) FROM ranked_duplicates WHERE rank = 1
    """)
    
    con.execute("DROP TABLE tweets")
    con.execute("ALTER TABLE tweets_deduplicated RENAME TO tweets")
    
    # First, ensure we have columns we need and fix any potential issues
    logger.info("Preparing data for consolidation...")

    # IMPORTANT: Infer authors for like tweets BEFORE consolidation
    logger.info("Inferring original authors for like tweets...")
    
    # Fix the author inference table creation
    con.execute("""
    CREATE TABLE inferred_authors AS
    WITH reply_info AS (
        -- Find tweets that are replies to our like tweets
        SELECT 
            l.id as liked_tweet_id,
            r.in_reply_to_user_id as author_id,
            r.in_reply_to_screen_name as author_screen_name,
            -- Pick the most common screen name for each tweet
            COUNT(*) as occurrence_count
        FROM tweets l
        JOIN tweets r ON l.id = r.in_reply_to_status_id
        WHERE 
            l.tweet_type = 'like' AND
            r.in_reply_to_screen_name IS NOT NULL
        GROUP BY l.id, r.in_reply_to_user_id, r.in_reply_to_screen_name
    ),
    ranked_authors AS (
        -- For each tweet, pick the most frequently occurring author
        SELECT 
            liked_tweet_id,
            author_id, 
            author_screen_name,
            ROW_NUMBER() OVER (PARTITION BY liked_tweet_id ORDER BY occurrence_count DESC) as rank
        FROM reply_info
    )
    -- Only keep one author per tweet (the most common one)
    SELECT 
        liked_tweet_id, 
        author_id, 
        author_screen_name
    FROM ranked_authors
    WHERE rank = 1
    """)
    
    # Add verification counts
    inferred_count = con.execute("SELECT COUNT(*) FROM inferred_authors").fetchone()[0]
    logger.info(f"Inferred {inferred_count} authors from reply relationships")
    
    # Add detailed verification
    con.execute("""
    CREATE TABLE author_verification AS
    SELECT 
        COUNT(DISTINCT ia.liked_tweet_id) as distinct_tweets_with_authors,
        COUNT(*) as total_author_records
    FROM inferred_authors ia
    """)
    
    verification = con.execute("SELECT * FROM author_verification").fetchone()
    logger.info(f"Verification: {verification[0]} distinct tweets with inferred authors, {verification[1]} total records")
    if verification[0] != verification[1]:
        logger.warning(f"⚠️ Mismatch in author counts! This indicates duplicate author assignments")
    
    # First, add an index to improve join performance
    logger.info("Creating indexes for faster processing...")
    con.execute("CREATE INDEX idx_tweets_id ON tweets(id)")
    con.execute("CREATE INDEX idx_inferred_liked_id ON inferred_authors(liked_tweet_id)")
    
    # Process non-like tweets (regular tweets)
    logger.info("Processing non-like tweets (faster step)...")
    con.execute("""
    CREATE TABLE tweets_regular AS
    SELECT 
        t.*,
        CAST(0 AS BOOLEAN) as is_like,
        NULL as liker_screen_name
    FROM tweets t
    WHERE tweet_type != 'like'
    """)

    # Process like tweets with author inference (only tracking the necessary fields)
    logger.info("Processing like tweets with author inference...")
    con.execute("""
    CREATE TABLE tweets_likes AS
    SELECT 
        t.id, 
        -- Only use inferred author ID if available, otherwise NULL
        ia.author_id as user_id,
        ia.author_screen_name as user_screen_name,
        -- Keep other information from the original tweet
        t.user_name,
        t.in_reply_to_status_id,
        t.in_reply_to_user_id,
        t.in_reply_to_screen_name,
        t.retweet_count,
        t.favorite_count,
        t.full_text,
        t.lang,
        t.source,
        t.created_at,
        t.favorited,
        t.retweeted,
        t.possibly_sensitive,
        t.urls,
        t.media,
        t.hashtags,
        t.user_mentions,
        t.tweet_type,
        t.archive_file,
        t.is_reply,
        CAST(1 AS BOOLEAN) as is_like,
        t.user_screen_name as liker_screen_name
    FROM tweets t
    LEFT JOIN inferred_authors ia ON t.id = ia.liked_tweet_id
    WHERE t.tweet_type = 'like'
    """)

    # Combine the regular and like tweets
    logger.info("Combining processed tweets...")
    con.execute("""
    CREATE TABLE tweets_prep AS
    SELECT * FROM tweets_regular
    UNION ALL
    SELECT * FROM tweets_likes
    """)
    
    # Simplify author stats to focus on what we really care about
    author_stats = con.execute("""
    SELECT
        COUNT(*) as total_like_tweets,
        SUM(CASE WHEN user_screen_name IS NOT NULL THEN 1 ELSE 0 END) as with_inferred_authors,
        SUM(CASE WHEN user_screen_name IS NULL THEN 1 ELSE 0 END) as without_authors
    FROM tweets_prep
    WHERE is_like
    """).fetchone()
    
    logger.info(f"Author inference results:")
    logger.info(f"  - Total like tweets: {author_stats[0]}")
    logger.info(f"  - With inferred authors: {author_stats[1]} ({author_stats[1]*100/author_stats[0]:.1f}%)")
    logger.info(f"  - Missing authors: {author_stats[2]} ({author_stats[2]*100/author_stats[0]:.1f}%)")

    # Create consolidated table with best version of each tweet
    logger.info("Consolidating tweets (this may take a while)...")
    
    # First, get the best version of each tweet
    con.execute("""
    CREATE TABLE best_versions AS
    WITH ranked_tweets AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id 
                ORDER BY 
                    -- Prioritize full tweets over likes
                    CASE 
                        WHEN tweet_type = 'tweet' THEN 1
                        WHEN tweet_type = 'note_tweet' THEN 2
                        WHEN tweet_type = 'community_tweet' THEN 3
                        WHEN tweet_type = 'like' THEN 4
                        ELSE 5
                    END,
                    -- Secondary sorting criteria
                    created_at IS NOT NULL DESC,
                    LENGTH(COALESCE(full_text, '')) DESC
            ) as row_num
        FROM tweets_prep
    )
    SELECT * FROM ranked_tweets
    WHERE row_num = 1
    """)
    
    # Now, collect all users who liked each tweet
    con.execute("""
    CREATE TABLE tweet_likers AS
    SELECT 
        id,
        LIST(DISTINCT liker_screen_name ORDER BY liker_screen_name) as liked_by_users,
        COUNT(DISTINCT liker_screen_name) as like_count
    FROM likes_by_user
    GROUP BY id
    """)
    
    # Create the final consolidated table
    con.execute("""
    CREATE TABLE consolidated_tweets AS
    SELECT 
        b.id, 
        b.user_id,
        b.user_screen_name,
        b.user_name,
        b.in_reply_to_status_id,
        b.in_reply_to_user_id,
        b.in_reply_to_screen_name,
        b.retweet_count,
        b.favorite_count,
        b.full_text,
        b.lang,
        b.source,
        b.created_at,
        b.favorited,
        b.retweeted,
        b.possibly_sensitive,
        b.urls,
        b.media,
        b.hashtags,
        b.user_mentions,
        b.tweet_type,
        b.archive_file,
        b.is_reply,
        COALESCE(l.liked_by_users, []) as liked_by_users,
        COALESCE(l.like_count, 0) as like_count,
        -- Infer timestamp if missing (especially for likes)
        CASE 
            WHEN b.created_at IS NULL THEN extract_timestamp_from_id(b.id)
            ELSE b.created_at
        END as inferred_timestamp
    FROM best_versions b
    LEFT JOIN tweet_likers l ON b.id = l.id
    """)
    
    # Count consolidated tweets
    consolidated_count = con.execute("SELECT COUNT(*) FROM consolidated_tweets").fetchone()[0]
    logger.info(f"Consolidated to {consolidated_count} tweets (removed {original_count - consolidated_count} duplicates)")
    
    # Show statistics about likes conversion
    like_stats = con.execute("""
    SELECT 
        SUM(like_count) as total_likes_preserved,
        SUM(CASE WHEN array_length(liked_by_users) > 0 THEN 1 ELSE 0 END) as tweets_with_likes,
        MAX(array_length(liked_by_users)) as max_likes_per_tweet,
        AVG(array_length(liked_by_users)) as avg_likes_per_tweet
    FROM consolidated_tweets
    WHERE array_length(liked_by_users) > 0
    """).fetchone()
    
    logger.info(f"Like information preserved: {like_stats[0]} total likes across {like_stats[1]} tweets")
    logger.info(f"Max likes per tweet: {like_stats[2]}, Average likes per tweet: {round(like_stats[3], 2)}")
    
    # Timestamp inference stats
    timestamp_stats = con.execute("""
    SELECT 
        COUNT(*) as total_tweets,
        SUM(CASE WHEN created_at IS NULL AND inferred_timestamp IS NOT NULL THEN 1 ELSE 0 END) as inferred_timestamps,
        SUM(CASE WHEN inferred_timestamp IS NULL THEN 1 ELSE 0 END) as missing_timestamps
    FROM consolidated_tweets
    """).fetchone()
    
    logger.info(f"Timestamp inference: {timestamp_stats[1]} timestamps inferred")
    if timestamp_stats[2] > 0:
        logger.warning(f"Warning: {timestamp_stats[2]} tweets still have no timestamp")
    
    # Double check for duplicates after consolidation
    dup_check = con.execute("""
    SELECT COUNT(*) FROM (
        SELECT id, COUNT(*) 
        FROM consolidated_tweets 
        GROUP BY id 
        HAVING COUNT(*) > 1
    )
    """).fetchone()[0]
    
    if dup_check > 0:
        logger.error(f"ERROR: Still have {dup_check} duplicate IDs after consolidation!")
    else:
        logger.info("Verification: No duplicate IDs in consolidated data ✓")
    
    # Export to parquet
    logger.info(f"Exporting consolidated tweets to {output_file}...")
    con.execute(f"COPY consolidated_tweets TO '{output_file}' (FORMAT PARQUET)")
    
    # Calculate time taken
    elapsed = time.time() - start_time
    logger.info(f"Consolidation completed in {elapsed:.2f} seconds")
    
    # Show top tweets by like count
    logger.info("Top 5 tweets by like count:")
    top_liked = con.execute("""
    SELECT 
        id, 
        user_screen_name,
        like_count,
        CASE WHEN full_text IS NULL THEN '[NULL]' ELSE LEFT(full_text, 50) || '...' END as preview
    FROM consolidated_tweets
    ORDER BY like_count DESC
    LIMIT 5
    """).fetchall()
    
    for tweet in top_liked:
        logger.info(f"  {tweet[0]} by @{tweet[1]}: {tweet[2]} likes - \"{tweet[3]}\"")
    
    return consolidated_count

def main():
    parser = argparse.ArgumentParser(description="Consolidate duplicate tweets and infer timestamps")
    parser.add_argument('input_file', type=str, help="Path to input parquet file")
    parser.add_argument('output_file', type=str, help="Path to output parquet file")
    parser.add_argument('--batch-size', type=int, default=1000000, help="Batch size for processing")
    
    args = parser.parse_args()
    
    input_path = Path(args.input_file)
    output_path = Path(args.output_file)
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        consolidate_tweets(input_path, output_path, args.batch_size)
        return 0
    except Exception as e:
        logger.error(f"Error during consolidation: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    sys.exit(main()) 