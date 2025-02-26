"""
Utility script to inspect Twitter archive processing results.
Provides comprehensive verification across all tweet types and structures.
"""

import argparse
import os
from pathlib import Path
import logging
import traceback
import sys
import pandas as pd
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def inspect_parquet_files(output_dir):
    """Inspect the processed parquet files with comprehensive verification."""
    tweets_path = os.path.join(output_dir, "processed_tweets.parquet")
    users_path = os.path.join(output_dir, "users.parquet")
    
    # Check directory contents
    logger.info(f"Checking contents of {output_dir}")
    dir_contents = os.listdir(output_dir)
    parquet_files = [f for f in dir_contents if f.endswith('.parquet')]
    logger.info(f"Found {len(parquet_files)} parquet files in directory: {', '.join(parquet_files)}")
    
    # Basic validation
    if not os.path.exists(tweets_path):
        logger.error(f"Tweets file not found: {tweets_path}")
        logger.info("Searching for alternative tweet files...")
        tweet_candidates = [f for f in dir_contents if 'tweet' in f.lower() and f.endswith('.parquet')]
        if tweet_candidates:
            logger.info(f"Found alternative tweet files: {tweet_candidates}")
            tweets_path = os.path.join(output_dir, tweet_candidates[0])
            logger.info(f"Using {tweets_path} instead")
        else:
            return
    
    if not os.path.exists(users_path):
        logger.error(f"Users file not found: {users_path}")
        logger.info("Searching for alternative user files...")
        user_candidates = [f for f in dir_contents if 'user' in f.lower() and f.endswith('.parquet')]
        if user_candidates:
            logger.info(f"Found alternative user files: {user_candidates}")
            users_path = os.path.join(output_dir, user_candidates[0])
            logger.info(f"Using {users_path} instead")
    
    # Check file sizes
    tweets_size = os.path.getsize(tweets_path) / (1024 * 1024)  # MB
    logger.info(f"Tweets parquet file size: {tweets_size:.2f} MB")
    
    if os.path.exists(users_path):
        users_size = os.path.getsize(users_path) / (1024 * 1024)  # MB
        logger.info(f"Users parquet file size: {users_size:.2f} MB")
    
    try:
        # Connect to DuckDB for efficient analysis
        logger.info("Connecting to DuckDB")
        con = duckdb.connect(database=':memory:')
        
        # Load the data into DuckDB
        logger.info(f"Loading tweets from {tweets_path}")
        try:
            con.execute(f"CREATE TABLE tweets AS SELECT * FROM read_parquet('{tweets_path}')")
            
            # Verify table creation
            table_count = con.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
            logger.info(f"Successfully created tweets table with {table_count} rows")
        except Exception as e:
            logger.error(f"Error loading tweets into DuckDB: {str(e)}")
            logger.error(traceback.format_exc())
            return
        
        if os.path.exists(users_path):
            logger.info(f"Loading users from {users_path}")
            try:
                con.execute(f"CREATE TABLE users AS SELECT * FROM read_parquet('{users_path}')")
            except Exception as e:
                logger.error(f"Error loading users into DuckDB: {str(e)}")
        else:
            logger.warning("Skipping users table analysis (file not found)")
        
        # Get basic stats
        logger.info("Getting basic tweet stats")
        tweet_count = con.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
        logger.info(f"Total tweets: {tweet_count}")
        
        # Get column list to understand table structure
        columns = con.execute("PRAGMA table_info(tweets)").fetchall()
        logger.info(f"Tweet table columns ({len(columns)}):")
        for col in columns:
            logger.info(f"  {col[1]} ({col[2]})")
        
        # Check if user table exists
        if table_exists(con, "users"):
            user_count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            logger.info(f"Total users: {user_count}")
            
            # Show top users
            top_users = con.execute("""
                SELECT user_screen_name, tweet_count
                FROM users
                ORDER BY tweet_count DESC
                LIMIT 10
            """).fetchall()
            
            if top_users:
                logger.info("Top 10 users by tweet count:")
                for username, count in top_users:
                    logger.info(f"  @{username}: {count} tweets")
        
        # Get tweet type distribution
        logger.info("Tweet type distribution:")
        type_dist = con.execute("""
        SELECT 
            tweet_type, 
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tweets), 2) as percentage
        FROM tweets
        GROUP BY tweet_type
        ORDER BY count DESC
        """).fetchall()
        
        for tweet_type, count, percentage in type_dist:
            logger.info(f"  {tweet_type}: {count} ({percentage}%)")
        
        # Check for missing user info by tweet type
        logger.info("User attribution check by tweet type:")
        missing_users = con.execute("""
        SELECT 
            tweet_type, 
            COUNT(*) as total,
            SUM(CASE WHEN user_screen_name IS NULL OR user_screen_name = '' THEN 1 ELSE 0 END) as missing_username,
            ROUND(100.0 * SUM(CASE WHEN user_screen_name IS NULL OR user_screen_name = '' THEN 1 ELSE 0 END) / COUNT(*), 2) as percent_missing
        FROM tweets
        GROUP BY tweet_type
        ORDER BY percent_missing DESC
        """).fetchall()
        
        for tweet_type, total, missing, percent in missing_users:
            logger.info(f"  {tweet_type}: {missing}/{total} missing usernames ({percent}%)")
        
        # Check timestamp availability by tweet type
        logger.info("Timestamp availability by tweet type:")
        missing_timestamps = con.execute("""
        SELECT 
            tweet_type, 
            COUNT(*) as total,
            SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) as missing_timestamp,
            ROUND(100.0 * SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as percent_missing
        FROM tweets
        GROUP BY tweet_type
        ORDER BY percent_missing DESC
        """).fetchall()
        
        for tweet_type, total, missing, percent in missing_timestamps:
            logger.info(f"  {tweet_type}: {missing}/{total} missing timestamps ({percent}%)")
        
        # Check reply statistics
        if any(col[1] == 'in_reply_to_status_id' for col in columns):
            reply_count = con.execute("""
            SELECT COUNT(*) FROM tweets 
            WHERE in_reply_to_status_id IS NOT NULL
            """).fetchone()[0]
            
            logger.info(f"Replies: {reply_count} ({round(reply_count * 100.0 / tweet_count, 2)}%)")
            
            # Look at thread structure (sample)
            logger.info("Thread structure examples:")
            
            # Find threads with at least 5 tweets
            thread_samples = con.execute("""
            WITH thread_counts AS (
                SELECT in_reply_to_status_id as thread_id, COUNT(*) as reply_count
                FROM tweets
                WHERE in_reply_to_status_id IS NOT NULL
                GROUP BY in_reply_to_status_id
                HAVING COUNT(*) >= 5
            )
            SELECT thread_id
            FROM thread_counts
            ORDER BY reply_count DESC
            LIMIT 5
            """).fetchall()
            
            if thread_samples:
                for i, (thread_id,) in enumerate(thread_samples):
                    thread_tweets = con.execute(f"""
                    SELECT
                        id,
                        user_screen_name,
                        created_at,
                        CASE 
                            WHEN full_text IS NULL THEN '[NULL]'
                            ELSE LEFT(full_text, 50) 
                        END as preview,
                        in_reply_to_status_id
                    FROM tweets
                    WHERE id = '{thread_id}'
                       OR in_reply_to_status_id = '{thread_id}'
                    ORDER BY created_at
                    """).fetchall()
                    
                    logger.info(f"Thread {i+1} (Root ID: {thread_id}) - {len(thread_tweets)} tweets:")
                    for tweet in thread_tweets[:5]:  # Show first 5 tweets in thread
                        tweet_id, user, timestamp, preview, reply_to = tweet
                        logger.info(f"  {timestamp} @{user}: {preview}...")
                    
                    if len(thread_tweets) > 5:
                        logger.info(f"  ... and {len(thread_tweets) - 5} more replies")
                    
                    logger.info("")
            else:
                logger.warning("No threads with 5+ replies found")
        else:
            logger.warning("in_reply_to_status_id column not found in table")
            
        # Sample each tweet type specifically
        logger.info("\n=== COMPREHENSIVE TWEET TYPE SAMPLES ===")
        
        tweet_types = [type_info[0] for type_info in type_dist]
        for tweet_type in tweet_types:
            logger.info(f"\nSamples of '{tweet_type}' tweets:")
            try:
                samples = con.execute(f"""
                SELECT
                    id,
                    user_screen_name,
                    created_at,
                    CASE WHEN full_text IS NULL THEN '[NULL]' ELSE LEFT(full_text, 100) END as preview,
                    in_reply_to_status_id,
                    archive_file
                FROM tweets
                WHERE tweet_type = '{tweet_type}'
                ORDER BY random()
                LIMIT 3
                """).fetchall()
                
                for i, sample in enumerate(samples):
                    tweet_id, user, timestamp, preview, reply_to, archive = sample
                    logger.info(f"Sample {i+1}:")
                    logger.info(f"  ID: {tweet_id}")
                    logger.info(f"  User: @{user}")
                    logger.info(f"  Created: {timestamp}")
                    logger.info(f"  Reply to: {reply_to if reply_to else 'N/A'}")
                    logger.info(f"  From archive: {archive}")
                    logger.info(f"  Text: {preview}...")
                
            except Exception as e:
                logger.error(f"Error getting {tweet_type} samples: {str(e)}")
        
        # Check for any NULL values in critical fields
        logger.info("\nData quality check - NULL values in critical fields:")
        null_counts = con.execute("""
        SELECT
            tweet_type,
            COUNT(*) as total,
            SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id,
            SUM(CASE WHEN user_screen_name IS NULL OR user_screen_name = '' THEN 1 ELSE 0 END) as null_screen_name,
            SUM(CASE WHEN full_text IS NULL THEN 1 ELSE 0 END) as null_text
        FROM tweets
        GROUP BY tweet_type
        """).fetchall()
        
        for tweet_type, total, null_id, null_screen_name, null_text in null_counts:
            logger.info(f"  {tweet_type} ({total} tweets):")
            logger.info(f"    - NULL IDs: {null_id} ({round(null_id * 100.0 / total if total > 0 else 0, 2)}%)")
            logger.info(f"    - NULL screen names: {null_screen_name} ({round(null_screen_name * 100.0 / total if total > 0 else 0, 2)}%)")
            logger.info(f"    - NULL text: {null_text} ({round(null_text * 100.0 / total if total > 0 else 0, 2)}%)")
            
        # Check archive distribution
        logger.info("\nArchive distribution (top 10):")
        archive_dist = con.execute("""
        SELECT 
            archive_file,
            COUNT(*) as tweet_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tweets), 3) as percentage
        FROM tweets
        GROUP BY archive_file
        ORDER BY tweet_count DESC
        LIMIT 10
        """).fetchall()
        
        for archive, count, percentage in archive_dist:
            logger.info(f"  {archive}: {count} tweets ({percentage}%)")
            
        # Add duplicate tweet ID analysis
        logger.info("\n=== DUPLICATE TWEET ANALYSIS ===")
        
        # Check for duplicate tweet IDs
        duplicate_counts = con.execute("""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT id, COUNT(*) as count
            FROM tweets
            GROUP BY id
            HAVING COUNT(*) > 1
        ) as dups
        """).fetchone()[0]
        
        if duplicate_counts > 0:
            logger.info(f"Found {duplicate_counts} unique tweet IDs with duplicates")
            
            # Analyze duplicate distribution by tweet type
            logger.info("Duplicate tweet distribution by type combination:")
            type_combinations = con.execute("""
            WITH tweet_types AS (
                SELECT 
                    id,
                    tweet_type,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY 
                        CASE 
                            WHEN tweet_type = 'tweet' THEN 1
                            WHEN tweet_type = 'note_tweet' THEN 2
                            WHEN tweet_type = 'community_tweet' THEN 3
                            WHEN tweet_type = 'like' THEN 4
                            ELSE 5
                        END
                    ) as row_num
                FROM tweets
                WHERE id IN (
                    SELECT id
                    FROM tweets
                    GROUP BY id
                    HAVING COUNT(*) > 1
                )
            )
            SELECT 
                a.tweet_type as type1,
                b.tweet_type as type2,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM (
                    SELECT id FROM tweets GROUP BY id HAVING COUNT(*) > 1
                )), 2) as percentage
            FROM tweet_types a
            JOIN tweet_types b ON a.id = b.id AND a.row_num = 1 AND b.row_num = 2
            GROUP BY type1, type2
            ORDER BY count DESC
            """).fetchall()
            
            for type1, type2, count, percentage in type_combinations:
                logger.info(f"  {type1} + {type2}: {count} ({percentage}%)")
            
            # Sample some duplicates for inspection
            logger.info("\nSample duplicate tweets:")
            duplicate_samples = con.execute("""
            WITH duplicates AS (
                SELECT id
                FROM tweets
                GROUP BY id
                HAVING COUNT(*) > 1
                ORDER BY RANDOM()
                LIMIT 5
            )
            SELECT 
                t.id,
                t.user_screen_name,
                t.tweet_type,
                t.created_at,
                CASE WHEN t.full_text IS NULL THEN '[NULL]' ELSE LEFT(t.full_text, 50) END as preview,
                t.archive_file
            FROM tweets t
            JOIN duplicates d ON t.id = d.id
            ORDER BY t.id, 
                CASE 
                    WHEN t.tweet_type = 'tweet' THEN 1
                    WHEN t.tweet_type = 'note_tweet' THEN 2
                    WHEN t.tweet_type = 'community_tweet' THEN 3
                    WHEN t.tweet_type = 'like' THEN 4
                    ELSE 5
                END
            """).fetchall()
            
            current_id = None
            for i, (id, user, tweet_type, timestamp, preview, archive) in enumerate(duplicate_samples):
                if id != current_id:
                    if current_id is not None:
                        logger.info("")
                    current_id = id
                    logger.info(f"Duplicate set for tweet ID {id}:")
                
                logger.info(f"  Version {i % 2 + 1}:")
                logger.info(f"    Type: {tweet_type}")
                logger.info(f"    User: @{user}")
                logger.info(f"    Created: {timestamp}")
                logger.info(f"    Archive: {archive}")
                logger.info(f"    Text: {preview}...")
            
            # Recommend a deduplication strategy
            like_vs_full = con.execute("""
            WITH dup_ids AS (
                SELECT id
                FROM tweets
                GROUP BY id
                HAVING COUNT(*) > 1
            ),
            like_and_full AS (
                SELECT id
                FROM (
                    SELECT id, 
                          SUM(CASE WHEN tweet_type = 'like' THEN 1 ELSE 0 END) as like_count,
                          SUM(CASE WHEN tweet_type != 'like' THEN 1 ELSE 0 END) as full_count
                    FROM tweets
                    WHERE id IN (SELECT id FROM dup_ids)
                    GROUP BY id
                ) t
                WHERE like_count > 0 AND full_count > 0
            )
            SELECT COUNT(*) 
            FROM like_and_full
            """).fetchone()[0]
            
            logger.info(f"\nFound {like_vs_full} duplicate IDs that have both 'like' and full tweet versions")
            
            if like_vs_full > 0:
                logger.info("Deduplication recommendation:")
                logger.info("  - Keep full tweet versions and discard duplicate 'like' versions")
                logger.info("  - When multiple full versions exist, keep the one with the most complete data")
                
                # Calculate size reduction
                total_duplicate_rows = con.execute("""
                SELECT SUM(count) - COUNT(*)
                FROM (
                    SELECT id, COUNT(*) as count
                    FROM tweets
                    GROUP BY id
                    HAVING COUNT(*) > 1
                ) t
                """).fetchone()[0]
                
                reduction_percentage = round(total_duplicate_rows * 100.0 / tweet_count, 2)
                logger.info(f"Deduplication would remove approximately {total_duplicate_rows} rows ({reduction_percentage}% of dataset)")
                
        else:
            logger.info("No duplicate tweet IDs found in the dataset")
        
    except Exception as e:
        logger.error(f"Error during inspection: {str(e)}")
        logger.error(traceback.format_exc())

def table_exists(con, table_name):
    """Check if a table exists in the database."""
    try:
        con.execute(f"SELECT * FROM {table_name} LIMIT 0")
        return True
    except:
        return False

def main():
    parser = argparse.ArgumentParser(description="Comprehensive inspection of Twitter archive processing results")
    parser.add_argument('output_dir', type=Path, help="Directory containing processed Parquet files")
    args = parser.parse_args()
    
    if not args.output_dir.is_dir():
        logger.error(f"Output directory does not exist: {args.output_dir}")
        return
    
    # Set excepthook to ensure we see full exception traces
    sys.excepthook = lambda exc_type, exc_value, exc_traceback: logger.error(
        "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    )
    
    inspect_parquet_files(args.output_dir)

if __name__ == '__main__':
    main() 