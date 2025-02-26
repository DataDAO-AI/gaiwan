"""
Verify the tweet consolidation and deduplication process.

This script:
1. Checks that no duplicate tweet IDs exist
2. Verifies that likes have been properly consolidated
3. Examines the structure and quality of the consolidated data
"""

import argparse
import os
from pathlib import Path
import logging
import sys
import duckdb
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def verify_consolidation(input_file, original_file=None):
    """
    Verify that the tweet consolidation was successful.
    
    Args:
        input_file: Path to consolidated parquet file
        original_file: Optional path to original parquet file for comparison
    """
    logger.info(f"Verifying consolidation of {input_file}")
    
    try:
        # Create DuckDB connection
        con = duckdb.connect(database=':memory:')
        
        # Load consolidated tweets
        logger.info("Loading consolidated tweets...")
        con.execute(f"CREATE TABLE consolidated AS SELECT * FROM read_parquet('{input_file}')")
        
        # Check row count
        total_count = con.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        logger.info(f"Loaded {total_count} consolidated tweets")
        
        # Check for duplicate IDs
        duplicate_check = con.execute("""
        WITH dup_check AS (
            SELECT id, COUNT(*) as count 
            FROM consolidated 
            GROUP BY id 
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) FROM dup_check
        """).fetchone()[0]
        
        if duplicate_check > 0:
            logger.error(f"ERROR: Found {duplicate_check} duplicate tweet IDs!")
            
            # Sample some duplicates
            logger.info("Sample duplicates:")
            duplicates = con.execute("""
            WITH dup_ids AS (
                SELECT id FROM consolidated GROUP BY id HAVING COUNT(*) > 1 LIMIT 5
            )
            SELECT id, user_screen_name, tweet_type, array_length(liked_by_users) as like_count
            FROM consolidated 
            WHERE id IN (SELECT id FROM dup_ids)
            ORDER BY id
            """).fetchall()
            
            for d in duplicates:
                logger.info(f"  ID: {d[0]}, User: @{d[1]}, Type: {d[2]}, Likes: {d[3]}")
        else:
            logger.info("✅ No duplicate tweet IDs found - deduplication successful")
        
        # Verify liked_by_users format
        liked_by_sample = con.execute("""
        SELECT id, user_screen_name, tweet_type, liked_by_users
        FROM consolidated
        WHERE array_length(liked_by_users) > 0
        ORDER BY array_length(liked_by_users) DESC
        LIMIT 3
        """).fetchall()
        
        logger.info("Sample of liked_by_users arrays:")
        for s in liked_by_sample:
            user_list = s[3][:5]  # First 5 users who liked it
            logger.info(f"  ID: {s[0]}, Author: @{s[1]}, Type: {s[2]}, Liked by: {len(s[3])} users")
            logger.info(f"    First few likers: {', '.join('@' + u for u in user_list)}")
            if len(s[3]) > 5:
                logger.info(f"    ... and {len(s[3]) - 5} more")
        
        # Check timestamp inference
        timestamp_stats = con.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) as missing_original_timestamp,
            SUM(CASE WHEN inferred_timestamp IS NULL THEN 1 ELSE 0 END) as missing_inferred_timestamp,
            SUM(CASE WHEN created_at IS NULL AND inferred_timestamp IS NOT NULL THEN 1 ELSE 0 END) as successfully_inferred
        FROM consolidated
        """).fetchone()
        
        logger.info("\nTimestamp verification:")
        logger.info(f"  Total tweets: {timestamp_stats[0]}")
        logger.info(f"  Missing original timestamps: {timestamp_stats[1]} ({timestamp_stats[1]*100/timestamp_stats[0]:.1f}%)")
        logger.info(f"  Successfully inferred timestamps: {timestamp_stats[3]} ({timestamp_stats[3]*100/timestamp_stats[1] if timestamp_stats[1] > 0 else 0:.1f}%)")
        
        if timestamp_stats[2] > 0:
            logger.warning(f"  ⚠️ {timestamp_stats[2]} tweets ({timestamp_stats[2]*100/timestamp_stats[0]:.1f}%) still have no timestamp")
        else:
            logger.info("  ✅ All tweets have timestamps (original or inferred)")
        
        # Check like distribution - revised query
        like_stats = con.execute("""
        SELECT 
            COUNT(*) as total_tweets,
            SUM(like_count) as total_likes,
            COUNT(*) FILTER (WHERE like_count > 0) as tweets_with_likes,
            MAX(like_count) as max_likes,
            MIN(like_count) as min_likes,
            -- Average across all tweets
            AVG(like_count) as avg_likes_all,
            -- Average only for tweets that have likes
            AVG(CASE WHEN like_count > 0 THEN like_count ELSE NULL END) as avg_likes_of_liked,
            -- Median across all tweets
            MEDIAN(like_count) as median_likes_all,
            -- Median only for tweets that have likes
            MEDIAN(CASE WHEN like_count > 0 THEN like_count ELSE NULL END) as median_likes_of_liked
        FROM consolidated
        """)
        
        like_stats = like_stats.fetchone()
        
        logger.info("\nLike statistics:")
        logger.info(f"  Total tweets with likes: {like_stats[2]} ({like_stats[2]*100/like_stats[0]:.1f}%)")
        logger.info(f"  Total likes preserved: {like_stats[1]}")
        logger.info(f"  All tweets: Min={like_stats[4]}, Max={like_stats[3]}, Avg={like_stats[5]:.2f}, Median={like_stats[7]}")
        logger.info(f"  Only tweets with likes: Avg={like_stats[6]:.2f}, Median={like_stats[8]}")
        
        # Author inference analysis
        logger.info("\n=== AUTHOR INFERENCE ANALYSIS ===")
        author_stats = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE tweet_type = 'like') as total_likes,
            COUNT(*) FILTER (WHERE tweet_type = 'like' AND in_reply_to_user_id IS NOT NULL) as likes_with_reply_authors,
            COUNT(*) FILTER (WHERE tweet_type = 'like' AND in_reply_to_screen_name IS NOT NULL) as likes_with_reply_screen_names,
            COUNT(*) FILTER (WHERE tweet_type = 'like' AND user_id IS NOT NULL AND user_id != '') as likes_with_user_ids,
            COUNT(*) FILTER (WHERE tweet_type = 'like' AND user_screen_name IS NOT NULL AND user_screen_name != '') as likes_with_screen_names
        FROM consolidated
        """)
        
        author_stats = author_stats.fetchone()
        
        logger.info(f"Author information in like tweets:")
        logger.info(f"  Total like tweets: {author_stats[0]:,}")
        logger.info(f"  Like tweets with user_id: {author_stats[3]:,} ({author_stats[3]*100/author_stats[0]:.1f}%)")
        logger.info(f"  Like tweets with user_screen_name: {author_stats[4]:,} ({author_stats[4]*100/author_stats[0]:.1f}%)")
        logger.info(f"  Like tweets with in_reply_to_user_id: {author_stats[1]:,} ({author_stats[1]*100/author_stats[0]:.1f}%)")
        logger.info(f"  Like tweets with in_reply_to_screen_name: {author_stats[2]:,} ({author_stats[2]*100/author_stats[0]:.1f}%)")
        
        # Check if we need author inference
        missing_authors = con.execute("""
        SELECT COUNT(*) FROM consolidated 
        WHERE tweet_type = 'like' 
        AND (user_screen_name IS NULL OR user_screen_name = '')
        """).fetchone()[0]
        
        if missing_authors > 0:
            logger.warning(f"⚠️ {missing_authors:,} like tweets ({missing_authors*100/author_stats[0]:.1f}%) have missing authors")
            logger.info("  Author inference is needed for these tweets")
        else:
            logger.info("✅ All like tweets have attribution to users who liked them")
        
        # Most liked tweets
        logger.info("\nTop 5 tweets by like count (verified):")
        top_liked = con.execute("""
        SELECT DISTINCT
            id, 
            user_screen_name,
            like_count,
            CASE WHEN full_text IS NULL THEN '[NULL]' ELSE LEFT(full_text, 50) || '...' END as preview
        FROM consolidated
        WHERE like_count > 0
        ORDER BY like_count DESC
        LIMIT 5
        """).fetchall()
        
        for tweet in top_liked:
            logger.info(f"  ID: {tweet[0]}")
            logger.info(f"    Author: @{tweet[1]}")
            logger.info(f"    Likes: {tweet[2]}")
            logger.info(f"    Text: \"{tweet[3]}\"")
        
        # Compare with original if provided
        if original_file:
            logger.info("\nComparing with original dataset...")
            con.execute(f"CREATE TABLE original AS SELECT * FROM read_parquet('{original_file}')")
            
            orig_count = con.execute("SELECT COUNT(*) FROM original").fetchone()[0]
            orig_unique = con.execute("SELECT COUNT(DISTINCT id) FROM original").fetchone()[0]
            
            logger.info(f"Original dataset: {orig_count} total tweets, {orig_unique} unique IDs")
            logger.info(f"Reduction: {orig_count - total_count} tweets ({(orig_count - total_count)*100/orig_count:.1f}%)")
            
            # Verify all IDs preserved
            missing_ids = con.execute("""
            SELECT COUNT(DISTINCT o.id) 
            FROM original o
            LEFT JOIN consolidated c ON o.id = c.id
            WHERE c.id IS NULL
            """).fetchone()[0]
            
            if missing_ids > 0:
                logger.error(f"⚠️ {missing_ids} tweet IDs from original dataset are missing in consolidated data")
            else:
                logger.info("✅ All tweet IDs from original dataset preserved in consolidated data")
        
        # Thread analysis
        logger.info("\n=== THREAD ANALYSIS ===")
        
        # Count replies and check reply relationships
        reply_stats = con.execute("""
        SELECT
            COUNT(*) as total_tweets,
            SUM(CASE WHEN in_reply_to_status_id IS NOT NULL THEN 1 ELSE 0 END) as reply_count,
            SUM(CASE WHEN in_reply_to_status_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as reply_percentage,
            COUNT(DISTINCT in_reply_to_status_id) FILTER (WHERE in_reply_to_status_id IS NOT NULL) as distinct_parent_tweets
        FROM consolidated
        """).fetchone()
        
        logger.info(f"Reply statistics:")
        logger.info(f"  Total replies: {reply_stats[1]:,} ({reply_stats[2]:.1f}% of all tweets)")
        logger.info(f"  Distinct parent tweets: {reply_stats[3]:,}")
        
        # Thread size distribution
        thread_sizes = con.execute("""
        WITH thread_roots AS (
            -- Get tweets that appear as reply targets but aren't themselves replies
            -- (or their parents aren't in our dataset)
            SELECT id
            FROM consolidated
            WHERE id IN (
                SELECT DISTINCT in_reply_to_status_id 
                FROM consolidated 
                WHERE in_reply_to_status_id IS NOT NULL
            )
            AND (in_reply_to_status_id IS NULL OR in_reply_to_status_id NOT IN (SELECT id FROM consolidated))
        ),
        thread_counts AS (
            -- For each root, count all replies at any depth
            SELECT 
                root.id as root_id,
                COUNT(replies.id) as thread_size
            FROM thread_roots root
            LEFT JOIN consolidated replies ON root.id = replies.in_reply_to_status_id
            GROUP BY root.id
        )
        SELECT
            MIN(thread_size) as min_size,
            MAX(thread_size) as max_size,
            AVG(thread_size) as avg_size,
            MEDIAN(thread_size) as median_size,
            COUNT(*) as thread_count,
            SUM(CASE WHEN thread_size >= 10 THEN 1 ELSE 0 END) as large_threads,
            SUM(CASE WHEN thread_size >= 50 THEN 1 ELSE 0 END) as very_large_threads
        FROM thread_counts
        """).fetchone()
        
        logger.info("\nThread size distribution:")
        logger.info(f"  Total identified thread roots: {thread_sizes[4]:,}")
        logger.info(f"  Thread sizes: Min={thread_sizes[0]}, Max={thread_sizes[1]}, Avg={thread_sizes[2]:.1f}, Median={thread_sizes[3]}")
        logger.info(f"  Large threads (≥10 replies): {thread_sizes[5]:,}")
        logger.info(f"  Very large threads (≥50 replies): {thread_sizes[6]:,}")
        
        # Find the top 5 largest threads
        largest_threads = con.execute("""
        WITH RECURSIVE thread_members AS (
            -- Start with root tweets
            SELECT 
                id as root_id,
                id,
                user_screen_name,
                CAST(NULL AS VARCHAR) as parent_id,
                full_text,
                inferred_timestamp,
                0 as depth
            FROM consolidated
            WHERE id IN (
                SELECT DISTINCT in_reply_to_status_id 
                FROM consolidated 
                WHERE in_reply_to_status_id IS NOT NULL
            )
            AND (in_reply_to_status_id IS NULL OR in_reply_to_status_id NOT IN (SELECT id FROM consolidated))
            
            UNION ALL
            
            -- Add all replies recursively
            SELECT
                t.root_id,
                r.id,
                r.user_screen_name,
                r.in_reply_to_status_id,
                r.full_text,
                r.inferred_timestamp,
                t.depth + 1
            FROM thread_members t
            JOIN consolidated r ON t.id = r.in_reply_to_status_id
        ),
        thread_stats AS (
            SELECT
                root_id,
                COUNT(*) - 1 as reply_count,  -- Don't count the root
                MAX(depth) as max_depth,
                COUNT(DISTINCT user_screen_name) as participant_count,
                MIN(inferred_timestamp) as earliest_post,
                MAX(inferred_timestamp) as latest_post
            FROM thread_members
            GROUP BY root_id
            HAVING COUNT(*) > 5  -- Only threads with at least 5 total posts
            ORDER BY reply_count DESC
            LIMIT 5
        )
        SELECT
            ts.root_id,
            ts.reply_count,
            ts.max_depth,
            ts.participant_count,
            ts.earliest_post,
            ts.latest_post,
            root.user_screen_name as root_author,
            CASE WHEN root.full_text IS NULL THEN '[NULL]' 
                 ELSE LEFT(root.full_text, 50) || '...' END as root_text
        FROM thread_stats ts
        JOIN consolidated root ON ts.root_id = root.id
        ORDER BY ts.reply_count DESC
        """).fetchall()
        
        logger.info("\nLargest threads:")
        for i, thread in enumerate(largest_threads):
            (root_id, reply_count, max_depth, participants, 
             earliest, latest, author, text) = thread
            
            duration = latest - earliest if latest and earliest else None
            duration_str = f"{duration.total_seconds() / 86400:.1f} days" if duration else "Unknown"
            
            logger.info(f"Thread #{i+1} (ID: {root_id}):")
            logger.info(f"  Root author: @{author}")
            logger.info(f"  Root text: \"{text}\"")
            logger.info(f"  Stats: {reply_count} replies, {max_depth} levels deep, {participants} participants")
            logger.info(f"  Duration: {duration_str} (from {earliest} to {latest})")
            
        # Sample a thread with its conversation flow
        sample_thread = con.execute("""
        WITH RECURSIVE thread_members AS (
            -- Start with root tweet of one of the largest threads
            SELECT 
                id,
                user_screen_name,
                CAST(NULL AS VARCHAR) as parent_id,
                full_text,
                inferred_timestamp,
                0 as depth,
                ARRAY[id] as path  -- Track the path for ordering
            FROM consolidated
            WHERE id = (
                SELECT root_id FROM (
                    SELECT 
                        id as root_id,
                        COUNT(*) OVER (PARTITION BY id) as reply_count
                    FROM consolidated
                    WHERE id IN (
                        SELECT DISTINCT in_reply_to_status_id 
                        FROM consolidated 
                        WHERE in_reply_to_status_id IS NOT NULL
                    )
                    ORDER BY reply_count DESC
                    LIMIT 1
                )
            )
            
            UNION ALL
            
            -- Add direct replies only (one level at a time for better sample)
            SELECT
                r.id,
                r.user_screen_name,
                r.in_reply_to_status_id,
                r.full_text,
                r.inferred_timestamp,
                t.depth + 1,
                array_append(t.path, r.id)  -- Use array_append instead of || operator
            FROM thread_members t
            JOIN consolidated r ON t.id = r.in_reply_to_status_id
            WHERE t.depth < 3  -- Only go 3 levels deep for the sample
        )
        SELECT
            id,
            user_screen_name,
            parent_id,
            depth,
            inferred_timestamp,
            CASE WHEN full_text IS NULL THEN '[NULL]' 
                 ELSE LEFT(full_text, 50) || '...' END as preview
        FROM thread_members
        ORDER BY path, inferred_timestamp
        LIMIT 10
        """).fetchall()
        
        if sample_thread:
            logger.info("\nSample thread conversation:")
            for tweet in sample_thread:
                indent = "  " * tweet[3]  # Indent based on depth
                logger.info(f"{indent}@{tweet[1]} ({tweet[4]}):")
                logger.info(f"{indent}\"{tweet[5]}\"")
        
        # Add cross-user interactions analysis
        user_interactions = con.execute("""
        SELECT 
            COUNT(DISTINCT CONCAT(c.user_screen_name, '-', 
                                 COALESCE(parent.user_screen_name, 'unknown'))) as interaction_pairs,
            COUNT(*) FILTER (WHERE c.user_screen_name != COALESCE(parent.user_screen_name, '')) as cross_user_replies
        FROM consolidated c
        LEFT JOIN consolidated parent ON c.in_reply_to_status_id = parent.id
        WHERE c.in_reply_to_status_id IS NOT NULL
        """).fetchone()
        
        logger.info("\nUser interaction analysis:")
        logger.info(f"  Unique user-to-user interaction pairs: {user_interactions[0]:,}")
        logger.info(f"  Cross-user replies (not self-replies): {user_interactions[1]:,}")
        
    except Exception as e:
        logger.error(f"Error during verification: {e}", exc_info=True)
        return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Verify Twitter data consolidation")
    parser.add_argument('consolidated_file', type=str, help="Path to consolidated parquet file")
    parser.add_argument('--original', type=str, help="Path to original parquet file for comparison", default=None)
    
    args = parser.parse_args()
    
    consolidated_path = Path(args.consolidated_file)
    original_path = Path(args.original) if args.original else None
    
    if not consolidated_path.exists():
        logger.error(f"Consolidated file not found: {consolidated_path}")
        return 1
    
    if original_path and not original_path.exists():
        logger.error(f"Original file not found: {original_path}")
        return 1
    
    success = verify_consolidation(consolidated_path, original_path)
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main()) 