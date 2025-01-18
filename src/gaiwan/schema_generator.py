"""Simpler script to convert Twitter archive JSON to Parquet using DuckDB."""

import argparse
import json
import logging
import os
from pathlib import Path

import duckdb

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Convert Twitter archive JSON to Parquet.")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archive JSON files.")
    parser.add_argument('output_dir', type=Path, help="Directory to save Parquet files.")
    args = parser.parse_args()

    # Collect JSON files
    json_files = list(args.archive_dir.glob('*.json'))
    if not json_files:
        raise FileNotFoundError("No archive JSON files found in the specified directory.")

    # Connect to DuckDB (in-memory)
    con = duckdb.connect(database=':memory:')

    # Define the Parquet output path
    parquet_path = args.output_dir / "archive_converted.parquet"

    # Prepare list of JSON file paths as strings
    json_files_list = ",".join([f"'{str(file)}'" for file in json_files])

    # Main query to convert to Parquet
    query = f"""
    COPY (
        WITH all_tweets AS (
            -- Regular tweets
            SELECT 
                t.tweet.id_str,
                t.tweet.in_reply_to_status_id_str,
                CAST(t.tweet.retweet_count AS BIGINT) AS retweet_count,
                CAST(t.tweet.favorite_count AS BIGINT) AS favorite_count,
                t.tweet.full_text,
                t.tweet.text,
                t.tweet.lang,
                t.tweet.source,
                t.tweet.created_at,
                t.tweet.favorited,
                t.tweet.retweeted,
                t.tweet.possibly_sensitive,
                t.tweet.entities,
                t.tweet.extended_entities,
                'tweet' AS tweet_type 
            FROM read_json_auto({json_files_list}, 
                maximum_object_size=2147483648
            ) j,
            UNNEST(j.tweets) AS t(tweet)  -- Correctly aliasing the 'tweet' object
            WHERE t.tweet.id_str IS NOT NULL

            UNION ALL

            -- Note tweets
            SELECT 
                n.noteTweetId AS id_str,
                NULL AS in_reply_to_status_id_str,
                NULL AS retweet_count,
                NULL AS favorite_count,
                n.core.text AS full_text,
                n.core.text AS text,
                NULL AS lang,
                NULL AS source,
                n.createdAt AS created_at,
                NULL AS favorited,
                NULL AS retweeted,
                NULL AS possibly_sensitive,
                NULL AS entities,
                NULL AS extended_entities,
                'note' AS tweet_type
            FROM read_json_auto({json_files_list}, 
                maximum_object_size=2147483648
            ) j,
            UNNEST(j."note-tweet") AS n(noteTweet)
            WHERE n.noteTweetId IS NOT NULL

            UNION ALL

            -- Community tweets
            SELECT 
                c.tweet.id_str,
                c.tweet.in_reply_to_status_id_str,
                CAST(c.tweet.retweet_count AS BIGINT) AS retweet_count,
                CAST(c.tweet.favorite_count AS BIGINT) AS favorite_count,
                c.tweet.full_text,
                c.tweet.text,
                c.tweet.lang,
                c.tweet.source,
                c.tweet.created_at,
                c.tweet.favorited,
                c.tweet.retweeted,
                c.tweet.possibly_sensitive,
                c.tweet.entities,
                c.tweet.extended_entities,
                'community' AS tweet_type
            FROM read_json_auto({json_files_list}, 
                maximum_object_size=2147483648
            ) j,
            UNNEST(j."community-tweet") AS c(tweet)
            WHERE c.tweet.id_str IS NOT NULL
        )
        SELECT * FROM all_tweets
    ) TO '{parquet_path}' (FORMAT 'parquet');
    """

    logger.info("Starting conversion of JSON to Parquet...")
    try:
        con.execute(query)
        logger.info(f"Conversion successful! Parquet file saved to {parquet_path}")
    except duckdb.Error as e:
        logger.error(f"Error during conversion:\n  {e}")
        logger.error("This likely means there's a structural issue in one of the files.")
        logger.error("Try running the schema generator with --validate to find problematic files.")
        return

if __name__ == '__main__':
    main()