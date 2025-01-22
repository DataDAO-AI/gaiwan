"""Convert Twitter archive JSON files to Parquet using DuckDB."""

import argparse
import json
import logging
from pathlib import Path
from typing import Iterator

import duckdb

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

CHUNK_SIZE = 100000

def create_tweets_table(con):
    """Create the table to store all tweets."""
    create_staging_query = """
    CREATE TABLE staging_tweets (
        id BIGINT,
        user_id BIGINT,
        user_screen_name VARCHAR,
        user_name VARCHAR,
        in_reply_to_status_id BIGINT,
        in_reply_to_user_id BIGINT,
        retweet_count BIGINT,
        favorite_count BIGINT,
        full_text VARCHAR,
        lang VARCHAR,
        source VARCHAR,
        created_at TIMESTAMP,
        favorited BOOLEAN,
        retweeted BOOLEAN,
        possibly_sensitive BOOLEAN DEFAULT NULL,
        urls JSON,
        media JSON,
        hashtags JSON,
        user_mentions JSON,
        tweet_type VARCHAR
    )
    """
    con.execute(create_staging_query)
    con.execute("CREATE TABLE all_tweets AS SELECT * FROM staging_tweets WHERE 1=0")
    logger.info("Created tables 'staging_tweets' and 'all_tweets'")

def chunk_tweets(tweets: list) -> Iterator[list]:
    """Yield chunks of tweets."""
    for i in range(0, len(tweets), CHUNK_SIZE):
        yield tweets[i:i + CHUNK_SIZE]

def process_tweet_chunk(con, chunk: list, tweet_type: str, user_info: dict):
    """Process a chunk of tweets."""
    con.execute("DROP TABLE IF EXISTS tweet_chunk")
    con.execute("""
        CREATE TEMPORARY TABLE tweet_chunk (
            tweet JSON
        )
    """)

    for tweet in chunk:
        if tweet_type in ('tweet', 'community'):
            tweet_data = tweet.get('tweet', {})
        else:
            tweet_data = tweet.get('noteTweet', {})
        con.execute("INSERT INTO tweet_chunk VALUES (?)", [json.dumps(tweet_data)])
    
    if tweet_type == 'note':
        query = """
            INSERT INTO staging_tweets
            SELECT 
                CAST(tweet->>'noteTweetId' AS BIGINT),
                CAST(? AS BIGINT) as user_id,
                ? as user_screen_name,
                ? as user_name,
                NULL AS in_reply_to_status_id,
                NULL AS in_reply_to_user_id,
                NULL AS retweet_count,
                NULL AS favorite_count,
                COALESCE(tweet->'core'->>'text', '') AS full_text,
                'und' AS lang,
                'Note Tweet' AS source,
                STRPTIME(tweet->>'createdAt', '%Y-%m-%dT%H:%M:%S.000Z'),
                FALSE AS favorited,
                FALSE AS retweeted,
                NULL AS possibly_sensitive,
                tweet->'core'->'urls' AS urls,
                NULL AS media,
                tweet->'core'->'hashtags' AS hashtags,
                tweet->'core'->'mentions' AS user_mentions,
                'note' AS tweet_type
            FROM tweet_chunk
            WHERE tweet->>'noteTweetId' IS NOT NULL
        """
        con.execute(query, [user_info['id_str'], user_info['screen_name'], user_info['name']])
    else:
        query = """
            INSERT INTO staging_tweets
            SELECT 
                CAST(tweet->>'id_str' AS BIGINT),
                CAST(? AS BIGINT) as user_id,
                ? as user_screen_name,
                ? as user_name,
                CAST(NULLIF(tweet->>'in_reply_to_status_id_str', '') AS BIGINT),
                CAST(NULLIF(tweet->>'in_reply_to_user_id_str', '') AS BIGINT),
                CAST(NULLIF(tweet->>'retweet_count', '') AS BIGINT),
                CAST(NULLIF(tweet->>'favorite_count', '') AS BIGINT),
                COALESCE(tweet->>'full_text', tweet->>'text', '') AS full_text,
                COALESCE(tweet->>'lang', '') AS lang,
                COALESCE(tweet->>'source', '') AS source,
                STRPTIME(tweet->>'created_at', '%a %b %d %H:%M:%S +0000 %Y'),
                COALESCE(CAST(tweet->>'favorited' AS BOOLEAN), FALSE),
                COALESCE(CAST(tweet->>'retweeted' AS BOOLEAN), FALSE),
                CASE 
                    WHEN tweet->>'possibly_sensitive' IS NOT NULL 
                    THEN CAST(tweet->>'possibly_sensitive' AS BOOLEAN)
                    ELSE NULL 
                END,
                tweet->'entities'->'urls',
                tweet->'extended_entities'->'media',
                tweet->'entities'->'hashtags',
                tweet->'entities'->'user_mentions',
                ? AS tweet_type
            FROM tweet_chunk
            WHERE tweet->>'id_str' IS NOT NULL
        """
        con.execute(query, [user_info['id_str'], user_info['screen_name'], user_info['name'], tweet_type])

def snowflake_to_timestamp_ms(tweet_id: str) -> int:
    """Convert Twitter snowflake ID to milliseconds since epoch."""
    return (int(tweet_id) >> 22) + 1288834974657

def process_file(con, json_file: Path):
    """Process a single JSON file, handling all tweet types."""
    logger.info(f"Processing {json_file}...")
    try:
        with open(json_file, encoding='utf-8') as f:
            data = json.load(f)
        
        # Account data is in data['account'][0]['account']
        if not isinstance(data, dict) or 'account' not in data:
            logger.warning(f"No account data found in {json_file}")
            return
            
        account = data['account'][0]['account']
        user_info = {
            'id_str': account['accountId'],
            'screen_name': account['username'],
            'name': account['accountDisplayName']
        }
        
        if data.get('tweets'):
            logger.info(f"Processing {len(data['tweets'])} regular tweets...")
            for chunk in chunk_tweets(data['tweets']):
                process_tweet_chunk(con, chunk, 'tweet', user_info)
        
        if data.get('community-tweet'):
            logger.info(f"Processing {len(data['community-tweet'])} community tweets...")
            for chunk in chunk_tweets(data['community-tweet']):
                process_tweet_chunk(con, chunk, 'community', user_info)
        
        if data.get('note-tweet'):
            logger.info(f"Processing {len(data['note-tweet'])} note tweets...")
            for chunk in chunk_tweets(data['note-tweet']):
                process_tweet_chunk(con, chunk, 'note', user_info)

        if data.get('like'):
            logger.info(f"Processing {len(data['like'])} liked tweets...")
            for chunk in chunk_tweets(data['like']):
                process_like_chunk(con, chunk, user_info)

    except Exception as e:
        logger.error(f"Error processing {json_file}: {e}")

def process_like_chunk(con, chunk: list, user_info: dict):
    """Process a chunk of liked tweets."""
    con.execute("DROP TABLE IF EXISTS tweet_chunk")
    con.execute("""
        CREATE TEMPORARY TABLE tweet_chunk (
            tweet JSON,
            timestamp_ms BIGINT
        )
    """)

    for tweet in chunk:
        tweet_data = tweet.get('like', {})
        tweet_id = tweet_data.get('tweetId')
        timestamp_ms = snowflake_to_timestamp_ms(tweet_id) if tweet_id else None
        con.execute(
            "INSERT INTO tweet_chunk VALUES (?, ?)", 
            [json.dumps(tweet_data), timestamp_ms]
        )
    
    query = """
        INSERT INTO staging_tweets
        SELECT 
            CAST(tweet->>'tweetId' AS BIGINT),
            CAST(? AS BIGINT) as user_id,
            ? as user_screen_name,
            ? as user_name,
            NULL AS in_reply_to_status_id,
            NULL AS in_reply_to_user_id,
            NULL AS retweet_count,
            NULL AS favorite_count,
            COALESCE(tweet->>'fullText', '') AS full_text,
            NULL AS lang,
            'Like' AS source,
            DATE_TRUNC('second', TO_TIMESTAMP(timestamp_ms / 1000)) as created_at,
            TRUE AS favorited,
            FALSE AS retweeted,
            NULL AS possibly_sensitive,
            NULL AS urls,
            NULL AS media,
            NULL AS hashtags,
            NULL AS user_mentions,
            'like' AS tweet_type
        FROM tweet_chunk
        WHERE tweet->>'tweetId' IS NOT NULL
    """
    
    con.execute(query, [
        user_info['id_str'],
        user_info['screen_name'],
        user_info['name']
    ])

def main():
    parser = argparse.ArgumentParser(description="Convert Twitter archive JSON to Parquet.")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archive JSON files")
    parser.add_argument('output_dir', type=Path, help="Directory to save Parquet files")
    parser.add_argument('--sample', type=float, help="Sample percentage (0-100) of tweets to process")
    args = parser.parse_args()

    if args.sample and not (0 < args.sample <= 100):
        logger.error("Sample percentage must be between 0 and 100")
        return

    if not args.archive_dir.is_dir():
        logger.error(f"Archive directory does not exist: {args.archive_dir}")
        return
    if not args.output_dir.exists():
        args.output_dir.mkdir(parents=True)

    json_files = list(args.archive_dir.glob('*.json'))
    if not json_files:
        logger.error("No archive JSON files found.")
        return

    # Sample json files if requested
    if args.sample:
        import random
        sample_size = max(1, int(len(json_files) * args.sample / 100))
        json_files = random.sample(json_files, sample_size)
        logger.info(f"Sampling {sample_size} files ({args.sample}% of total)")

    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='8GB'")
    create_tweets_table(con)

    for json_file in json_files:
        process_file(con, json_file)

    parquet_path = args.output_dir / "archive_converted.parquet"
    if args.sample:
        parquet_path = args.output_dir / f"archive_converted_{int(args.sample)}pct.parquet"
    
    logger.info("Exporting to Parquet...")
    try:
        con.execute(f"""
            COPY (
                SELECT * FROM staging_tweets
                ORDER BY created_at
            ) TO '{parquet_path}' (
                FORMAT 'parquet',
                COMPRESSION 'ZSTD'
            )
        """)
        logger.info(f"Successfully exported to {parquet_path}")
    except duckdb.Error as e:
        logger.error(f"Error exporting to Parquet: {e}")

if __name__ == '__main__':
    main()