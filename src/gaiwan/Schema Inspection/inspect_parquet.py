"""Inspect the schema and contents of the converted Parquet file."""

import argparse
from pathlib import Path
import duckdb
from datetime import datetime
import json

def inspect_parquet(parquet_path: Path):
    """Show schema and basic stats of the Parquet file."""
    con = duckdb.connect(':memory:')
    
    # Get schema
    print("\nSchema:")
    print("-------")
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()
    for column in schema:
        print(f"{column[0]:<20} {column[1]}")
    
    # Get basic stats
    print("\nBasic Stats:")
    print("-----------")
    stats = con.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT id) as unique_tweets,
            COUNT(DISTINCT user_id) as unique_users,
            MIN(created_at) as earliest_tweet,
            MAX(created_at) as latest_tweet,
            COUNT(DISTINCT tweet_type) as tweet_types,
            STRING_AGG(DISTINCT tweet_type, ', ') as types
        FROM read_parquet('{parquet_path}')
    """).fetchone()
    
    print(f"Total Rows:      {stats[0]:,}")
    print(f"Unique Tweets:   {stats[1]:,}")
    print(f"Unique Users:    {stats[2]:,}")
    print(f"Date Range:      {stats[3]} to {stats[4]}")
    print(f"Tweet Types:     {stats[5]} ({stats[6]})")
    
    # Enhanced field statistics
    print("\nField Statistics:")
    print("----------------")
    field_stats = con.execute(f"""
        SELECT 
            COUNT(DISTINCT lang) as unique_langs,
            COUNT(DISTINCT source) as unique_sources,
            SUM(CASE WHEN in_reply_to_status_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as reply_percentage,
            SUM(CASE WHEN in_reply_to_user_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as reply_user_percentage,
            AVG(LENGTH(full_text)) as avg_text_length,
            MAX(LENGTH(full_text)) as max_text_length,
            SUM(CASE WHEN urls IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as tweets_with_urls,
            SUM(CASE WHEN media IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as tweets_with_media
        FROM read_parquet('{parquet_path}')
    """).fetchone()
    
    print(f"Unique Languages: {field_stats[0]}")
    print(f"Unique Sources:   {field_stats[1]}")
    print(f"Reply Tweet %:    {field_stats[2]:.1f}%")
    print(f"Reply User %:     {field_stats[3]:.1f}%")
    print(f"Avg Text Length: {field_stats[4]:.1f}")
    print(f"Max Text Length: {field_stats[5]}")
    print(f"With URLs:       {field_stats[6]:.1f}%")
    print(f"With Media:      {field_stats[7]:.1f}%")

    # Entity statistics
    print("\nEntity Statistics:")
    print("-----------------")
    entity_stats = con.execute(f"""
        SELECT 
            AVG(CASE WHEN urls IS NOT NULL THEN json_array_length(urls) ELSE 0 END) as avg_urls,
            MAX(CASE WHEN urls IS NOT NULL THEN json_array_length(urls) ELSE 0 END) as max_urls,
            AVG(CASE WHEN media IS NOT NULL THEN json_array_length(media) ELSE 0 END) as avg_media,
            MAX(CASE WHEN media IS NOT NULL THEN json_array_length(media) ELSE 0 END) as max_media,
            AVG(CASE WHEN hashtags IS NOT NULL THEN json_array_length(hashtags) ELSE 0 END) as avg_hashtags,
            MAX(CASE WHEN hashtags IS NOT NULL THEN json_array_length(hashtags) ELSE 0 END) as max_hashtags,
            AVG(CASE WHEN user_mentions IS NOT NULL THEN json_array_length(user_mentions) ELSE 0 END) as avg_mentions,
            MAX(CASE WHEN user_mentions IS NOT NULL THEN json_array_length(user_mentions) ELSE 0 END) as max_mentions
        FROM read_parquet('{parquet_path}')
    """).fetchone()
    
    print(f"URLs per tweet:     {entity_stats[0]:.2f} avg, {entity_stats[1]} max")
    print(f"Media per tweet:    {entity_stats[2]:.2f} avg, {entity_stats[3]} max")
    print(f"Hashtags per tweet: {entity_stats[4]:.2f} avg, {entity_stats[5]} max")
    print(f"Mentions per tweet: {entity_stats[6]:.2f} avg, {entity_stats[7]} max")

    # Sample tweets from each type
    print("\nSample Tweets by Type:")
    print("--------------------")
    for tweet_type in ['tweet', 'community', 'note', 'like']:
        print(f"\n{tweet_type.upper()} TWEETS:")
        print("-" * (len(tweet_type) + 7))
        samples = con.execute(f"""
            SELECT 
                id,
                user_id,
                user_screen_name,
                created_at,
                full_text,
                source,
                lang,
                retweet_count,
                favorite_count,
                in_reply_to_status_id,
                in_reply_to_user_id,
                COALESCE(json_array_length(urls), 0) as url_count,
                COALESCE(json_array_length(media), 0) as media_count,
                COALESCE(json_array_length(hashtags), 0) as hashtag_count,
                COALESCE(json_array_length(user_mentions), 0) as mention_count,
                urls,
                media
            FROM read_parquet('{parquet_path}')
            WHERE tweet_type = ?
            ORDER BY random()
            LIMIT 3
        """, [tweet_type]).fetchall()
        
        for i, sample in enumerate(samples, 1):
            print(f"\nSample {i}:")
            print(f"ID:        {sample[0]}")
            print(f"User:      @{sample[2]} ({sample[1]})")
            print(f"Date:      {sample[3]}")
            print(f"Source:    {sample[5]}")
            print(f"Language:  {sample[6]}")
            print(f"Retweets:  {sample[7]}")
            print(f"Favorites: {sample[8]}")
            if sample[9]:
                print(f"Reply to:   {sample[9]} (user: {sample[10]})")
            print(f"Entities:  {sample[11]} URLs, {sample[12]} media, {sample[13]} hashtags, {sample[14]} mentions")
            if sample[15]:  # Show first URL if present
                urls = json.loads(sample[15])
                if urls:
                    print(f"First URL: {urls[0].get('expanded_url', 'N/A')}")
            if sample[16]:  # Show first media if present
                media = json.loads(sample[16])
                if media:
                    print(f"First Media: {media[0].get('type', 'N/A')} - {media[0].get('media_url_https', 'N/A')}")
            print("Text:")
            print(sample[4])

    # Tweet Type Breakdown with user stats
    print("\nTweet Type Breakdown:")
    print("-------------------")
    type_stats = con.execute(f"""
        SELECT 
            tweet_type,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage,
            COUNT(DISTINCT user_id) as unique_users,
            MIN(created_at) as earliest,
            MAX(created_at) as latest
        FROM read_parquet('{parquet_path}')
        GROUP BY tweet_type
        ORDER BY count DESC
    """).fetchall()
    
    for type_stat in type_stats:
        print(f"\n{type_stat[0]}:")
        print(f"  Count:        {type_stat[1]:>10,} ({type_stat[2]:>6.2f}%)")
        print(f"  Unique Users: {type_stat[3]:>10,}")
        print(f"  Date Range:   {type_stat[4]} to {type_stat[5]}")

def main():
    parser = argparse.ArgumentParser(description="Inspect Parquet file schema and contents")
    parser.add_argument('parquet_file', type=Path, help="Path to Parquet file")
    args = parser.parse_args()
    
    if not args.parquet_file.exists():
        print(f"Error: File not found: {args.parquet_file}")
        return
        
    inspect_parquet(args.parquet_file)

if __name__ == '__main__':
    main() 