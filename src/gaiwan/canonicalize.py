"""Canonicalize Twitter archive data into a unified timeline."""

import argparse
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
import time
import jsonschema
from gaiwan.schema_generator import generate_schema
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class CanonicalTweet:
    """Normalized tweet format that works for all tweet types."""
    
    id: str
    created_at: datetime
    text: str
    entities: dict
    author_username: str
    
    # Engagement metrics
    likers: Set[str] = field(default_factory=set)
    retweet_count: int = 0
    
    # Reply metadata
    in_reply_to_status_id: Optional[str] = None
    in_reply_to_username: Optional[str] = None
    
    # Optional metadata
    quoted_tweet_id: Optional[str] = None

    @classmethod
    def from_any_tweet(cls, data: Dict, username: str) -> Optional['CanonicalTweet']:
        """Create canonical tweet from any tweet type."""
        try:
            # Handle wrapped tweet data
            if 'tweet' in data:
                data = data['tweet']
                # Regular tweet format: "Sat Feb 17 19:24:40 +0000 2024"
                created_at = datetime.strptime(data['created_at'], "%a %b %d %H:%M:%S %z %Y")
                text = data.get('full_text', data.get('text', ''))
                entities = data.get('entities', {})
                tweet_id = data['id_str']
                
            elif 'noteTweet' in data:
                data = data['noteTweet']
                # Note tweet format: ISO format
                created_at = datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
                
                # Note tweets store content in core.text
                if 'core' not in data or 'text' not in data['core']:
                    return None
                    
                text = data['core']['text']
                tweet_id = data['noteTweetId']
                
                # Build entities from note data
                entities = {
                    'hashtags': [
                        {'text': h['text'], 'indices': [int(h['fromIndex']), int(h['toIndex'])]}
                        for h in data['core'].get('hashtags', [])
                    ],
                    'urls': [
                        {
                            'url': u['shortUrl'],
                            'expanded_url': u['expandedUrl'],
                            'display_url': u['displayUrl'],
                            'indices': [int(u['fromIndex']), int(u['toIndex'])]
                        }
                        for u in data['core'].get('urls', [])
                    ],
                    'user_mentions': [
                        {
                            'screen_name': m['screenName'],
                            'indices': [int(m['fromIndex']), int(m['toIndex'])]
                        }
                        for m in data['core'].get('mentions', [])
                    ]
                }
            else:
                return None
                
            if not text:
                return None
                
            return cls(
                id=tweet_id,
                created_at=created_at,
                text=text,
                entities=entities,
                author_username=username,
                retweet_count=int(data.get('retweet_count', 0)),
                in_reply_to_status_id=data.get('in_reply_to_status_id_str'),
                in_reply_to_username=data.get('in_reply_to_screen_name'),
                quoted_tweet_id=data.get('quoted_status_id_str')
            )
            
        except Exception as e:
            logger.error(f"Error converting tweet: {str(e)}", exc_info=True)
            return None

def canonicalize_archive(archive_dir: Path, output_file: Path, schema_file: Optional[Path] = None, validate: bool = False):
    """Process all archives in a directory into a unified timeline."""
    
    # Main data structures
    tweets: Dict[str, CanonicalTweet] = {}
    orphaned_likes: Dict[str, Dict] = {}
    profiles: Dict[str, Dict] = {}
    
    # Stats
    stats = {
        'archives_processed': 0,
        'tweets_imported': 0,
        'profiles_imported': 0,
        'likes_matched': 0,
        'likes_orphaned': 0
    }
    
    archives = list(archive_dir.glob("*_archive.json"))
    logger.info(f"Found {len(archives)} archives to process")
    
    # First pass: Process all tweets and profiles
    logger.info("Pass 1: Processing tweets and profiles...")
    for archive_path in tqdm(archives, desc="Processing tweets"):
        try:
            # Get username by removing '_archive.json' suffix
            username = archive_path.stem[:-8] if archive_path.stem.endswith('_archive') else archive_path.stem
            
            with open(archive_path) as f:
                data = json.load(f)
            
            # Store profile
            if 'profile' in data:
                profiles[username] = data['profile']
                stats['profiles_imported'] += 1
            
            # Process tweets
            archive_tweets = 0
            for section in ['tweets', 'community-tweet', 'note-tweet']:
                if section not in data:
                    continue
                    
                for tweet_data in data[section]:
                    tweet = CanonicalTweet.from_any_tweet(tweet_data, username)
                    if tweet:
                        tweets[tweet.id] = tweet
                        archive_tweets += 1
            
            logger.debug(f"Imported {archive_tweets} tweets from {username}")
            stats['tweets_imported'] += archive_tweets
            stats['archives_processed'] += 1
                        
        except Exception as e:
            logger.error(f"Error processing tweets from {archive_path}: {str(e)}")
            continue
    
    logger.info(f"Imported {stats['tweets_imported']} tweets from {stats['archives_processed']} archives")
    
    # Second pass: Process likes now that we have all tweets
    logger.info("Pass 2: Processing likes...")
    for archive_path in tqdm(archives, desc="Processing likes"):
        try:
            username = archive_path.stem.split('_')[0]
            
            with open(archive_path) as f:
                data = json.load(f)
            
            # Process likes
            archive_matches = 0
            archive_orphans = 0
            
            if 'like' in data:
                for like in data['like']:
                    if 'like' not in like:
                        continue
                    like_data = like['like']
                    tweet_id = like_data.get('tweetId')
                    if not tweet_id:
                        continue
                        
                    if tweet_id in tweets:
                        tweets[tweet_id].likers.add(username)
                        archive_matches += 1
                    else:
                        if tweet_id not in orphaned_likes:
                            orphaned_likes[tweet_id] = {
                                'tweet_id': tweet_id,
                                'text': like_data.get('fullText', ''),
                                'url': like_data.get('expandedUrl'),
                                'likers': set()
                            }
                        orphaned_likes[tweet_id]['likers'].add(username)
                        archive_orphans += 1
                
            logger.debug(f"Processed {archive_matches} matched and {archive_orphans} orphaned likes from {username}")
            stats['likes_matched'] += archive_matches
            stats['likes_orphaned'] += archive_orphans
                
        except Exception as e:
            logger.error(f"Error processing likes from {archive_path}: {str(e)}")
            continue
    
    logger.info(f"Processed {stats['likes_matched']} matched and {stats['likes_orphaned']} orphaned likes")
    
    # Prepare output
    output = {
        "tweets": [asdict(tweet) for tweet in sorted(
            tweets.values(),
            key=lambda t: t.created_at,
            reverse=True
        )],
        "orphaned_likes": list(orphaned_likes.values()),
        "profiles": [{"username": u, "profile": p} for u, p in profiles.items()]
    }
    
    # Write output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    # Generate schema for output format if requested
    if validate:
        output_schema = output_file.with_suffix('.schema.json')
        logger.info(f"Generating schema for canonical format in {output_schema}")
        generate_schema(Path(output_file).parent, output_schema)
        
        # Compare with expected schema if it exists
        expected_schema = Path(__file__).parent / "canonical_schema.json"
        if expected_schema.exists():
            logger.info(f"Validating against expected schema {expected_schema}")
            with open(expected_schema) as f:
                schema = json.load(f)
            try:
                jsonschema.validate(instance=output, schema=schema)
                logger.info("Output matches expected schema")
            except jsonschema.exceptions.ValidationError as e:
                logger.error(f"Output doesn't match expected schema: {e.message}")
        else:
            logger.info(f"Generated new schema at {output_schema} - review and use as canonical if correct")
    
    # Print statistics
    logger.info(f"\nFinal statistics:")
    logger.info(f"  Archives processed: {stats['archives_processed']}")
    logger.info(f"  Tweets imported: {stats['tweets_imported']}")
    logger.info(f"  Profiles imported: {stats['profiles_imported']}")
    logger.info(f"  Likes matched: {stats['likes_matched']}")
    logger.info(f"  Likes orphaned: {stats['likes_orphaned']}")

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Canonicalize Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_file', type=Path, help="Output JSON file")
    parser.add_argument('--schema', type=Path, help="Optional schema file to use")
    parser.add_argument('--validate', action='store_true', 
                       help="Generate and validate schema for output")
    parser.add_argument('--debug', action='store_true', 
                       help="Enable debug logging")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    canonicalize_archive(args.archive_dir, args.output_file, 
                        args.schema, validate=args.validate)

if __name__ == '__main__':
    main() 