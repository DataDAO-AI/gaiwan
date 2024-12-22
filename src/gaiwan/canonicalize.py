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
    
    # Process each archive
    for archive_path in archive_dir.glob("*_archive.json"):
        try:
            username = archive_path.stem.split('_')[0]
            logger.info(f"Processing archive for {username}")
            
            with open(archive_path) as f:
                data = json.load(f)
            
            # Store profile
            if 'profile' in data:
                profiles[username] = data['profile']
            
            # Process tweets
            for section in ['tweets', 'community-tweet', 'note-tweet']:
                if section not in data:
                    continue
                    
                for tweet_data in data[section]:
                    tweet = CanonicalTweet.from_any_tweet(tweet_data, username)
                    if tweet:
                        tweets[tweet.id] = tweet
            
            # Process likes
            if 'like' in data:
                for like in data['like']:
                    if 'like' not in like:
                        continue
                    like_data = like['like']
                    tweet_id = like_data.get('tweetId') or like_data.get('tWeetId')
                    if not tweet_id:
                        continue
                        
                    if tweet_id in tweets:
                        tweets[tweet_id].likers.add(username)
                    else:
                        if tweet_id not in orphaned_likes:
                            orphaned_likes[tweet_id] = {
                                'tweet_id': tweet_id,
                                'text': like_data.get('fullText', ''),
                                'url': like_data.get('expandedUrl'),
                                'likers': set()
                            }
                        orphaned_likes[tweet_id]['likers'].add(username)
                
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {str(e)}")
            continue
    
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

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Canonicalize Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_file', type=Path, help="Output JSON file")
    parser.add_argument('--schema', type=Path, help="Optional schema file to use")
    args = parser.parse_args()
    
    canonicalize_archive(args.archive_dir, args.output_file, args.schema)

if __name__ == '__main__':
    main() 