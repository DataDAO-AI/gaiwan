"""Canonicalize Twitter archive data into a unified timeline."""

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, ClassVar
import time

logger = logging.getLogger(__name__)

@dataclass
class CanonicalTweet:
    """Normalized tweet format that works for all tweet types."""
    
    # Add class-level constants for required fields
    REQUIRED_FIELDS: ClassVar[Set[str]] = {
        'id_str', 'created_at', 'text'
    }
    
    id: str
    created_at: datetime
    text: str
    entities: dict
    
    # Optional fields with defaults
    possibly_sensitive: bool = False
    favorited: bool = False
    retweeted: bool = False
    retweet_count: Optional[int] = None
    favorite_count: Optional[int] = None
    in_reply_to_status_id: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None
    in_reply_to_screen_name: Optional[str] = None
    screen_name: Optional[str] = None
    source_type: str = "tweet"
    quoted_tweet_id: Optional[str] = None
    community_id: Optional[str] = None

    @classmethod
    def from_any_tweet(cls, data: Dict, source_type: str = "tweet", username: Optional[str] = None) -> Optional['CanonicalTweet']:
        """Create canonical tweet from any tweet type."""
        try:
            if source_type == "note":
                return cls._from_note_tweet(data, username)
            else:
                if 'tweet' not in data:
                    logger.error(f"Missing tweet wrapper in {source_type} tweet: {data}")
                    return None
                return cls._from_regular_tweet(data['tweet'], source_type)
        except Exception as e:
            logger.error(f"Error converting {source_type} tweet: {str(e)}", exc_info=True)
            return None

    @classmethod
    def _from_regular_tweet(cls, data: Dict, source_type: str) -> Optional['CanonicalTweet']:
        """Convert regular or community tweet."""
        try:
            # Check required fields
            if not all(k in data for k in ['id_str', 'created_at']):
                logger.error(f"Missing required fields in {source_type} tweet: {data}")
                return None
            
            # Get text from either full_text or text field
            text = data.get('full_text', data.get('text', ''))
            if not text:
                logger.error(f"Missing text in {source_type} tweet: {data}")
                return None
            
            created_at = parse_twitter_timestamp(data['created_at'])
            if not created_at:
                logger.error(f"Could not parse timestamp in {source_type} tweet: {data}")
                return None
            
            return cls(
                id=data['id_str'],
                created_at=created_at,
                text=text,
                entities=data.get('entities', {}),
                possibly_sensitive=data.get('possibly_sensitive'),
                favorited=data.get('favorited'),
                retweeted=data.get('retweeted'),
                retweet_count=int(data['retweet_count']) if 'retweet_count' in data else None,
                favorite_count=int(data['favorite_count']) if 'favorite_count' in data else None,
                in_reply_to_status_id=data.get('in_reply_to_status_id_str'),
                in_reply_to_user_id=data.get('in_reply_to_user_id_str'),
                in_reply_to_screen_name=data.get('in_reply_to_screen_name'),
                screen_name=data.get('user', {}).get('screen_name'),
                source_type=source_type,
                quoted_tweet_id=data.get('quoted_status_id_str')
            )
        except Exception as e:
            logger.error(f"Error in _from_regular_tweet: {str(e)}", exc_info=True)
            return None

    @classmethod
    def _from_note_tweet(cls, data: Dict, username: Optional[str] = None) -> Optional['CanonicalTweet']:
        """Convert note tweet."""
        try:
            note = data.get('noteTweet', {})
            core = note.get('core', {})
            
            if not all([note.get('noteTweetId'), note.get('createdAt'), core.get('text')]):
                logger.error(f"Missing required fields in note tweet: {data}")
                return None
            
            created_at = parse_twitter_timestamp(note['createdAt'])
            if not created_at:
                logger.error(f"Could not parse timestamp in note tweet: {data}")
                return None
            
            return cls(
                id=note['noteTweetId'],
                created_at=created_at,
                text=core['text'],
                entities=core.get('entities', {}),
                screen_name=username or note.get('screenName'),
                source_type='note'
            )
        except Exception as e:
            logger.error(f"Error in _from_note_tweet: {str(e)}", exc_info=True)
            return None

def canonicalize_archive(archive_dir: Path, output_file: Path):
    """Process all archives in a directory into a unified timeline."""
    timeline = {
        "tweets": [],
        "profiles": [],
        "likes": [],
        "social": {
            "followers": [],
            "following": []
        }
    }
    
    for archive_path in archive_dir.glob("*_archive.json"):
        try:
            username = archive_path.stem.split('_')[0]
            logger.info(f"Processing archive for {username}")
            
            with open(archive_path) as f:
                data = json.load(f)
            
            # Process tweets
            for section, source_type in [
                ('tweets', 'tweet'),
                ('community-tweet', 'community'),
                ('note-tweet', 'note')
            ]:
                if section in data:
                    for tweet_data in data[section]:
                        tweet = CanonicalTweet.from_any_tweet(tweet_data, source_type, username)
                        if tweet:
                            timeline["tweets"].append(tweet.__dict__)
            
            # Add profile if present
            if 'profile' in data:
                timeline["profiles"].append({
                    "username": username,
                    "profile": data['profile']
                })
            
            # Add likes
            if 'like' in data:
                timeline["likes"].extend(data['like'])
            
            # Add social graph
            if 'follower' in data:
                timeline["social"]["followers"].extend(data['follower'])
            if 'following' in data:
                timeline["social"]["following"].extend(data['following'])
                
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {str(e)}", exc_info=True)
            continue
    
    # Sort tweets by date
    timeline["tweets"].sort(key=lambda t: t["created_at"], reverse=True)
    
    # Write output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(timeline, f, indent=2, default=str)

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Canonicalize Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_file', type=Path, help="Output JSON file")
    args = parser.parse_args()
    
    canonicalize_archive(args.archive_dir, args.output_file)

def parse_twitter_timestamp(ts: str) -> Optional[datetime]:
    """Convert Twitter's timestamp format to datetime.
    
    Handles both formats:
    - ISO format: "2024-03-13T12:34:56Z"
    - Twitter format: "Wed Mar 13 12:34:56 +0000 2024"
    """
    try:
        # Try ISO format first (for note tweets)
        if 'T' in ts:
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
            
        # Try Twitter format (for regular tweets)
        time_struct = time.strptime(ts, "%a %b %d %H:%M:%S +0000 %Y")
        return datetime.fromtimestamp(time.mktime(time_struct)).replace(tzinfo=timezone.utc)
        
    except ValueError as e:
        logger.warning(f"Could not parse timestamp: {ts} - {str(e)}")
        return None

if __name__ == '__main__':
    main() 