# Twitter Archive Tweet Package

This package provides a flexible object-oriented framework for handling different types of tweets from Twitter archives. It includes base classes and specialized implementations for standard tweets and note tweets.

## Features

- Support for multiple tweet types:
  - Standard tweets
  - Note tweets (Twitter Notes)
  - Like tweets
- Consistent interface for all tweet types
- Rich metadata handling
- Media attachment support
- URL, mention, and hashtag extraction

## Installation

```bash
pip install gaiwan-twitter-archive-processor
```

## Quick Start

```python
from gaiwan.twitter_archive_processor.tweets import TweetFactory
from pathlib import Path

# Create a tweet from raw data
tweet_data = {...}  # Raw Twitter API data
tweet = TweetFactory.create_tweet(tweet_data, "tweet")

# Access tweet properties
print(tweet.text)
print(tweet.created_at)
print(tweet.clean_text())
print(tweet.get_urls())
```

## Architecture

### Class Hierarchy
```
BaseTweet (ABC)
├── StandardTweet
└── NoteTweet
```

### Base Class Interface

```python
class BaseTweet:
    def __init__(
        self,
        id: str,
        text: str,
        created_at: Optional[datetime],
        media: List[Dict],
        parent_id: Optional[str],
        metadata: TweetMetadata
    ):
        """
        Initialize a tweet
        Args:
            id (str): Tweet ID
            text (str): Tweet content
            created_at (datetime, optional): Creation timestamp
            media (List[Dict]): Media attachments
            parent_id (str, optional): Parent tweet ID for replies
            metadata (TweetMetadata): Tweet metadata
        """
    
    def clean_text(self) -> str:
        """Remove mentions, URLs, and hashtags from text"""
        
    def get_urls(self) -> Set[str]:
        """Extract URLs from the tweet"""
        
    def get_mentions(self) -> Set[str]:
        """Extract user mentions from the tweet"""
        
    def get_hashtags(self) -> Set[str]:
        """Extract hashtags from the tweet"""
```

## Input Types

### Raw Tweet Data Format
```python
# Standard Tweet
{
    "id_str": "123456789",
    "full_text": "Tweet content",
    "created_at": "Wed Oct 10 20:19:24 +0000 2018",
    "extended_entities": {
        "media": [...]
    },
    "in_reply_to_status_id_str": "123456788",
    "entities": {
        "urls": [...],
        "user_mentions": [...],
        "hashtags": [...]
    }
}

# Note Tweet
{
    "noteTweetId": "123456789",
    "core": {
        "text": "Note content",
        "urls": [...],
        "mentions": [...],
        "hashtags": [...]
    },
    "createdAt": "2018-10-10T20:19:24.000Z"
}
```

## Tweet Types

### StandardTweet
- Regular Twitter posts
- Full metadata support
- Media attachment handling
- Reply threading support

### NoteTweet
- Twitter Notes format
- Extended content support
- Different metadata structure
- Limited media support

## Factory Usage

```python
from gaiwan.twitter_archive_processor.tweets import TweetFactory

# Create a standard tweet
standard_tweet = TweetFactory.create_tweet(data, "tweet")

# Create a note tweet
note_tweet = TweetFactory.create_tweet(data, "note")

# Create a like tweet
like_tweet = TweetFactory.create_tweet(data, "like")
```

## Maintaining This README

This README should be updated when:
1. New tweet types are added
2. Base interface changes
3. Input data format changes
4. Factory methods are modified

Update checklist:
- [ ] Class hierarchy diagram
- [ ] Input format documentation
- [ ] Tweet type descriptions
- [ ] Factory usage examples
- [ ] Interface documentation

## Error Handling

All tweet types handle:
- Missing timestamps
- Missing media data
- Invalid date formats
- Missing metadata fields
- Invalid URLs/mentions/hashtags

## Testing

Run tests with:
```bash
pytest tests/tweets/
```

Test coverage includes:
- Tweet creation
- Text cleaning
- Metadata extraction
- Media handling
- Factory methods
- Error cases

## Dependencies

- Python 3.6+
- Standard library only (datetime, abc, typing)
- Optional: pytest for testing

## License

MIT License - See LICENSE file for details 