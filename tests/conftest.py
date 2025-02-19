import pytest
from pathlib import Path
import json
from datetime import datetime, timezone
from ..tweets.types import StandardTweet
from ..metadata import TweetMetadata
from ..conversation import ConversationThread

@pytest.fixture
def sample_archive_data():
    return {
        "account": [{
            "account": {
                "username": "testuser",
                "accountId": "12345",
                "createdAt": "2020-01-01T00:00:00.000Z"
            }
        }],
        "tweets": [{
            "tweet": {
                "id_str": "123456789",
                "full_text": "This is a test tweet",
                "created_at": "Wed Oct 10 20:19:24 +0000 2018",
                "entities": {},
                "extended_entities": {
                    "media": [{
                        "type": "photo",
                        "media_url": "http://example.com/image.jpg"
                    }]
                }
            }
        }],
        "note-tweet": [],
        "like": []
    }

@pytest.fixture
def sample_archives(tmp_path):
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    # Create two sample archives
    archives = {
        "user1": {
            "tweets": [{"tweet": {"id_str": "1", "full_text": "Test 1"}}],
            "account": [{"account": {"username": "user1"}}]
        },
        "user2": {
            "tweets": [{"tweet": {"id_str": "2", "full_text": "Test 2"}}],
            "account": [{"account": {"username": "user2"}}]
        }
    }
    
    for username, data in archives.items():
        with open(archive_dir / f"{username}_archive.json", 'w') as f:
            json.dump(data, f)
    
    return archive_dir

@pytest.fixture
def sample_tweet():
    return StandardTweet(
        id="123",
        text="Test tweet with media",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        media=[{
            'type': 'photo',
            'media_url': 'http://example.com/image.jpg'
        }],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )

@pytest.fixture
def sample_thread(sample_tweet):
    reply = StandardTweet(
        id="456",
        text="Test reply",
        created_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        media=[],
        parent_id="123",
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    thread = ConversationThread(root_tweet=sample_tweet)
    thread.add_reply(reply)
    return thread 