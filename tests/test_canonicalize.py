"""Tests for tweet canonicalization."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gaiwan.canonicalize import CanonicalTweet, canonicalize_archive

@pytest.fixture
def sample_tweet_data():
    """Create sample tweet data for testing."""
    return {
        "tweet": {
            "id_str": "123",
            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
            "full_text": "Regular tweet",
            "entities": {},
            "retweet_count": "0",
            "favorite_count": "0"
        }
    }

def test_canonical_tweet(sample_tweet_data):
    """Test tweet canonicalization."""
    tweet = CanonicalTweet.from_any_tweet(sample_tweet_data, "tweet")
    
    assert tweet is not None
    assert tweet.id == "123"
    assert tweet.text == "Regular tweet"
    assert tweet.source_type == "tweet"
    
    # Test community tweet
    community_tweet = CanonicalTweet.from_any_tweet({
        "tweet": {
            "id_str": "456",
            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
            "text": "Community tweet",
            "entities": {}
        }
    }, "community")
    
    assert community_tweet is not None
    assert community_tweet.id == "456"
    assert community_tweet.text == "Community tweet"
    assert community_tweet.source_type == "community"
    
    # Test note tweet
    note_tweet = CanonicalTweet.from_any_tweet({
        "noteTweet": {
            "noteTweetId": "789",
            "createdAt": "2024-03-13T12:34:56Z",
            "core": {
                "text": "Note tweet",
                "entities": {}
            }
        }
    }, "note")
    
    assert note_tweet is not None
    assert note_tweet.id == "789"
    assert note_tweet.text == "Note tweet"
    assert note_tweet.source_type == "note"

def test_canonicalize_archive(tmp_path):
    """Test archive canonicalization."""
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    # Create test archives
    archives = {
        "user1": {
            "tweets": [
                {
                    "tweet": {
                        "id_str": "1",
                        "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                        "text": "User 1 tweet",
                        "entities": {},
                        "retweet_count": "0",
                        "favorite_count": "0"
                    }
                }
            ],
            "profile": {"screen_name": "user1"}
        },
        "user2": {
            "tweets": [
                {
                    "tweet": {
                        "id_str": "2",
                        "created_at": "Wed Mar 13 12:34:57 +0000 2024",
                        "text": "User 2 tweet",
                        "entities": {},
                        "retweet_count": "0",
                        "favorite_count": "0"
                    }
                }
            ],
            "profile": {"screen_name": "user2"}
        }
    }
    
    for username, data in archives.items():
        with open(archive_dir / f"{username}_archive.json", "w") as f:
            json.dump(data, f)
    
    output_file = tmp_path / "timeline.json"
    canonicalize_archive(archive_dir, output_file)
    
    assert output_file.exists()
    with open(output_file) as f:
        timeline = json.load(f)
        assert "tweets" in timeline
        assert "profiles" in timeline
        assert len(timeline["tweets"]) == 2
        assert len(timeline["profiles"]) == 2