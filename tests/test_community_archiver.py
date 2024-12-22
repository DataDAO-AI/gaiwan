"""Tests for community archiver."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gaiwan.community_archiver import ArchiveProcessor, CanonicalTweet

@pytest.fixture
def sample_archive_data():
    """Create sample archive data for testing."""
    return {
        "tweet": [
            {
                "tweet": {
                    "id_str": "123",
                    "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                    "full_text": "Regular tweet",
                    "favorite_count": "5",
                    "retweet_count": "2",
                    "entities": {
                        "hashtags": [],
                        "user_mentions": [],
                        "urls": [],
                        "media": []
                    }
                }
            }
        ]
    }

def test_process_tweet(sample_archive_data, tmp_path):
    """Test processing of individual tweets."""
    processor = ArchiveProcessor(tmp_path)
    
    # Test regular tweet
    tweet = processor.process_tweet({
        "tweet": {
            "id_str": "123",
            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
            "full_text": "Regular tweet",
            "entities": {}
        }
    }, "tweet")
    
    assert tweet.id == "123"
    assert tweet.text == "Regular tweet"
    assert isinstance(tweet.created_at, datetime)