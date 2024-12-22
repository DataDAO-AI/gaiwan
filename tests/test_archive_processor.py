"""Tests for archive processing."""

import pytest
from gaiwan.archive_processor import ArchiveProcessor

@pytest.fixture
def sample_archive_data():
    """Create sample archive data for testing."""
    return {
        "note-tweet": [{
            "noteTweet": {
                "noteTweetId": "789",
                "createdAt": "2024-03-13T12:34:56.000Z",
                "core": {
                    "text": "Note tweet",
                    "entities": {}
                }
            }
        }],
        "community-tweet": [{
            "tweet": {
                "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                "entities": {},
                "full_text": "Community tweet",
                "favorite_count": "5",
                "id_str": "456"
            }
        }]
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
    
    # Test community tweet
    community_tweet = processor.process_tweet({
        "tweet": {
            "id_str": "456",
            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
            "full_text": "Community tweet",
            "entities": {},
            "community_id": "789"
        }
    }, "community_tweet")
    assert community_tweet.id == "456"
    assert community_tweet.source_type == "community_tweet"
    
    # Test note tweet
    note_tweet = processor.process_tweet(sample_archive_data["note-tweet"][0], "note")
    assert note_tweet.id == "789"
    assert note_tweet.source_type == "note"
    assert note_tweet.text == "Note tweet" 