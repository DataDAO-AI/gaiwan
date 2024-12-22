"""Tests for archive processing."""

import pytest
from gaiwan.archive_processor import ArchiveProcessor

def test_process_tweet(sample_archive_data, tmp_path):
    """Test processing of individual tweets."""
    processor = ArchiveProcessor(tmp_path)
    
    # Test regular tweet
    tweet = processor.process_tweet({
        "id_str": "123",
        "created_at": "Wed Mar 13 12:34:56 +0000 2024",
        "full_text": "Regular tweet",
        "entities": {}
    }, "tweet")
    assert tweet.id == "123"
    assert tweet.source_type == "tweet"
    
    # Test community tweet
    tweet = processor.process_tweet({
        "id_str": "456",
        "created_at": "Wed Mar 13 12:34:56 +0000 2024",
        "full_text": "Community tweet",
        "entities": {}
    }, "community_tweet")
    assert tweet.id == "456"
    assert tweet.source_type == "community_tweet"
    
    # Test note tweet
    tweet = processor.process_tweet(sample_archive_data["note-tweet"][0], "note")
    assert tweet.id == "789"
    assert tweet.source_type == "note" 