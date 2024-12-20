"""Tests for data models."""

import pytest
from datetime import datetime, timezone
from gaiwan.models import TweetMetadata, CanonicalTweet

def test_tweet_metadata_extraction():
    """Test metadata extraction from tweet text."""
    text = "@user1 Hello! Check out #python https://example.com RT @user2: original"
    metadata = TweetMetadata.extract_from_text(text)
    
    assert "user1" in metadata.mentioned_users
    assert "python" in metadata.hashtags
    assert "https://example.com" in metadata.urls
    assert metadata.is_retweet
    assert metadata.retweet_of_id == "user2"

def test_canonical_tweet_creation():
    """Test CanonicalTweet creation from raw data."""
    tweet_data = {
        "id_str": "123456",
        "created_at": "Wed Mar 13 12:34:56 +0000 2024",
        "full_text": "Hello @world #test",
        "in_reply_to_status_id_str": None
    }
    
    tweet = CanonicalTweet.from_tweet_data(tweet_data, "user1")
    
    assert tweet.id == "123456"
    assert tweet.author_id == "user1"
    assert tweet.text == "Hello @world #test"
    assert "world" in tweet.metadata.mentioned_users
    assert "test" in tweet.metadata.hashtags

def test_tweet_serialization():
    """Test tweet serialization to dictionary."""
    tweet = CanonicalTweet(
        id="123",
        text="Test tweet",
        author_id="user1",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc)
    )
    
    data = tweet.to_dict()
    
    assert data["id"] == "123"
    assert data["text"] == "Test tweet"
    assert data["author_id"] == "user1"
    assert "created_at" in data 