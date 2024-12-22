"""Tests for data models."""

import pytest
from datetime import datetime, timezone
from gaiwan.models import CanonicalTweet, parse_twitter_timestamp

def test_canonical_tweet_normalization():
    """Test normalization of different tweet types."""
    # Test regular tweet
    regular_tweet = {
        "tweet": {
            "id_str": "123456",
            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
            "full_text": "Hello world!",
            "entities": {
                "hashtags": [],
                "user_mentions": []
            },
            "favorite_count": "5",
            "retweet_count": "2",
            "possibly_sensitive": False,
            "lang": "en"
        }
    }
    
    # Test both old and new methods
    tweet1 = CanonicalTweet.from_tweet_data(regular_tweet, "tweet")
    tweet2 = CanonicalTweet.from_dict(regular_tweet, "tweet")

    assert tweet1.id == "123456"
    assert tweet2.id == "123456"
    assert tweet1.favorite_count == 5
    assert tweet2.favorite_count == 5
    
    # Test community tweet
    community_tweet = {
        "tweet": {
            "id_str": "345678",
            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
            "full_text": "Community post",
            "entities": {},
            "possibly_sensitive": "false",
            "community_id": "789"
        }
    }
    
    community = CanonicalTweet.from_tweet_data(community_tweet, "community_tweet")
    assert community.id == "345678"
    assert community.source_type == "community_tweet"

def test_timestamp_normalization():
    """Test normalization of different timestamp formats."""
    timestamps = [
        "Wed Mar 13 12:34:56 +0000 2024",
        "2024-03-13T12:34:56.000Z",
        "2024-03-13T12:34:56Z"
    ]
    
    for ts in timestamps:
        dt = parse_twitter_timestamp(ts)
        assert isinstance(dt, datetime)
        assert dt.tzinfo is not None
        assert dt.year == 2024
        assert dt.month == 3
        assert dt.day == 13 