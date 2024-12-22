"""Common test fixtures and configuration."""

import json
from datetime import datetime, timezone
from pathlib import Path
import pytest
from gaiwan.models import CanonicalTweet
from gaiwan.config import MixPRConfig
from gaiwan.models import UserSimilarityConfig

@pytest.fixture
def sample_tweets():
    """Create sample tweets for testing."""
    return [
        CanonicalTweet(
            id="1",
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            text="Hello world!",
            entities={
                "hashtags": [],
                "user_mentions": [{"screen_name": "user2"}],
                "urls": [],
                "media": []
            },
            screen_name="user1",
            favorite_count=5,
            retweet_count=2,
            possibly_sensitive=False,
            source_type="tweet"
        )
    ]

@pytest.fixture
def sample_archive_data():
    """Create sample archive data for testing."""
    return {
        "tweets": [{
            "tweet": {
                "id_str": "123",
                "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                "full_text": "Regular tweet",
                "favorite_count": "5",
                "retweet_count": "2",
                "entities": {}
            }
        }],
        "community-tweet": [{
            "tweet": {
                "id_str": "456",
                "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                "full_text": "Community tweet",
                "entities": {}
            }
        }],
        "note-tweet": [{
            "noteTweet": {
                "noteTweetId": "789",
                "createdAt": "2024-03-13T12:34:56.000Z",
                "text": "Note tweet",
                "entities": {}
            }
        }]
    }

@pytest.fixture
def mixpr_config():
    """Create MixPR configuration for testing."""
    return MixPRConfig(
        local_alpha=0.6,
        similarity_threshold=0.2,
        max_iterations=10
    )

@pytest.fixture
def user_similarity_config():
    """Create test configuration for user similarity."""
    return UserSimilarityConfig(
        min_tweets_per_user=2,
        min_likes_per_user=1,
        mention_weight=0.7,
        reply_weight=0.8,
        quote_weight=0.6
    )

def pytest_configure(config):
    """Register custom marks."""
    config.addinivalue_line(
        "markers", "slow: mark test as slow (deselect with '-m \"not slow\"')"
    )