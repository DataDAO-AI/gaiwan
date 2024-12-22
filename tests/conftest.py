"""Test fixtures and configuration."""

import pytest
from datetime import datetime, timezone
from pathlib import Path

from gaiwan.community_archiver import CanonicalTweet
from gaiwan.mixpr import MixPRConfig
from gaiwan.user_similarity import UserSimilarityConfig

@pytest.fixture
def sample_tweets():
    """Create sample tweets for testing."""
    return [
        CanonicalTweet(
            id="123",
            text="Hello world! @user2 check this out",
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            entities={
                "user_mentions": [{"screen_name": "user2"}],
                "urls": [{"expanded_url": "https://example.com"}]
            },
            screen_name="user1"
        ),
        CanonicalTweet(
            id="456",
            text="@user1 Thanks for sharing!",
            created_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            entities={
                "user_mentions": [{"screen_name": "user1"}]
            },
            screen_name="user2",
            in_reply_to_status_id="123"
        )
    ]

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
    """Create user similarity configuration for testing."""
    return UserSimilarityConfig(
        ncd_weight=0.5,
        interaction_weight=0.5,
        min_tweets=1
    )

def pytest_configure(config):
    """Register custom marks."""
    config.addinivalue_line(
        "markers", "slow: mark test as slow (deselect with '-m \"not slow\"')"
    )