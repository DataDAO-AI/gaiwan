"""Common test fixtures and configuration."""

import pytest
from datetime import datetime, timezone
from gaiwan.models import CanonicalTweet, TweetMetadata, MixPRConfig
from gaiwan.user_similarity import UserSimilarityConfig

@pytest.fixture
def sample_tweets():
    """Create a set of interconnected sample tweets for testing."""
    tweets = [
        CanonicalTweet(
            id="1",
            text="Hello world! @user2 check this out",
            author_id="user1",
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            metadata=TweetMetadata.extract_from_text("Hello world! @user2 check this out")
        ),
        CanonicalTweet(
            id="2",
            text="@user1 Nice tweet! #testing",
            author_id="user2",
            created_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
            reply_to_tweet_id="1",
            metadata=TweetMetadata.extract_from_text("@user1 Nice tweet! #testing")
        ),
        CanonicalTweet(
            id="3",
            text="Interesting discussion https://example.com",
            author_id="user3",
            created_at=datetime(2024, 1, 1, 2, tzinfo=timezone.utc),
            metadata=TweetMetadata.extract_from_text("Interesting discussion https://example.com")
        )
    ]
    return tweets

@pytest.fixture
def mixpr_config():
    """Create a MixPR configuration for testing."""
    return MixPRConfig(
        local_alpha=0.6,
        similarity_threshold=0.2,
        max_iterations=10,
        min_df=1,
        max_df=0.95,
        batch_size=100,
        graph_weight=0.3,
        reply_weight=1.0,
        quote_weight=0.8,
        user_similarity_weight=0.4
    )

@pytest.fixture
def user_similarity_config():
    """Create a UserSimilarity configuration for testing."""
    return UserSimilarityConfig(
        min_tweets_per_user=2,
        min_likes_per_user=1,
        mention_weight=0.7,
        reply_weight=0.8,
        quote_weight=0.6,
        ncd_threshold=0.7
    ) 