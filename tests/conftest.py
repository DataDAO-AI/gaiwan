"""Common test fixtures and configuration."""

import json
from datetime import datetime
from pathlib import Path
import pytest
from gaiwan.models import CanonicalTweet, TweetMetadata, MixPRConfig
from gaiwan.user_similarity import UserSimilarityConfig
import logging

logger = logging.getLogger(__name__)

@pytest.fixture
def sample_tweets():
    """Create sample tweets for testing."""
    metadata = TweetMetadata(
        mentioned_users={'user2'},
        hashtags=set(),
        urls=set(),
        quoted_tweet_id=None,
        is_retweet=False,
        retweet_of_id=None,
        media=[]
    )

    return [
        CanonicalTweet(
            id='1',
            text='Hello world! @user2 check this out',
            author_id='user1',
            created_at=datetime(2024, 1, 1, 12, 0),
            metadata=metadata,
            reply_to_tweet_id=None,
            liked_by=set(),
            source_type='tweet'
        )
    ]

@pytest.fixture
def sample_archive_path(tmp_path) -> Path:
    """Create a sample archive for testing."""
    archive_dir = tmp_path / "sample_archive"
    archive_dir.mkdir()
    
    # Create tweet.js with proper structure
    tweet_data = [  # Direct list of tweet objects
        {
            "tweet": {
                "id_str": "1",
                "full_text": "Test tweet",
                "created_at": "Fri Sep 27 16:17:03 +0000 2024",
                "user": {
                    "screen_name": "test_user",
                    "id_str": "123"
                },
                "entities": {
                    "user_mentions": [],
                    "hashtags": [],
                    "urls": [],
                    "media": []
                }
            }
        }
    ]
    
    tweet_js = archive_dir / "tweet.js"
    tweet_js.write_text("window.YTD.tweet.part0 = " + json.dumps(tweet_data))
    logger.debug(f"Writing test data: {tweet_data}")
    
    return archive_dir

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
    """Create test configuration for user similarity."""
    return UserSimilarityConfig(
        min_tweets_per_user=2,
        min_likes_per_user=1,
        mention_weight=0.7,
        reply_weight=0.8,
        quote_weight=0.6,
        ncd_threshold=0.7,
        like_weight=0.5,
        retweet_weight=0.4,
        conversation_weight=0.3,
        community_weight=0.6,
        media_weight=0.4,
        url_weight=0.5
    ) 