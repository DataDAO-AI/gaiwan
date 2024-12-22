"""Integration tests for the full Gaiwan system."""

import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gaiwan.archive_processor import ArchiveProcessor
from gaiwan.conversation_analyzer import ConversationAnalyzer
from gaiwan.models import CanonicalTweet, MixPRConfig
from gaiwan.mixpr import MixPR
from gaiwan.stats_collector import StatsManager
from gaiwan.user_similarity import UserSimilarityGraph, UserSimilarityConfig

logger = logging.getLogger(__name__)

@pytest.fixture
def sample_archive_path(tmp_path) -> Path:
    """Create a sample archive for testing."""
    archive_dir = tmp_path / "sample_archive"
    archive_dir.mkdir()
    
    # Create tweet.js
    tweets = {
        "tweet": [
            {
                "id_str": "123456789",
                "created_at": "Wed Oct 10 20:19:24 +0000 2018",
                "full_text": "Hello world! #testing @user1",
                "in_reply_to_status_id_str": None,
            },
            {
                "id_str": "987654321",
                "created_at": "Wed Oct 10 20:20:24 +0000 2018",
                "full_text": "RT @user2: A retweet test",
                "in_reply_to_status_id_str": "123456789",
            }
        ]
    }
    
    with open(archive_dir / "tweet.js", "w") as f:
        f.write("window.YTD.tweet.part0 = ")
        json.dump(tweets, f)
    
    return archive_dir

def test_end_to_end_processing(
    sample_archive_path: Path,
    tmp_path: Path,
    caplog  # Add pytest caplog fixture
):
    """Test full archive processing pipeline."""
    caplog.set_level(logging.DEBUG)
    
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Create tweet.js file with test data
    tweet_js = sample_archive_path / "tweet.js"
    tweet_js.parent.mkdir(parents=True, exist_ok=True)
    tweet_data = {
        "tweet": [
            {
                "id_str": "123456789",
                "created_at": "Wed Oct 10 20:19:24 +0000 2018",
                "full_text": "Hello world! #testing @user1",
                "entities": {},
                "user": {"screen_name": "testuser"}
            }
        ]
    }
    tweet_js.write_text("window.YTD.tweet.part0 = " + json.dumps(tweet_data))
    
    # Process archive
    processor = ArchiveProcessor(output_dir)
    tweets = processor.process_archive(sample_archive_path)
    
    # Basic assertions
    assert len(tweets) > 0

def test_mixpr_retrieval(tmp_path: Path):
    """Test MixPR retrieval system."""
    tweets = [
        CanonicalTweet(
            id="1",
            text="Original tweet",
            created_at=datetime.now(timezone.utc),
            entities={},  # Required by schema
            screen_name="user1"
        ),
        CanonicalTweet(
            id="2",
            text="Reply to original",
            created_at=datetime.now(timezone.utc),
            entities={},
            screen_name="user2",
            in_reply_to_status_id="1"  # Use schema field name
        )
    ]
    
    # Initialize and fit MixPR
    config = MixPRConfig()
    mixpr = MixPR(config)
    mixpr.fit(tweets)
    
    # Test retrieval
    results = mixpr.retrieve(tweets[1], k=1)
    assert len(results) == 1
    assert results[0].tweet.id == "1"  # Should retrieve parent tweet

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

def test_tweet_type_processing(tmp_path):
    """Test processing of different tweet types."""
    # Write test archive
    archive_path = tmp_path / "tweet.js"
    with open(archive_path, 'w') as f:
        f.write('window.YTD.tweet.part0 = ')
        json.dump({
            "tweet": [
                {
                    "tweet": {
                        "id_str": "123",
                        "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                        "full_text": "Regular tweet",
                        "entities": {}
                    }
                }
            ]
        }, f)
    
    # Process archive
    processor = ArchiveProcessor(tmp_path)
    tweets = processor.process_archive(tmp_path)
    
    # Verify normalization
    assert len(tweets) > 0
    for tweet in tweets:
        assert isinstance(tweet, CanonicalTweet)
        assert tweet.id is not None