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
    tmp_path: Path
):
    """Test full archive processing pipeline."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Initialize components
    processor = ArchiveProcessor(output_dir)
    stats_manager = StatsManager(output_dir)
    
    # Process archive
    tweets = processor.process_archive(sample_archive_path)
    
    # Generate stats
    stats_manager.process_archive(sample_archive_path, tweets)
    
    # Verify outputs
    stats_file = output_dir / "stats" / f"{sample_archive_path.stem}_stats.json"
    assert stats_file.exists()
    
    with open(stats_file) as f:
        stats_data = json.load(f)
    
    # Basic assertions
    assert len(tweets) > 0
    assert stats_data["tweet_counts"]["total"] == "2"
    assert stats_data["tweet_counts"]["replies"] == "1"
    assert stats_data["tweet_counts"]["retweets"] == "1"

def test_mixpr_retrieval(tmp_path: Path):
    """Test MixPR retrieval system."""
    # Create test tweets
    tweets = [
        CanonicalTweet(
            id="1",
            text="Original tweet",
            author_id="user1",
            created_at=datetime.now(timezone.utc)
        ),
        CanonicalTweet(
            id="2",
            text="Reply to original",
            author_id="user2",
            created_at=datetime.now(timezone.utc),
            reply_to_tweet_id="1"
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