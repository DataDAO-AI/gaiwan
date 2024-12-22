"""End-to-end test using visakanv's Twitter archive."""

import pytest
from pathlib import Path
import logging
from datetime import datetime, timezone
import json
import os
from functools import partial
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from gaiwan.archive_processor import ArchiveProcessor, download_archive
from gaiwan.stats_collector import StatsManager
from gaiwan.mixpr import MixPR
from gaiwan.models import MixPRConfig

logger = logging.getLogger(__name__)

def timeout(seconds):
    def decorator(func):
        @pytest.mark.timeout(seconds)
        def wrapper(*args, **kwargs):
            with ThreadPoolExecutor() as executor:
                future = executor.submit(func, *args, **kwargs)
                return future.result(timeout=seconds)
        return wrapper
    return decorator

@pytest.mark.slow
@pytest.mark.timeout(60)  # Use pytest's timeout
def test_visakanv_archive(tmp_path: Path, caplog):
    """Test full pipeline with visakanv's archive."""
    caplog.set_level(logging.INFO)
    
    # Constants for testing
    MAX_TWEETS = 1000  # Limit tweets for testing
    TIMEOUT = 60  # Seconds
    
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    # Download archive
    logger.info("Downloading visakanv archive...")
    archive_path = download_archive("visakanv", archive_dir)
    assert archive_path is not None
    assert archive_path.exists()
    
    # Process archive
    logger.info("Processing tweets...")
    processor = ArchiveProcessor(output_dir)
    tweets = processor.process_archive(archive_path)[:MAX_TWEETS]  # Limit tweets
    
    # Basic tweet assertions
    assert len(tweets) > 0
    logger.info(f"Processing sample of {len(tweets)} tweets")
    
    # Verify tweet date range
    dates = [t.created_at for t in tweets if t.created_at]
    assert min(dates) < datetime(2020, 1, 1, tzinfo=timezone.utc)  # Has old tweets
    assert max(dates) > datetime(2022, 1, 1, tzinfo=timezone.utc)  # Has recent tweets
    
    # Generate stats
    logger.info("Generating statistics...")
    stats_manager = StatsManager(output_dir)
    stats_manager.process_archive(archive_path, tweets)
    
    # Verify stats file exists
    stats_file = output_dir / "stats" / f"{archive_path.stem}_stats.json"
    assert stats_file.exists()
    
    # Load and verify stats
    with open(stats_file) as f:
        stats = json.load(f)
    
    # Known facts about visakanv's Twitter usage (adjusted for sample size)
    assert "visakanv" in stats, "Stats should contain visakanv's data"
    user_stats = stats["visakanv"]
    assert int(user_stats["tweet_count"]) > 0  # Has tweets
    assert len(user_stats["hashtags"]) > 0  # Uses hashtags
    assert len(user_stats["mentioned_users"]) > 0  # Has mentions
    
    # Test MixPR retrieval with sample
    logger.info("Testing MixPR retrieval...")
    config = MixPRConfig(
        local_alpha=0.6,
        similarity_threshold=0.2,
        max_iterations=10
    )
    mixpr = MixPR(config)
    mixpr.fit(tweets)
    
    # Find a question tweet
    question_tweets = [t for t in tweets if '?' in t.text]
    assert len(question_tweets) > 0
    
    # Test retrieval
    results = mixpr.retrieve(question_tweets[0], k=5)
    assert len(results) > 0
    
    # Log sample stats
    logger.info(f"Sample size: {len(tweets)} tweets")
    logger.info(f"Sample date range: {min(dates)} to {max(dates)}")
    logger.info(f"Sample unique mentioned users: {len(user_stats['mentioned_users'])}")
    logger.info(f"Sample hashtags: {list(user_stats['hashtags'])[:5]}") 