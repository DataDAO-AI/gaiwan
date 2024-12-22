"""End-to-end test using visakanv's Twitter archive."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from gaiwan.community_archiver import ArchiveProcessor, download_archive

logger = logging.getLogger(__name__)

@pytest.mark.slow
@pytest.mark.timeout(60)
def test_visakanv_archive(tmp_path: Path, caplog):
    """Test full pipeline with visakanv's archive."""
    caplog.set_level(logging.INFO)
    
    # Constants for testing
    MAX_TWEETS = 1000  # Limit tweets for testing
    
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    # Download archive
    logger.info("Downloading visakanv archive...")
    archive_path, metadata = download_archive("visakanv", archive_dir)  # Unpack tuple
    assert archive_path is not None
    assert metadata is not None  # Verify we got metadata
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
    
    # Verify tweet content
    for tweet in tweets:
        assert tweet.id is not None
        assert tweet.text is not None
        assert tweet.created_at is not None
        assert isinstance(tweet.entities, dict)
    
    # Log sample stats
    logger.info(f"Sample size: {len(tweets)} tweets")
    logger.info(f"Sample date range: {min(dates)} to {max(dates)}")