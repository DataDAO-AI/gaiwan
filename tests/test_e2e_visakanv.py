"""End-to-end test using visakanv's Twitter archive."""

import logging
from datetime import datetime, timezone
from pathlib import Path
import json

import pytest

from gaiwan.community_archiver import download_archive
from gaiwan.canonicalize import canonicalize_archive

logger = logging.getLogger(__name__)

@pytest.mark.slow
@pytest.mark.timeout(60)
def test_visakanv_archive(tmp_path: Path, caplog):
    """Test full pipeline with visakanv's archive."""
    caplog.set_level(logging.INFO)
    
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    output_file = tmp_path / "timeline.json"
    
    # Download archive
    logger.info("Downloading visakanv archive...")
    archive_path, metadata = download_archive("visakanv", archive_dir)
    assert archive_path is not None
    assert metadata is not None
    assert archive_path.exists()
    
    # Canonicalize archive
    logger.info("Canonicalizing archive...")
    canonicalize_archive(archive_dir, output_file)
    assert output_file.exists()
    
    # Verify output
    with open(output_file) as f:
        timeline = json.load(f)
        assert "tweets" in timeline
        assert "profiles" in timeline
        assert len(timeline["tweets"]) > 0