"""Tests for Archive class."""
import pytest
from pathlib import Path
from gaiwan.twitter_archive_processor.core import Archive

def test_archive_initialization():
    """Test basic archive initialization."""
    path = Path("test.json")
    archive = Archive(path)
    assert archive.file_path == path
    assert not archive.tweets

def test_archive_load_invalid_json():
    """Test handling of invalid JSON."""
    with pytest.raises(ValueError):
        Archive(Path("invalid.json")).load()

def test_archive_thread_reconstruction():
    """Test conversation thread reconstruction."""
    archive = Archive(Path("test.json"))
    # Add test data
    threads = archive.get_conversation_threads()
    assert len(threads) > 0
    assert all(t.root_tweet for t in threads) 