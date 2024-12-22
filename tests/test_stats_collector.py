"""Tests for statistics collection."""

import pytest
from pathlib import Path
from gaiwan.community_archiver import CanonicalTweet
from gaiwan.stats_collector import StatsManager

def test_archive_stats_initialization():
    """Test ArchiveStats initialization with default values."""
    stats = ArchiveStats()
    assert stats.total_tweets == 0
    assert stats.total_replies == 0
    assert stats.total_likes == 0
    assert len(stats.mentioned_users) == 0

def test_update_from_tweet(sample_tweets):
    """Test updating stats from a tweet."""
    stats = ArchiveStats()
    
    for tweet in sample_tweets:
        stats.update_from_tweet(tweet)
    
    assert stats.total_tweets == len(sample_tweets)
    assert stats.first_tweet_date is not None
    assert stats.last_tweet_date is not None
    assert len(stats.tweet_lengths) == len(sample_tweets)

def test_generate_summary(sample_tweets):
    """Test generation of statistics summary."""
    stats = ArchiveStats()
    
    for tweet in sample_tweets:
        stats.update_from_tweet(tweet)
    
    summary = stats.generate_summary()
    
    assert "tweet_counts" in summary
    assert "activity_period" in summary
    assert "content_metrics" in summary
    assert "engagement_metrics" in summary
    assert "temporal_patterns" in summary

@pytest.fixture
def temp_stats_dir(tmp_path):
    """Create temporary directory for stats files."""
    return tmp_path / "stats"

def test_stats_manager(temp_stats_dir, sample_tweets):
    """Test StatsManager functionality."""
    manager = StatsManager(temp_stats_dir)
    
    # Process sample archive
    archive_path = Path("test_archive")
    manager.process_archive(archive_path, sample_tweets)
    
    # Check if stats file was created
    stats_file = temp_stats_dir / "stats" / "test_archive_stats.json"
    assert stats_file.exists()
    
    # Test aggregate stats
    aggregate_stats = manager.generate_aggregate_stats()
    assert isinstance(aggregate_stats, dict) 