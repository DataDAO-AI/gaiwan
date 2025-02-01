import pytest
from pathlib import Path
from ...tweets.types import StandardTweet
from ...metadata import TweetMetadata
from ...export.markdown import MarkdownExporter

def test_markdown_export_single_tweet(sample_tweet, tmp_path):
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_export.md"
    
    exporter.export_tweets([sample_tweet], output_path)
    
    assert output_path.exists()
    content = output_path.read_text()
    assert "Test tweet with media" in content
    assert "2024-01-01 00:00:00" in content
    assert "![photo](http://example.com/image.jpg)" in content

def test_markdown_export_thread(sample_thread, tmp_path):
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_thread.md"
    
    exporter.export_thread(sample_thread, output_path)
    
    content = output_path.read_text()
    assert "Thread started on" in content
    assert "Test tweet with media" in content
    assert "Test reply" in content
    assert content.index("Test tweet") < content.index("Test reply")

def test_markdown_chronological_order(sample_tweet, tmp_path):
    # Create tweet without timestamp
    tweet_no_time = StandardTweet(
        id="789",
        text="Tweet with no timestamp",
        created_at=None,
        media=[],
        parent_id=None,
        metadata=TweetMetadata(tweet_type="tweet", raw_data={}, urls=set())
    )
    
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_chronological.md"
    exporter.export_tweets([tweet_no_time, sample_tweet], output_path)
    
    content = output_path.read_text()
    # Tweet with no timestamp should come before tweet with timestamp
    assert content.index("Tweet with no timestamp") < content.index("Test tweet") 