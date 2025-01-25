import pytest
from pathlib import Path
from datetime import datetime, timezone
from ..export import MarkdownExporter
from ..tweet import Tweet
from ..metadata import TweetMetadata
from ..conversation import ConversationThread

@pytest.fixture
def sample_tweet():
    return Tweet(
        id="123",
        text="Test tweet with **markdown** formatting",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        media=[{
            'type': 'photo',
            'media_url': 'http://example.com/image.jpg'
        }],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )

@pytest.fixture
def sample_thread(sample_tweet):
    reply = Tweet(
        id="456",
        text="Test reply",
        created_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        media=[],
        parent_id="123",
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    thread = ConversationThread(root_tweet=sample_tweet)
    thread.add_reply(reply)
    return thread

def test_markdown_export_single_tweet(sample_tweet, tmp_path):
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_export.md"
    
    exporter.export_tweets([sample_tweet], output_path)
    
    assert output_path.exists()
    content = output_path.read_text()
    assert "Test tweet with **markdown** formatting" in content
    assert "2024-01-01 00:00:00" in content
    assert "![photo](http://example.com/image.jpg)" in content

def test_markdown_export_thread(sample_thread, tmp_path):
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_thread.md"
    
    exporter.export_thread(sample_thread, output_path)
    
    assert output_path.exists()
    content = output_path.read_text()
    assert "Thread started on" in content
    assert "Test tweet with **markdown** formatting" in content
    assert "Test reply" in content
    assert content.index("Test tweet") < content.index("Test reply")  # Check order

def test_markdown_export_tweets_chronological_order(tmp_path):
    tweets = [
        Tweet(
            id=str(i),
            text=f"Tweet {i}",
            created_at=datetime(2024, 1, i, tzinfo=timezone.utc),
            media=[],
            parent_id=None,
            metadata=TweetMetadata(
                tweet_type="tweet",
                raw_data={},
                urls=set()
            )
        ) for i in range(1, 4)
    ]
    
    # Add tweets in non-chronological order
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_chronological.md"
    exporter.export_tweets([tweets[1], tweets[2], tweets[0]], output_path)
    
    content = output_path.read_text()
    # Check that tweets appear in chronological order
    assert content.index("Tweet 1") < content.index("Tweet 2") < content.index("Tweet 3")

def test_markdown_export_with_missing_timestamp(sample_tweet, tmp_path):
    tweet_no_time = Tweet(
        id="999",
        text="Tweet with no timestamp",
        created_at=None,
        media=[],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    exporter = MarkdownExporter()
    output_path = tmp_path / "test_no_timestamp.md"
    exporter.export_tweets([tweet_no_time, sample_tweet], output_path)
    
    content = output_path.read_text()
    # Tweet with timestamp should come after tweet without timestamp
    assert content.index("Tweet with no timestamp") < content.index("Test tweet") 