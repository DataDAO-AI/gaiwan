import json
import pytest
from datetime import datetime, timezone
from pathlib import Path

from ...tweets.types import StandardTweet
from ...metadata import TweetMetadata
from ...conversation import ConversationThread
from ...export.chatml import ChatMLExporter

@pytest.fixture
def sample_tweet():
    return StandardTweet(
        id="123",
        text="Test tweet with media",
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
    reply = StandardTweet(
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

def test_chatml_export_single_tweet(sample_tweet, tmp_path):
    exporter = ChatMLExporter(system_message="Test system message")
    output_path = tmp_path / "test_export.json"
    
    exporter.export_tweets([sample_tweet], output_path)
    
    with open(output_path) as f:
        data = json.load(f)
    
    assert "messages" in data
    messages = data["messages"]
    assert len(messages) == 2  # system message + tweet
    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    assert "Test tweet with media" in messages[1]["content"]
    assert "[photo](http://example.com/image.jpg)" in messages[1]["content"]
    assert "2024-01-01" in messages[1]["content"]

def test_chatml_export_thread(sample_thread, tmp_path):
    exporter = ChatMLExporter()
    output_path = tmp_path / "test_thread.json"
    
    exporter.export_thread(sample_thread, output_path)
    
    with open(output_path) as f:
        data = json.load(f)
    
    messages = data["messages"]
    assert len(messages) == 3  # system + 2 tweets
    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    assert messages[2]["role"] == "assistant"
    assert "Test tweet with media" in messages[1]["content"]
    assert "Test reply" in messages[2]["content"]

def test_chatml_thread_roles(sample_thread, tmp_path):
    exporter = ChatMLExporter(system_message="Test system message")
    output_path = tmp_path / "test_thread.json"
    
    exporter.export_thread(sample_thread, output_path)
    
    with open(output_path) as f:
        data = json.load(f)
    
    messages = data["messages"]
    assert len(messages) == 3  # system + 2 tweets
    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    assert messages[2]["role"] == "assistant"

def test_chatml_json_format(sample_thread, tmp_path):
    exporter = ChatMLExporter()
    output_path = tmp_path / "test_format.json"
    
    exporter.export_thread(sample_thread, output_path)
    
    content = output_path.read_text()
    assert "{\n" in content  # Check for pretty-printing
    assert content.count("\n") > 3  # Should have multiple lines 