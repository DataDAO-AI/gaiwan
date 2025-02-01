import pytest
from pathlib import Path
import json
from datetime import datetime, timezone
from ...export.oai import OpenAIExporter
from ...tweets.types import StandardTweet
from ...metadata import TweetMetadata
from ...conversation import ConversationThread

@pytest.fixture
def sample_thread():
    root = StandardTweet(
        id="123",
        text="Hello world!",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        media=[],
        parent_id=None,
        metadata=TweetMetadata(tweet_type="tweet", raw_data={}, urls=set())
    )
    
    reply = StandardTweet(
        id="456",
        text="Hello back!",
        created_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        media=[],
        parent_id="123",
        metadata=TweetMetadata(tweet_type="tweet", raw_data={}, urls=set())
    )
    
    thread = ConversationThread(root_tweet=root)
    thread.add_reply(reply)
    return thread

def test_oai_export(sample_thread, tmp_path):
    exporter = OpenAIExporter()
    output_path = tmp_path / "conversations.jsonl"
    system_message = "You are a helpful assistant"
    
    exporter.export_conversations([sample_thread], output_path, system_message)
    
    assert output_path.exists()
    with open(output_path) as f:
        data = [json.loads(line) for line in f]
    
    assert len(data) == 1
    conversation = data[0]
    assert len(conversation['messages']) == 3
    assert conversation['messages'][0]['role'] == 'system'
    assert conversation['messages'][0]['content'] == system_message
    assert conversation['messages'][1]['content'] == "Hello world!"
    assert conversation['messages'][2]['content'] == "Hello back!" 