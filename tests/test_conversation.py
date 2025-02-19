import pytest
from datetime import datetime, timezone
from pathlib import Path

from ..tweets.types import StandardTweet
from ..metadata import TweetMetadata
from ..conversation import ConversationThread

@pytest.fixture
def sample_tweets():
    """Create sample tweets for testing."""
    root = StandardTweet(
        id="123",
        text="Root tweet",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        media=[],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    reply1 = StandardTweet(
        id="456",
        text="First reply",
        created_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        media=[],
        parent_id="123",
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    reply2 = StandardTweet(
        id="789",
        text="Second reply",
        created_at=datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc),  # Between root and reply1
        media=[],
        parent_id="123",
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    return root, reply1, reply2

def test_conversation_thread_creation(sample_tweets):
    root, _, _ = sample_tweets
    thread = ConversationThread(root_tweet=root)
    
    assert thread.root_tweet == root
    assert len(thread.replies) == 0
    assert thread.created_at == root.created_at
    assert thread.length == 1

def test_adding_replies(sample_tweets):
    root, reply1, reply2 = sample_tweets
    thread = ConversationThread(root_tweet=root)
    
    thread.add_reply(reply1)
    assert len(thread.replies) == 1
    assert thread.length == 2
    
    thread.add_reply(reply2)
    assert len(thread.replies) == 2
    assert thread.length == 3

def test_replies_chronological_order(sample_tweets):
    root, reply1, reply2 = sample_tweets
    thread = ConversationThread(root_tweet=root)
    
    # Add replies in non-chronological order
    thread.add_reply(reply1)  # 01:00
    thread.add_reply(reply2)  # 00:30
    
    # Check that all_tweets returns them in chronological order
    tweets = thread.all_tweets
    assert len(tweets) == 3
    assert tweets[0] == root    # Root first
    assert tweets[1] == reply2  # Earlier reply
    assert tweets[2] == reply1  # Later reply

def test_thread_with_missing_timestamps(sample_tweets):
    root, reply1, _ = sample_tweets
    # Create a reply with no timestamp
    reply_no_time = StandardTweet(
        id="999",
        text="Reply with no timestamp",
        created_at=None,
        media=[],
        parent_id="123",
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    thread = ConversationThread(root_tweet=root)
    thread.add_reply(reply_no_time)
    thread.add_reply(reply1)
    
    # Replies with no timestamp should sort to the beginning
    assert thread.all_tweets[-1] == reply1  # Latest reply last
    assert thread.length == 3 