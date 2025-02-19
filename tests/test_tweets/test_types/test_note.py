"""Tests for NoteTweet handling."""
import pytest
from datetime import datetime
from gaiwan.twitter_archive_processor.tweets import NoteTweet

@pytest.fixture
def sample_note_data():
    return {
        "noteTweetId": "123456789",
        "core": {
            "text": "Test note content",
            "urls": [],
            "mentions": [],
            "hashtags": []
        },
        "createdAt": "2024-01-12T05:37:33.000Z"
    }

def test_note_tweet_creation(sample_note_data):
    """Test NoteTweet creation from data."""
    note = NoteTweet.from_data(sample_note_data)
    assert note.id == "123456789"
    assert note.text == "Test note content"
    assert isinstance(note.created_at, datetime)

def test_note_tweet_clean_text():
    """Test text cleaning for notes."""
    note = NoteTweet(
        id="123",
        text="@user Check this https://t.co/abc #test",
        created_at=datetime.now(),
        media=[],
        parent_id=None,
        metadata=None
    )
    assert note.clean_text() == "Check this" 