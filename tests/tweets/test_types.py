import pytest
from datetime import datetime, timezone
from gaiwan.twitter_archive_processor.tweets.types import StandardTweet, NoteTweet
from gaiwan.twitter_archive_processor.core.metadata import TweetMetadata

@pytest.fixture
def standard_tweet_data():
    return {
        'entities': {
            'urls': [
                {'expanded_url': 'https://example.com'},
                {'expanded_url': 'https://test.com'}
            ],
            'user_mentions': [
                {'screen_name': 'user1'},
                {'screen_name': 'user2'}
            ],
            'hashtags': [
                {'text': 'python'},
                {'text': 'coding'}
            ]
        }
    }

@pytest.fixture
def note_tweet_data():
    return {
        'core': {
            'urls': [
                {'expanded_url': 'https://example.com'}
            ],
            'mentions': [
                {'screenName': 'user1'}
            ],
            'hashtags': ['python', 'coding']
        }
    }

def test_standard_tweet_clean_text():
    tweet = StandardTweet(
        id="123",
        text="Hello @user1 check https://example.com #python",
        created_at=datetime.now(timezone.utc),
        media=[],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data={},
            urls=set()
        )
    )
    
    cleaned = tweet.clean_text()
    assert "@user1" not in cleaned
    assert "https://example.com" not in cleaned
    assert "#python" not in cleaned
    assert "Hello check" in cleaned

def test_standard_tweet_entities(standard_tweet_data):
    tweet = StandardTweet(
        id="123",
        text="Test tweet",
        created_at=datetime.now(timezone.utc),
        media=[],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="tweet",
            raw_data=standard_tweet_data,
            urls=set()
        )
    )
    
    urls = tweet.get_urls()
    assert len(urls) == 2
    assert "https://example.com" in urls
    assert "https://test.com" in urls
    
    mentions = tweet.get_mentions()
    assert len(mentions) == 2
    assert "user1" in mentions
    assert "user2" in mentions
    
    hashtags = tweet.get_hashtags()
    assert len(hashtags) == 2
    assert "python" in hashtags
    assert "coding" in hashtags

def test_note_tweet_entities(note_tweet_data):
    tweet = NoteTweet(
        id="123",
        text="Test note",
        created_at=datetime.now(timezone.utc),
        media=[],
        parent_id=None,
        metadata=TweetMetadata(
            tweet_type="note",
            raw_data=note_tweet_data,
            urls=set()
        )
    )
    
    urls = tweet.get_urls()
    assert len(urls) == 1
    assert "https://example.com" in urls
    
    mentions = tweet.get_mentions()
    assert len(mentions) == 1
    assert "user1" in mentions
    
    hashtags = tweet.get_hashtags()
    assert len(hashtags) == 2
    assert "python" in hashtags
    assert "coding" in hashtags 