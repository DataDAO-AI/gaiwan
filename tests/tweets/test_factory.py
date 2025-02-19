import pytest
from datetime import datetime, timezone
from gaiwan.twitter_archive_processor.tweets.factory import TweetFactory
from gaiwan.twitter_archive_processor.tweets.types import StandardTweet, NoteTweet

def test_create_standard_tweet():
    tweet_data = {
        'id_str': '123',
        'full_text': 'Test tweet',
        'created_at': 'Wed Feb 28 21:13:12 +0000 2024',
        'in_reply_to_status_id_str': None,
        'extended_entities': {
            'media': [{
                'type': 'photo',
                'media_url': 'http://example.com/image.jpg'
            }]
        }
    }
    
    tweet = TweetFactory.create_tweet(tweet_data, 'tweet')
    assert isinstance(tweet, StandardTweet)
    assert tweet.id == '123'
    assert tweet.text == 'Test tweet'
    assert isinstance(tweet.created_at, datetime)
    assert len(tweet.media) == 1

def test_create_note_tweet():
    tweet_data = {
        'noteTweetId': '456',
        'core': {
            'text': 'Test note'
        },
        'createdAt': 'Wed Feb 28 21:13:12 +0000 2024'
    }
    
    tweet = TweetFactory.create_tweet(tweet_data, 'note')
    assert isinstance(tweet, NoteTweet)
    assert tweet.id == '456'
    assert tweet.text == 'Test note'

def test_invalid_tweet_type():
    with pytest.raises(ValueError):
        TweetFactory.create_tweet({}, 'invalid_type')

def test_missing_tweet_id():
    tweet_data = {
        'full_text': 'Test tweet'
    }
    
    tweet = TweetFactory.create_tweet(tweet_data, 'tweet')
    assert tweet is None 