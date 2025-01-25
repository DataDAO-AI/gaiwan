import pytest
from pathlib import Path
import json
from datetime import datetime
from ..archive import Archive
from ..tweet import Tweet

@pytest.fixture
def sample_archive_data():
    return {
        "account": [{
            "account": {
                "username": "testuser",
                "accountId": "12345",
                "accountDisplayName": "Test User"
            }
        }],
        "tweets": [{
            "tweet": {
                "id_str": "123456789",
                "full_text": "This is a test tweet",
                "created_at": "Wed Feb 28 21:13:12 +0000 2024",
                "in_reply_to_status_id_str": None,
                "extended_entities": {
                    "media": [{
                        "type": "photo",
                        "media_url": "http://example.com/image.jpg"
                    }]
                }
            }
        }],
        "note-tweet": [{
            "noteTweet": {
                "noteTweetId": "987654321",
                "core": {
                    "text": "This is a note tweet",
                }
            }
        }],
        "like": [{
            "like": {
                "tweetId": "11111111",
                "fullText": "This is a liked tweet"
            }
        }]
    }

@pytest.fixture
def sample_archive_file(tmp_path, sample_archive_data):
    archive_file = tmp_path / "testuser_archive.json"
    with open(archive_file, 'w') as f:
        json.dump(sample_archive_data, f)
    return archive_file

def test_archive_initialization(sample_archive_file):
    archive = Archive(sample_archive_file)
    assert archive.archive_path == sample_archive_file
    assert archive.username is None
    assert len(archive.tweets) == 0
    assert isinstance(archive.metadata, dict)

def test_archive_loading(sample_archive_file):
    archive = Archive(sample_archive_file)
    archive.load()
    
    assert archive.username == "testuser"
    assert len(archive.tweets) == 3  # One regular tweet, one note, one like
    assert archive.metadata['account']['username'] == "testuser"

def test_tweet_creation(sample_archive_file, sample_archive_data):
    archive = Archive(sample_archive_file)
    tweet_data = sample_archive_data['tweets'][0]['tweet']
    
    tweet = archive._create_tweet(tweet_data, 'tweet')
    
    assert isinstance(tweet, Tweet)
    assert tweet.id == "123456789"
    assert tweet.text == "This is a test tweet"
    assert isinstance(tweet.created_at, datetime)
    assert len(tweet.media) == 1
    assert tweet.metadata.tweet_type == 'tweet'

def test_note_tweet_processing(sample_archive_file):
    archive = Archive(sample_archive_file)
    archive.load()
    
    note_tweets = [t for t in archive.tweets if t.metadata.tweet_type == 'note']
    assert len(note_tweets) == 1
    assert note_tweets[0].id == "987654321"

def test_like_processing(sample_archive_file):
    archive = Archive(sample_archive_file)
    archive.load()
    
    likes = [t for t in archive.tweets if t.metadata.tweet_type == 'like']
    assert len(likes) == 1
    assert likes[0].id == "11111111"

def test_invalid_archive_handling():
    with pytest.raises(Exception):
        archive = Archive(Path("nonexistent_file.json"))
        archive.load() 