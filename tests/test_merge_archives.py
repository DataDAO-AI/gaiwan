"""Tests for archive merging functionality."""

import json
from pathlib import Path
from datetime import datetime, timezone

import pytest

from gaiwan.community_archiver import ArchiveProcessor

@pytest.fixture
def processor(tmp_path):
    """Create ArchiveProcessor with temp directory."""
    return ArchiveProcessor(tmp_path)

@pytest.fixture
def old_archive():
    """Sample old archive data."""
    return {
        "tweets": [
            {"id_str": "1", "text": "old tweet 1"},
            {"id_str": "2", "text": "old tweet 2"}
        ],
        "community-tweet": [
            {"tweet": {"id_str": "3", "text": "old community tweet"}}
        ],
        "note-tweet": [
            {"noteTweet": {"noteTweetId": "4", "text": "old note"}}
        ],
        "like": [
            {"like": {"tweetId": "5", "fullText": "old like"}}
        ],
        "follower": [
            {"follower": {"accountId": "6", "userLink": "old_follower"}}
        ],
        "following": [
            {"following": {"accountId": "7", "userLink": "old_following"}}
        ],
        "profile": {
            "description": {"bio": "old bio"}
        }
    }

@pytest.fixture
def new_archive():
    """Sample new archive data with updates and additions."""
    return {
        "tweets": [
            {"id_str": "1", "text": "updated tweet 1"},  # Updated
            {"id_str": "8", "text": "new tweet"}  # New
        ],
        "community-tweet": [
            {"tweet": {"id_str": "9", "text": "new community tweet"}}  # New
        ],
        "note-tweet": [
            {"noteTweet": {"noteTweetId": "4", "text": "updated note"}},  # Updated
            {"noteTweet": {"noteTweetId": "10", "text": "new note"}}  # New
        ],
        "like": [
            {"like": {"tweetId": "11", "fullText": "new like"}}  # New
        ],
        "follower": [
            {"follower": {"accountId": "12", "userLink": "new_follower"}}  # New
        ],
        "following": [
            {"following": {"accountId": "13", "userLink": "new_following"}}  # New
        ],
        "profile": {
            "description": {"bio": "new bio"}  # Updated
        }
    }

def test_merge_tweets(processor, old_archive, new_archive):
    """Test merging of regular tweets."""
    merged = processor.merge_archives(old_archive, new_archive)
    
    # Should have 3 tweets: updated tweet 1, old tweet 2, new tweet 8
    assert len(merged['tweets']) == 3
    tweet_ids = {t['id_str'] for t in merged['tweets']}
    assert tweet_ids == {'1', '2', '8'}
    
    # Check updated content
    updated_tweet = next(t for t in merged['tweets'] if t['id_str'] == '1')
    assert updated_tweet['text'] == 'updated tweet 1'

def test_merge_community_tweets(processor, old_archive, new_archive):
    """Test merging of community tweets."""
    merged = processor.merge_archives(old_archive, new_archive)
    
    # Should preserve old and add new
    assert len(merged['community-tweet']) == 2
    tweet_ids = {t['tweet']['id_str'] for t in merged['community-tweet']}
    assert tweet_ids == {'3', '9'}

def test_merge_note_tweets(processor, old_archive, new_archive):
    """Test merging of note tweets."""
    merged = processor.merge_archives(old_archive, new_archive)
    
    # Should have updated note 4 and new note 10
    assert len(merged['note-tweet']) == 2
    note_ids = {n['noteTweet']['noteTweetId'] for n in merged['note-tweet']}
    assert note_ids == {'4', '10'}
    
    # Check updated content
    updated_note = next(n for n in merged['note-tweet'] 
                       if n['noteTweet']['noteTweetId'] == '4')
    assert updated_note['noteTweet']['text'] == 'updated note'

def test_merge_social_data(processor, old_archive, new_archive):
    """Test merging of likes, followers, and following."""
    merged = processor.merge_archives(old_archive, new_archive)
    
    # Likes should combine (no duplicates)
    like_ids = {l['like']['tweetId'] for l in merged['like']}
    assert like_ids == {'5', '11'}
    
    # Followers should combine
    follower_ids = {f['follower']['accountId'] for f in merged['follower']}
    assert follower_ids == {'6', '12'}
    
    # Following should combine
    following_ids = {f['following']['accountId'] for f in merged['following']}
    assert following_ids == {'7', '13'}

def test_merge_profile(processor, old_archive, new_archive):
    """Test profile updates."""
    merged = processor.merge_archives(old_archive, new_archive)
    
    # Should use new profile
    assert merged['profile']['description']['bio'] == 'new bio'

def test_merge_partial_data(processor, old_archive):
    """Test merging when new archive has partial data."""
    partial_new = {
        "tweets": [{"id_str": "8", "text": "new tweet"}],
        # No community-tweet, note-tweet, etc.
    }
    
    merged = processor.merge_archives(old_archive, partial_new)
    
    # Should preserve old data for missing sections
    assert len(merged['community-tweet']) == len(old_archive['community-tweet'])
    assert len(merged['note-tweet']) == len(old_archive['note-tweet'])
    assert len(merged['like']) == len(old_archive['like'])
    
    # Should merge available data
    assert len(merged['tweets']) == len(old_archive['tweets']) + 1

def test_merge_empty_archives(processor):
    """Test merging with empty archives."""
    empty_old = {}
    empty_new = {}
    
    merged = processor.merge_archives(empty_old, empty_new)
    assert merged == {}
    
    # Test merging empty new into non-empty old
    merged = processor.merge_archives({"tweets": []}, empty_new)
    assert "tweets" in merged
    assert merged["tweets"] == [] 