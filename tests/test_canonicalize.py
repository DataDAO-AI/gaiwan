"""Tests for tweet canonicalization using live archive data."""

import json
import logging
from pathlib import Path
import pytest

from gaiwan.canonicalize import canonicalize_archive
from gaiwan.community_archiver import download_archives

# Test with accounts we know are in the archive
TEST_ACCOUNTS = [
    "visakanv",        # Has lots of tweets and likes
    "eigenrobot",      # Has community tweets
    "selentelechia"    # Has note tweets
]

@pytest.fixture
def live_archives(tmp_path):
    """Download real archives for testing."""
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    # Download test archives
    download_archives(TEST_ACCOUNTS, archive_dir)
    
    # Verify we got some data
    archives = list(archive_dir.glob("*_archive.json"))
    if not archives:
        pytest.skip("Failed to download any test archives")
    
    # Print what we got
    print("\nDownloaded archives:")
    for archive in archives:
        size_mb = archive.stat().st_size / (1024 * 1024)
        print(f"{archive.name}: {size_mb:.1f}MB")
        
    return archive_dir

def test_canonicalization_with_live_data(live_archives, tmp_path):
    """Test canonicalization using live archive data."""
    # First check what archives we actually got
    archives = list(live_archives.glob("*_archive.json"))
    usernames = {p.stem.split('_')[0].lower() for p in archives}
    
    if not usernames:
        pytest.skip("No archives downloaded successfully")
    
    print(f"\nSuccessfully downloaded archives for: {', '.join(sorted(usernames))}")
    
    output_file = tmp_path / "canonical.json"
    canonicalize_archive(live_archives, output_file)
    
    with open(output_file) as f:
        result = json.load(f)
    
    # Basic structure checks
    assert set(result.keys()) == {"tweets", "orphaned_likes", "profiles"}
    
    # Print summary stats
    tweets = result["tweets"]
    orphaned = result["orphaned_likes"]
    profiles = result["profiles"]
    
    print(f"\nCanonical archive summary:")
    print(f"Total tweets: {len(tweets)}")
    print(f"Total orphaned likes: {len(orphaned)}")
    print(f"Total profiles: {len(profiles)}")
    
    # Verify we got profiles for all archives we downloaded
    profile_usernames = {p["username"].lower() for p in profiles}
    assert profile_usernames == usernames, \
        f"Missing profiles for: {usernames - profile_usernames}"
    
    # Check tweet types
    print("\nTweet samples by author:")
    by_author = {}
    for tweet in tweets:
        author = tweet["author_username"]
        if author not in by_author:
            by_author[author] = []
        by_author[author].append(tweet)
    
    for author, author_tweets in by_author.items():
        print(f"\n{author} ({len(author_tweets)} tweets):")
        # Show first tweet from this author
        tweet = author_tweets[0]
        print(f"- {tweet['text'][:100]}...")
        print(f"  ID: {tweet['id']}")
        print(f"  Created: {tweet['created_at']}")
        print(f"  Likers: {len(tweet.get('likers', []))}")
    
    # Check likes distribution
    tweets_with_likes = sum(1 for t in tweets if t.get("likers"))
    print(f"\nLike stats:")
    print(f"Tweets with likes: {tweets_with_likes}")
    if tweets_with_likes:
        likes_counts = [len(t["likers"]) for t in tweets if t.get("likers")]
        avg_likes = sum(likes_counts) / len(likes_counts)
        max_likes = max(likes_counts)
        print(f"Average likes per tweet: {avg_likes:.1f}")
        print(f"Max likes on a tweet: {max_likes}")
    
    # Sample orphaned likes
    if orphaned:
        print(f"\nOrphaned likes sample:")
        for like in orphaned[:3]:
            print(f"\nTweet {like['tweet_id']}:")
            print(f"Text: {like['text'][:100]}...")
            print(f"Liked by: {len(like['likers'])} users")
    
    # Verify data integrity
    assert len(tweets) > 0, "Should have some tweets"
    assert all("id" in t for t in tweets), "All tweets should have IDs"
    assert all("text" in t for t in tweets), "All tweets should have text"
    assert all("created_at" in t for t in tweets), "All tweets should have timestamps"
    assert all("author_username" in t for t in tweets), "All tweets should have authors"
    
    # Verify chronological ordering
    timestamps = [t["created_at"] for t in tweets]
    assert timestamps == sorted(timestamps, reverse=True), "Tweets should be in reverse chronological order"