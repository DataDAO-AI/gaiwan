"""Tests for MixPR implementation."""

import pytest
import numpy as np
from scipy import sparse
from gaiwan.mixpr import MixPR
from gaiwan.models import CanonicalTweet, TweetMetadata
from datetime import datetime, timezone
from pathlib import Path

@pytest.fixture
def sample_tweets():
    return [
        CanonicalTweet(
            id="1",
            text="Hello world! @user2 check this out",
            screen_name="user1",
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            entities={
                "user_mentions": [{"screen_name": "user2"}],
                "urls": [{"expanded_url": "https://example.com"}]
            }
        )
    ]

def test_mixpr_initialization(mixpr_config):
    """Test MixPR initialization with config."""
    mixpr = MixPR(mixpr_config)
    assert mixpr.config == mixpr_config
    assert mixpr.vectorizer is not None

def test_mixpr_text_preprocessing(mixpr_config):
    """Test text preprocessing for TF-IDF."""
    mixpr = MixPR(mixpr_config)
    text = "Hello @User! Check https://example.com #Test"
    processed = mixpr._preprocess_text(text)
    
    assert "@" not in processed
    assert "http" not in processed
    assert "#" not in processed
    assert "user" in processed.lower()
    assert "example" in processed.lower() and "com" in processed.lower()

def test_mixpr_fit(mixpr_config, sample_tweets):
    """Test fitting MixPR on sample tweets."""
    mixpr = MixPR(mixpr_config)
    mixpr.fit(sample_tweets)
    
    assert mixpr.embeddings is not None
    assert mixpr.adjacency_matrix is not None
    assert mixpr.embeddings.shape[0] == len(sample_tweets)
    assert mixpr.adjacency_matrix.shape == (len(sample_tweets), len(sample_tweets))
    
    # Verify adjacency matrix
    for idx, tweet in enumerate(mixpr.tweets):
        if tweet.in_reply_to_status_id:  # Use correct field name
            parent_idx = mixpr.tweet_id_to_idx.get(tweet.in_reply_to_status_id)
            if parent_idx is not None:
                assert mixpr.adjacency_matrix[idx, parent_idx] > 0

def test_mixpr_retrieval(mixpr_config, tmp_path):
    tweets = [
        CanonicalTweet(
            id="1",
            text="Original tweet",
            screen_name="user1",
            created_at=datetime.now(timezone.utc),
            entities={}
        ),
        CanonicalTweet(
            id="2",
            text="Reply to original",
            screen_name="user2",
            created_at=datetime.now(timezone.utc),
            entities={},
            in_reply_to_status_id="1"
        )
    ]
    
    mixpr = MixPR(mixpr_config)
    mixpr.fit(tweets)
    
    results = mixpr.retrieve(tweets[0], k=2)
    
    assert len(results) <= 2
    assert all(isinstance(r.score, float) for r in results)
    assert all(r.tweet in tweets for r in results)

def test_query_classification(mixpr_config):
    """Test query type classification."""
    mixpr = MixPR(mixpr_config)
    
    question_tweet = CanonicalTweet(
        id="1",
        text="What do you think about this?",
        screen_name="user1",
        created_at=datetime.now(timezone.utc),
        entities={}
    )
    assert mixpr._classify_query_type(question_tweet) is True
    
    regular_tweet = CanonicalTweet(
        id="2",
        text="Just sharing my thoughts.",
        screen_name="user1",
        created_at=datetime.now(timezone.utc),
        entities={}
    )
    assert mixpr._classify_query_type(regular_tweet) is False