"""Tests for MixPR implementation."""

import pytest
import numpy as np
from scipy import sparse
from gaiwan.mixpr import MixPR, MixPRConfig
from gaiwan.models import CanonicalTweet, TweetMetadata
from datetime import datetime, timezone

@pytest.fixture
def sample_tweets():
    return [
        CanonicalTweet(
            id="1",
            text="Hello world! @user2 check this out",
            author_id="user1",
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            liked_by=set(),
            metadata=TweetMetadata(
                mentioned_users={"user2"},
                hashtags=set(),
                urls={"https://example.com"}
            )
        )
    ]

@pytest.fixture
def mixpr_config():
    """Create a MixPR configuration for testing."""
    return MixPRConfig(
        local_alpha=0.6,
        similarity_threshold=0.2,
        max_iterations=10,
        min_df=1,
        max_df=0.95,
        batch_size=100,
        graph_weight=0.3,
        reply_weight=1.0,
        quote_weight=0.8,
        user_similarity_weight=0.4
    )

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

def test_mixpr_retrieval(mixpr_config, sample_tweets):
    """Test tweet retrieval."""
    mixpr = MixPR(mixpr_config)
    mixpr.fit(sample_tweets)
    
    results = mixpr.retrieve(sample_tweets[0], k=2)
    
    assert len(results) <= 2
    assert all(isinstance(r.score, float) for r in results)
    assert all(r.tweet in sample_tweets for r in results)

def test_query_classification(mixpr_config):
    """Test query type classification."""
    mixpr = MixPR(mixpr_config)
    
    # Create proper CanonicalTweet objects instead of modifying dict
    question_tweet = CanonicalTweet(
        id="1",
        text="What do you think about this?",
        author_id="user1",
        created_at=datetime.now(timezone.utc)
    )
    assert mixpr._classify_query_type(question_tweet) is True
    
    regular_tweet = CanonicalTweet(
        id="2",
        text="Just sharing my thoughts.",
        author_id="user1",
        created_at=datetime.now(timezone.utc)
    )
    assert mixpr._classify_query_type(regular_tweet) is False