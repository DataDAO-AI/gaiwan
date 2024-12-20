"""Tests for user similarity calculations."""

import pytest
import numpy as np
from scipy import sparse
from gaiwan.user_similarity import UserSimilarityGraph

def test_user_similarity_initialization(user_similarity_config):
    """Test UserSimilarityGraph initialization."""
    graph = UserSimilarityGraph(user_similarity_config)
    assert graph.config == user_similarity_config
    assert len(graph.user_tweets) == 0
    assert len(graph.user_likes) == 0

def test_add_tweet(user_similarity_config, sample_tweets):
    """Test adding tweets to user collections."""
    graph = UserSimilarityGraph(user_similarity_config)
    
    for tweet in sample_tweets:
        graph.add_tweet(tweet)
    
    assert len(graph.user_tweets) > 0
    assert all(isinstance(tweets, list) for tweets in graph.user_tweets.values())

def test_ncd_computation(user_similarity_config):
    """Test Normalized Compression Distance computation."""
    graph = UserSimilarityGraph(user_similarity_config)
    
    text1 = "Hello world"
    text2 = "Hello there world"
    text3 = "Something completely different"
    
    ncd1 = graph._compute_ncd(text1, text2)
    ncd2 = graph._compute_ncd(text1, text3)
    
    assert 0 <= ncd1 <= 1
    assert 0 <= ncd2 <= 1
    assert ncd1 < ncd2  # Similar texts should have lower NCD

def test_similarity_matrix_computation(user_similarity_config, sample_tweets):
    """Test computation of similarity matrices."""
    graph = UserSimilarityGraph(user_similarity_config)
    
    for tweet in sample_tweets:
        graph.add_tweet(tweet)
    
    ncd_matrix = graph.compute_ncd_similarity()
    interaction_matrix = graph.compute_interaction_similarity()
    
    assert isinstance(ncd_matrix, sparse.csr_matrix)
    assert isinstance(interaction_matrix, sparse.csr_matrix)
    assert ncd_matrix.shape[0] == ncd_matrix.shape[1]
    assert interaction_matrix.shape[0] == interaction_matrix.shape[1] 