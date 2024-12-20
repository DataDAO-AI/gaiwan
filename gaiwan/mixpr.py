"""MixPR implementation for tweet context retrieval."""

import logging
import re
from datetime import datetime
from typing import List, Dict, Optional

import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from .models import CanonicalTweet, MixPRConfig, RetrievalResult

logger = logging.getLogger(__name__)

class MixPR:
    """MixPR implementation for tweet context retrieval."""

    def __init__(self, config: MixPRConfig):
        """Initialize MixPR with configuration."""
        self.config = config
        self.vectorizer = TfidfVectorizer(
            min_df=config.min_df,
            max_df=config.max_df,
            strip_accents='unicode',
            analyzer='word',
            token_pattern=r'\b\w+\b',
            ngram_range=(1, 1),
            norm='l2'
        )

        self.tweets: List[CanonicalTweet] = []
        self.tweet_id_to_idx: Dict[str, int] = {}
        self.embeddings: Optional[sparse.csr_matrix] = None
        self.adjacency_matrix: Optional[sparse.csr_matrix] = None

    def _preprocess_text(self, text: str) -> str:
        """
        Preprocess tweet text for TF-IDF vectorization.

        Args:
            text: Raw tweet text

        Returns:
            Preprocessed text with normalized tokens
        """
        text = text.lower()
        # Remove URLs while preserving domain for context
        text = re.sub(r'https?://([^\s/]+)[^\s]*', r'\1', text)
        # Remove mentions while preserving username for context
        text = re.sub(r'@(\w+)', r'\1', text)
        # Remove hashtag symbol but keep the text
        text = re.sub(r'#(\w+)', r'\1', text)
        # Remove special characters
        text = re.sub(r'[^\w\s]', ' ', text)
        # Normalize whitespace
        return ' '.join(text.split())

    def fit(self, tweets: List[CanonicalTweet]) -> None:
        """
        Fit MixPR on a collection of tweets.

        Args:
            tweets: List of tweets to process
        """
        logger.info("Fitting MixPR on %d tweets", len(tweets))

        self.tweets = tweets
        self.tweet_id_to_idx = {tweet.id: idx for idx, tweet in enumerate(tweets)}

        # Create TF-IDF embeddings
        preprocessed_texts = [self._preprocess_text(tweet.text) for tweet in tweets]
        self.embeddings = self.vectorizer.fit_transform(preprocessed_texts)

        # Compute similarity matrix
        self.adjacency_matrix = self._compute_adjacency_matrix()
        logger.info(
            "Created adjacency matrix with %d non-zero elements",
            self.adjacency_matrix.nnz
        )

    def _compute_adjacency_matrix(self) -> sparse.csr_matrix:
        """
        Compute and normalize the adjacency matrix.

        Returns:
            Sparse adjacency matrix with normalized columns
        """
        # Compute cosine similarity matrix
        similarity_matrix = self.embeddings @ self.embeddings.T

        # Sparsify by threshold
        similarity_matrix = sparse.csr_matrix(similarity_matrix)
        similarity_matrix.data[
            similarity_matrix.data < self.config.similarity_threshold
        ] = 0
        similarity_matrix.eliminate_zeros()

        # Normalize columns for PageRank
        return normalize(similarity_matrix, norm='l1', axis=0)

    def _personalized_pagerank(
        self,
        query_idx: int,
        alpha: float
    ) -> np.ndarray:
        """
        Run personalized PageRank from query tweet.

        Args:
            query_idx: Index of query tweet
            alpha: Teleport probability

        Returns:
            PageRank scores for all tweets
        """
        n = len(self.tweets)

        # Initialize personalization vector
        p = np.zeros(n)
        p[query_idx] = 1.0

        # Initialize PageRank vector
        pi = np.ones(n) / n

        # Power iteration
        for _ in range(self.config.max_iterations):
            pi_next = (1 - alpha) * (self.adjacency_matrix @ pi) + alpha * p

            if np.allclose(pi, pi_next, rtol=1e-6):
                break

            pi = pi_next

        return pi

    def _classify_query_type(self, tweet: CanonicalTweet) -> bool:
        """
        Classify if query requires local (True) or global (False) retrieval.

        Args:
            tweet: Query tweet

        Returns:
            True if query needs local context, False for global
        """
        # Keywords suggesting specific information needs
        local_keywords = {
            'what', 'when', 'where', 'who', 'why', 'how',
            'explain', 'details', 'specifically'
        }

        # Check for question marks or question words
        text = tweet.text.lower()
        has_question = '?' in text or any(word in text.split() for word in local_keywords)

        # Check if it's a reply (more likely to need local context)
        is_reply = bool(tweet.reply_to_tweet_id)

        # Check for @mentions (suggesting conversation context needed)
        has_mentions = bool(tweet.metadata.mentioned_users)

        return has_question or is_reply or has_mentions

    def retrieve(
        self,
        query_tweet: CanonicalTweet,
        k: int = 10,
        force_mode: Optional[str] = None
    ) -> List[RetrievalResult]:
        """
        Retrieve k most relevant tweets for the query tweet.

        Args:
            query_tweet: Tweet to find context for
            k: Number of tweets to retrieve
            force_mode: Force 'local' or 'global' mode, or None for automatic

        Returns:
            List of retrieval results ordered by relevance and time
        """
        query_idx = self.tweet_id_to_idx[query_tweet.id]

        # Determine retrieval mode
        if force_mode == 'local':
            alpha = self.config.local_alpha
        elif force_mode == 'global':
            alpha = 0.0
        else:
            is_local = self._classify_query_type(query_tweet)
            alpha = self.config.local_alpha if is_local else 0.0

        # Run PageRank
        scores = self._personalized_pagerank(query_idx, alpha)

        # Get top-k indices excluding query tweet
        top_indices = np.argsort(scores)[::-1]
        top_indices = top_indices[top_indices != query_idx][:k]

        # Create results
        results = [
            RetrievalResult(
                tweet=self.tweets[idx],
                score=float(scores[idx])
            )
            for idx in top_indices
        ]

        # Sort by score groups then time
        results.sort(
            key=lambda x: (
                round(x.score, 3),
                x.tweet.created_at or datetime.min
            )
        )

        return results
