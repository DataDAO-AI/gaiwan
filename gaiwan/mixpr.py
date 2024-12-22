"""MixPR implementation for tweet context retrieval."""

import logging
import re
from datetime import datetime
from typing import List, Dict, Optional, Set

import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from .models import CanonicalTweet, RetrievalResult
from .config import MixPRConfig

logger = logging.getLogger(__name__)

class MixPR:
    """MixPR implementation for tweet context retrieval."""

    def __init__(self, config: MixPRConfig):
        """Initialize MixPR with configuration."""
        self.config = config
        self.vectorizer = TfidfVectorizer(
            min_df=1,
            max_df=1.0,
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
        self.user_similarity_matrix = None

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
        """Compute adjacency matrix."""
        similarity_matrix = self.embeddings @ self.embeddings.T
        similarity_matrix = sparse.csr_matrix(similarity_matrix)

        graph_matrix = sparse.lil_matrix((len(self.tweets), len(self.tweets)))
        for idx, tweet in enumerate(self.tweets):
            # Add reply relationships
            if tweet.in_reply_to_status_id in self.tweet_id_to_idx:
                reply_idx = self.tweet_id_to_idx[tweet.in_reply_to_status_id]
                graph_matrix[idx, reply_idx] = self.config.reply_weight
                graph_matrix[reply_idx, idx] = self.config.reply_weight

            # Add quote relationships
            if tweet.quoted_tweet_id in self.tweet_id_to_idx:
                quote_idx = self.tweet_id_to_idx[tweet.quoted_tweet_id]
                graph_matrix[idx, quote_idx] = self.config.quote_weight
                graph_matrix[quote_idx, idx] = self.config.quote_weight

            # Add user similarity relationships
            if tweet.screen_name and self.user_similarity_matrix is not None:
                user_idx = list(self.user_similarity_matrix.keys()).index(tweet.screen_name)
                for other_idx, other_tweet in enumerate(self.tweets):
                    if other_tweet.screen_name:
                        other_user_idx = list(self.user_similarity_matrix.keys()).index(other_tweet.screen_name)
                        sim = self.user_similarity_matrix[user_idx, other_user_idx]
                        graph_matrix[idx, other_idx] = sim * self.config.user_similarity_weight

        # Combine matrices
        combined_matrix = (1 - self.config.graph_weight) * similarity_matrix + \
                         self.config.graph_weight * graph_matrix.tocsr()
        
        # Sparsify and normalize
        combined_matrix.data[combined_matrix.data < self.config.similarity_threshold] = 0
        combined_matrix.eliminate_zeros()
        return normalize(combined_matrix, norm='l1', axis=0)

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
        """Classify if query requires local or global retrieval."""
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
        has_mentions = len(tweet.mentioned_users) > 0

        return has_question or is_reply or has_mentions

    def retrieve(self, query_tweet: CanonicalTweet, k: int = 10) -> List[RetrievalResult]:
        """Enhanced retrieval using conversation context"""
        n = len(self.tweets)  # Get total number of tweets
        
        # Get basic similarity scores and convert to dense array
        basic_scores = np.asarray(self._compute_basic_similarity(query_tweet)).flatten()
        
        # Add conversation context
        if query_tweet.id in self.tweet_id_to_idx:
            query_idx = self.tweet_id_to_idx[query_tweet.id]
            conversation_context = self._compute_conversation_context(query_idx)
            
            # Convert sparse matrix to dense array and ensure correct shape
            context_scores = np.asarray(conversation_context.toarray()).flatten()
            
            # Ensure both arrays have the same shape
            if len(basic_scores) != len(context_scores):
                # Pad the shorter array with zeros
                if len(basic_scores) < n:
                    basic_scores = np.pad(basic_scores, (0, n - len(basic_scores)))
                if len(context_scores) < n:
                    context_scores = np.pad(context_scores, (0, n - len(context_scores)))
            
            # Combine scores
            final_scores = (
                (1 - self.config.conversation_weight) * basic_scores +
                self.config.conversation_weight * context_scores
            )
        else:
            final_scores = basic_scores
            
        # Return top results
        return self._get_top_results(final_scores, k, exclude_ids={query_tweet.id})

    def _create_personalization_vector(
        self,
        query_idx: int,
        is_local: bool
    ) -> np.ndarray:
        """
        Create personalization vector incorporating conversation structure.
        """
        n = len(self.tweets)
        p = np.zeros(n)
        
        query_tweet = self.tweets[query_idx]
        
        if is_local:
            # Add weight to query tweet
            p[query_idx] = 0.5
            
            # Add weight to direct conversation participants
            if query_tweet.reply_to_tweet_id in self.tweet_id_to_idx:
                parent_idx = self.tweet_id_to_idx[query_tweet.reply_to_tweet_id]
                p[parent_idx] = 0.3
                
            # Add weight to mentioned users' recent tweets
            mentioned_weight = 0.2 / max(len(query_tweet.metadata.mentioned_users), 1)
            for user in query_tweet.metadata.mentioned_users:
                user_tweets = [idx for idx, t in enumerate(self.tweets) 
                             if t.author_id == user]
                if user_tweets:
                    p[user_tweets] = mentioned_weight
        else:
            # For global queries, use uniform distribution
            p = np.ones(n) / n
            
        return p

    def _compute_conversation_context(self, tweet_idx: int) -> sparse.csr_matrix:
        """Compute conversation context matrix"""
        n = len(self.tweets)
        context_matrix = sparse.lil_matrix((n, n))  # Ensure matrix is n x n
        
        tweet = self.tweets[tweet_idx]
        
        # Add reply chain context
        if tweet.reply_to_tweet_id in self.tweet_id_to_idx:
            parent_idx = self.tweet_id_to_idx[tweet.reply_to_tweet_id]
            context_matrix[tweet_idx, parent_idx] = self.config.reply_weight
            
            # Find siblings (other replies to same tweet)
            for idx, other_tweet in enumerate(self.tweets):
                if other_tweet.reply_to_tweet_id == tweet.reply_to_tweet_id:
                    context_matrix[tweet_idx, idx] = self.config.sibling_weight
                    
        # Convert to CSR format and ensure it's a vector
        return context_matrix.tocsr()[tweet_idx]  # Return just the row for tweet_idx

    def _compute_basic_similarity(self, query_tweet: CanonicalTweet) -> np.ndarray:
        """Compute basic similarity scores between query and all tweets."""
        query_vec = self.vectorizer.transform([query_tweet.text])
        return (query_vec @ self.embeddings.T).toarray().flatten()

    def _get_top_results(self, scores: np.ndarray, k: int, exclude_ids: Set[str] = None) -> List[RetrievalResult]:
        """Get top k results from similarity scores."""
        if exclude_ids is None:
            exclude_ids = set()
        
        # Ensure scores is a 1D numpy array
        scores = np.asarray(scores).flatten()
        
        # Get indices of tweets not in exclude_ids
        valid_indices = [
            i for i, tweet in enumerate(self.tweets)
            if tweet.id not in exclude_ids
        ]
        
        if not valid_indices:
            return []
        
        # Get scores for valid indices
        valid_scores = scores[valid_indices]
        
        # Get top k indices
        k = min(k, len(valid_indices))
        top_k_indices = np.argsort(valid_scores)[-k:][::-1]
        
        # Convert to RetrievalResult objects
        results = []
        for idx in top_k_indices:
            tweet_idx = valid_indices[idx]
            results.append(RetrievalResult(
                tweet=self.tweets[tweet_idx],
                score=float(valid_scores[idx])
            ))
        
        return results
