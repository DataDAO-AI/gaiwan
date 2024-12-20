"""User similarity calculations for graph construction."""

import logging
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple
import zlib
from collections import defaultdict

import numpy as np
from scipy import sparse

from .models import CanonicalTweet

logger = logging.getLogger(__name__)

@dataclass
class UserSimilarityConfig:
    """Configuration for user similarity calculations."""
    min_tweets_per_user: int = 5
    min_likes_per_user: int = 3
    mention_weight: float = 0.7
    reply_weight: float = 0.8
    quote_weight: float = 0.6
    ncd_threshold: float = 0.7

class UserSimilarityGraph:
    """Builds various user similarity graphs."""
    
    def __init__(self, config: UserSimilarityConfig):
        """Initialize with configuration."""
        self.config = config
        self.user_tweets: Dict[str, List[CanonicalTweet]] = defaultdict(list)
        self.user_likes: Dict[str, List[CanonicalTweet]] = defaultdict(list)
        
    def add_tweet(self, tweet: CanonicalTweet) -> None:
        """Add a tweet to the appropriate user collections."""
        if tweet.author_id:
            self.user_tweets[tweet.author_id].append(tweet)
        for liker in tweet.liked_by:
            self.user_likes[liker].append(tweet)
            
    def _compute_ncd(self, x: str, y: str) -> float:
        """Compute Normalized Compression Distance between two strings."""
        if not x or not y:
            return 1.0
        
        x_comp = len(zlib.compress(x.encode()))
        y_comp = len(zlib.compress(y.encode()))
        xy_comp = len(zlib.compress((x + y).encode()))
        
        return (xy_comp - min(x_comp, y_comp)) / max(x_comp, y_comp)
    
    def _get_user_text(self, user_id: str, include_tweets: bool = True, 
                      include_likes: bool = True) -> str:
        """Concatenate all text for a user."""
        texts = []
        if include_tweets:
            texts.extend(t.text for t in self.user_tweets[user_id])
        if include_likes:
            texts.extend(t.text for t in self.user_likes[user_id])
        return " ".join(texts)
    
    def compute_ncd_similarity(self, include_tweets: bool = True,
                             include_likes: bool = True) -> sparse.csr_matrix:
        """Compute NCD-based similarity matrix between users."""
        users = sorted({uid for uid, tweets in self.user_tweets.items() 
                       if len(tweets) >= self.config.min_tweets_per_user})
        n = len(users)
        user_to_idx = {uid: idx for idx, uid in enumerate(users)}
        
        # Pre-compute user texts
        user_texts = {
            uid: self._get_user_text(uid, include_tweets, include_likes)
            for uid in users
        }
        
        # Build sparse similarity matrix
        rows, cols, data = [], [], []
        
        for i, user1 in enumerate(users):
            for j in range(i+1, len(users)):
                user2 = users[j]
                ncd = self._compute_ncd(user_texts[user1], user_texts[user2])
                similarity = 1 - ncd
                
                if similarity > self.config.ncd_threshold:
                    rows.extend([i, j])
                    cols.extend([j, i])
                    data.extend([similarity, similarity])
                    
        return sparse.csr_matrix((data, (rows, cols)), shape=(n, n))
    
    def compute_interaction_similarity(self) -> sparse.csr_matrix:
        """Compute similarity matrix based on mentions, replies, and quotes."""
        users = sorted(self.user_tweets.keys())
        n = len(users)
        user_to_idx = {uid: idx for idx, uid in enumerate(users)}
        
        # Track interactions
        mentions = defaultdict(int)
        replies = defaultdict(int)
        quotes = defaultdict(int)
        
        for tweets in self.user_tweets.values():
            for tweet in tweets:
                author_idx = user_to_idx.get(tweet.author_id)
                if author_idx is None:
                    continue
                    
                # Count mentions
                for mentioned in tweet.metadata.mentioned_users:
                    if mentioned in user_to_idx:
                        mentions[(author_idx, user_to_idx[mentioned])] += 1
                        
                # Count replies
                if tweet.reply_to_tweet_id:
                    for t in self.user_tweets.values():
                        for potential_parent in t:
                            if potential_parent.id == tweet.reply_to_tweet_id:
                                if potential_parent.author_id in user_to_idx:
                                    replies[(author_idx, user_to_idx[potential_parent.author_id])] += 1
                                break
                                
                # Count quotes
                if tweet.metadata.quoted_tweet_id:
                    for t in self.user_tweets.values():
                        for potential_quoted in t:
                            if potential_quoted.id == tweet.metadata.quoted_tweet_id:
                                if potential_quoted.author_id in user_to_idx:
                                    quotes[(author_idx, user_to_idx[potential_quoted.author_id])] += 1
                                break
        
        # Build sparse matrix
        rows, cols, data = [], [], []
        
        def add_symmetric_edge(i: int, j: int, weight: float) -> None:
            rows.extend([i, j])
            cols.extend([j, i])
            data.extend([weight, weight])
            
        for (i, j), count in mentions.items():
            add_symmetric_edge(i, j, count * self.config.mention_weight)
            
        for (i, j), count in replies.items():
            add_symmetric_edge(i, j, count * self.config.reply_weight)
            
        for (i, j), count in quotes.items():
            add_symmetric_edge(i, j, count * self.config.quote_weight)
            
        return sparse.csr_matrix((data, (rows, cols)), shape=(n, n))
    
    def combine_similarity_graphs(self, matrices: List[Tuple[sparse.csr_matrix, float]]) -> sparse.csr_matrix:
        """Combine multiple similarity matrices with weights."""
        if not matrices:
            raise ValueError("No matrices provided")
            
        result = matrices[0][0] * matrices[0][1]
        for matrix, weight in matrices[1:]:
            result += matrix * weight
            
        # Normalize
        result.data /= result.data.max()
        return result 