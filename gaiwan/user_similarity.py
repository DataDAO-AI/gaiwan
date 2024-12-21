"""User similarity calculations for graph construction."""

import logging
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, DefaultDict
import zlib
from collections import defaultdict, Counter
from urllib.parse import urlparse

import numpy as np
from scipy import sparse
import math

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
    like_weight: float = 0.5
    retweet_weight: float = 0.4
    conversation_weight: float = 0.3
    community_weight: float = 0.6
    media_weight: float = 0.4
    url_weight: float = 0.5

class UserSimilarityGraph:
    """Builds various user similarity graphs."""
    
    def __init__(self, config: UserSimilarityConfig):
        """Initialize with configuration."""
        self.config = config
        self.user_tweets: Dict[str, List[CanonicalTweet]] = defaultdict(list)
        self.user_likes: Dict[str, List[CanonicalTweet]] = defaultdict(list)
        
        # Add new tracking structures
        self.follower_counts: Dict[str, int] = {}
        self.following_counts: Dict[str, int] = {}
        self.followers: Dict[str, Set[str]] = defaultdict(set)
        self.following: Dict[str, Set[str]] = defaultdict(set)
        self.tweet_counts: Dict[str, int] = {}
        self.like_counts: Dict[str, int] = {}
        self.mutual_likes: DefaultDict[Tuple[str, str], int] = defaultdict(int)
        self.mutual_retweets: DefaultDict[Tuple[str, str], int] = defaultdict(int)
        self.conversation_pairs: DefaultDict[Tuple[str, str], int] = defaultdict(int)
        self.user_communities: Dict[str, Set[str]] = defaultdict(set)
        self.user_media_types: Dict[str, Counter] = defaultdict(Counter)
        self.user_domains: Dict[str, Counter] = defaultdict(Counter)
        
    def add_tweet(self, tweet: CanonicalTweet) -> None:
        """Add a tweet to the appropriate user collections."""
        if tweet.author_id:
            self.user_tweets[tweet.author_id].append(tweet)
            if tweet.author_id not in self.like_counts:
                self.like_counts[tweet.author_id] = 0
            if tweet.author_id not in self.tweet_counts:
                self.tweet_counts[tweet.author_id] = 0
            self.tweet_counts[tweet.author_id] += 1
            
        for liker in tweet.liked_by:
            self.user_likes[liker].append(tweet)
            if liker not in self.like_counts:
                self.like_counts[liker] = 0
            self.like_counts[liker] += 1
            
        if tweet.author_id:
            if tweet.source_type == 'community_tweet' and tweet.community_id:
                self.user_communities[tweet.author_id].add(tweet.community_id)
            
            for media in tweet.metadata.media:
                self.user_media_types[tweet.author_id][media.get('type', 'unknown')] += 1
            
            for url in tweet.metadata.urls:
                try:
                    domain = urlparse(url).netloc
                    self.user_domains[tweet.author_id][domain] += 1
                except Exception:
                    continue

    def add_social_data(self, user_id: str, followers: Set[str], following: Set[str],
                       tweet_count: int, like_count: int) -> None:
        """Add social graph data for a user."""
        self.follower_counts[user_id] = len(followers)
        self.following_counts[user_id] = len(following)
        self.followers[user_id] = followers
        self.following[user_id] = following
        self.tweet_counts[user_id] = tweet_count
        self.like_counts[user_id] = like_count

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
        """Enhanced interaction similarity incorporating more signals."""
        users = sorted(self.user_tweets.keys())
        n = len(users)
        user_to_idx = {uid: idx for idx, uid in enumerate(users)}
        
        rows, cols, data = [], [], []
        
        for user1 in users:
            idx1 = user_to_idx[user1]
            if user1 not in self.like_counts:
                self.like_counts[user1] = 0
            if user1 not in self.tweet_counts:
                self.tweet_counts[user1] = 0
            
            for user2 in users:
                if user1 >= user2:
                    continue
                    
                idx2 = user_to_idx[user2]
                if user2 not in self.like_counts:
                    self.like_counts[user2] = 0
                if user2 not in self.tweet_counts:
                    self.tweet_counts[user2] = 0
                
                pair = tuple(sorted([user1, user2]))
                
                # Combine multiple interaction signals
                mutual_like_strength = self.mutual_likes[pair] / math.sqrt(self.like_counts[user1] * self.like_counts[user2]) if self.like_counts[user1] and self.like_counts[user2] else 0
                mutual_retweet_strength = self.mutual_retweets[pair] / math.sqrt(self.tweet_counts[user1] * self.tweet_counts[user2]) if self.tweet_counts[user1] and self.tweet_counts[user2] else 0
                conversation_strength = self.conversation_pairs[pair] / math.sqrt(self.tweet_counts[user1] * self.tweet_counts[user2]) if self.tweet_counts[user1] and self.tweet_counts[user2] else 0
                
                strength = (
                    mutual_like_strength * self.config.like_weight +
                    mutual_retweet_strength * self.config.retweet_weight +
                    conversation_strength * self.config.conversation_weight
                )
                
                if strength > 0:
                    rows.extend([idx1, idx2])
                    cols.extend([idx2, idx1])
                    data.extend([strength, strength])
        
        return sparse.csr_matrix((data, (rows, cols)), shape=(n, n))
    
    def compute_temporal_similarity(self) -> sparse.csr_matrix:
        """Compute similarity based on temporal tweeting patterns."""
        users = sorted(self.user_tweets.keys())
        n = len(users)
        user_to_idx = {uid: idx for idx, uid in enumerate(users)}
        
        # Create temporal activity vectors (24 hours)
        hour_vectors = np.zeros((n, 24))
        
        for user_id in users:
            idx = user_to_idx[user_id]
            for tweet in self.user_tweets[user_id]:
                if tweet.created_at:
                    hour_vectors[idx, tweet.created_at.hour] += 1
                    
        # Normalize vectors
        row_sums = hour_vectors.sum(axis=1)
        # Use np.where for conditional operation
        hour_vectors = np.where(
            row_sums[:, np.newaxis] > 0,
            hour_vectors / row_sums[:, np.newaxis],
            hour_vectors
        )
        
        # Compute cosine similarity
        similarity = hour_vectors @ hour_vectors.T
        
        return sparse.csr_matrix(similarity)
    
    def compute_mutual_follow_strength(self) -> sparse.csr_matrix:
        """Compute similarity based on mutual follow relationships."""
        users = sorted(self.user_tweets.keys())
        n = len(users)
        user_to_idx = {uid: idx for idx, uid in enumerate(users)}
        
        rows, cols, data = [], [], []
        
        for user1 in users:
            idx1 = user_to_idx[user1]
            following1 = self.following[user1]
            followers1 = self.followers[user1]
            
            for user2 in users:
                if user1 >= user2:  # Only compute once per pair
                    continue
                    
                idx2 = user_to_idx[user2]
                following2 = self.following[user2]
                followers2 = self.followers[user2]
                
                # Calculate mutual follow strength
                mutual_follows = 0
                if user1 in followers2 and user2 in followers1:
                    mutual_follows = 1
                elif user1 in followers2 or user2 in followers1:
                    mutual_follows = 0.5
                
                # Calculate Jaccard similarity of follow/follower networks
                following_jaccard = len(following1 & following2) / len(following1 | following2) if following1 or following2 else 0
                follower_jaccard = len(followers1 & followers2) / len(followers1 | followers2) if followers1 or followers2 else 0
                
                strength = (mutual_follows + following_jaccard + follower_jaccard) / 3
                if strength > 0:
                    rows.extend([idx1, idx2])
                    cols.extend([idx2, idx1])
                    data.extend([strength, strength])
        
        return sparse.csr_matrix((data, (rows, cols)), shape=(n, n))
    
    def compute_content_similarity(self) -> sparse.csr_matrix:
        """Compute similarity based on content patterns."""
        users = sorted(self.user_tweets.keys())
        n = len(users)
        user_to_idx = {uid: idx for idx, uid in enumerate(users)}
        
        rows, cols, data = [], [], []
        
        for i, user1 in enumerate(users):
            for j in range(i+1, len(users)):
                user2 = users[j]
                
                # Community similarity
                community_sim = len(
                    self.user_communities[user1] & self.user_communities[user2]
                ) / max(
                    len(self.user_communities[user1] | self.user_communities[user2]), 1
                )
                
                # Media type similarity
                media1 = self.user_media_types[user1]
                media2 = self.user_media_types[user2]
                media_sim = sum(
                    min(media1[t], media2[t])
                    for t in set(media1) & set(media2)
                ) / max(
                    sum(media1.values()) + sum(media2.values()), 1
                )
                
                # Domain similarity
                domains1 = self.user_domains[user1]
                domains2 = self.user_domains[user2]
                domain_sim = sum(
                    min(domains1[d], domains2[d])
                    for d in set(domains1) & set(domains2)
                ) / max(
                    sum(domains1.values()) + sum(domains2.values()), 1
                )
                
                # Combine similarities
                strength = (
                    community_sim * self.config.community_weight +
                    media_sim * self.config.media_weight +
                    domain_sim * self.config.url_weight
                ) / (
                    self.config.community_weight +
                    self.config.media_weight +
                    self.config.url_weight
                )
                
                if strength > 0:
                    rows.extend([i, j])
                    cols.extend([j, i])
                    data.extend([strength, strength])
        
        return sparse.csr_matrix((data, (rows, cols)), shape=(n, n))
    
    def combine_similarity_graphs(self, matrices: List[Tuple[sparse.csr_matrix, float]]) -> sparse.csr_matrix:
        """Combine multiple similarity matrices with weights."""
        # Add content similarity to the mix
        content_sim = self.compute_content_similarity()
        matrices.append((content_sim, 0.5))  # Weight can be adjusted
        
        return super().combine_similarity_graphs(matrices) 