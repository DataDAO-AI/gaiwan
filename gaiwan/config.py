"""Configuration classes for Gaiwan."""

from dataclasses import dataclass

@dataclass
class MixPRConfig:
    """Configuration for MixPR retrieval."""
    local_alpha: float = 0.6
    similarity_threshold: float = 0.27
    max_iterations: int = 18
    min_df: int = 2
    max_df: float = 0.95
    batch_size: int = 1000
    graph_weight: float = 0.3  # Weight of graph relationships vs text similarity
    reply_weight: float = 1.0  # Weight of reply edges
    quote_weight: float = 0.8  # Weight of quote tweet edges
    user_similarity_weight: float = 0.4  # Weight for user similarity edges
    sibling_weight: float = 0.5  # Weight for sibling tweets (replies to same parent)
    conversation_weight: float = 0.4  # Weight for conversation context vs basic similarity

@dataclass
class UserSimilarityConfig:
    """Configuration for user similarity calculations."""
    min_tweets_per_user: int = 5
    min_likes_per_user: int = 3
    mention_weight: float = 0.7
    reply_weight: float = 0.8
    quote_weight: float = 0.6
    like_weight: float = 0.7
    retweet_weight: float = 0.8
    conversation_weight: float = 0.9
    temporal_weight: float = 0.5
    mutual_follow_weight: float = 0.8
    ncd_threshold: float = 0.7 