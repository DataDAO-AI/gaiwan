# stats_collector.py
"""Collect and analyze statistics from Twitter archive data."""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, DefaultDict, Optional, List
from urllib.parse import urlparse

import orjson
import logging

from models import CanonicalTweet

logger = logging.getLogger(__name__)

@dataclass
class ArchiveStats:
    """Statistics collected from processing Twitter archives."""
    # Basic counts
    total_tweets: int = 0
    total_replies: int = 0
    total_likes: int = 0
    total_retweets: int = 0
    total_quotes: int = 0

    # User interaction stats
    mentioned_users: Counter = field(default_factory=Counter)
    replied_to_users: Counter = field(default_factory=Counter)
    retweeted_users: Counter = field(default_factory=Counter)

    # Content stats
    hashtag_usage: Counter = field(default_factory=Counter)
    domains_shared: Counter = field(default_factory=Counter)
    tweet_lengths: List[int] = field(default_factory=list)

    # Temporal stats
    tweets_by_hour: Counter = field(default_factory=Counter)
    tweets_by_dow: Counter = field(default_factory=Counter)
    tweets_by_month: Counter = field(default_factory=Counter)

    # Conversation stats
    thread_lengths: Counter = field(default_factory=Counter)
    conversation_participants: DefaultDict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    # Client usage
    client_sources: Counter = field(default_factory=Counter)

    # Language stats
    languages_used: Counter = field(default_factory=Counter)

    # Engagement metrics
    likes_received: DefaultDict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    retweets_received: DefaultDict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )

    # Time periods
    first_tweet_date: Optional[datetime] = None
    last_tweet_date: Optional[datetime] = None

    def update_from_tweet(self, tweet: CanonicalTweet) -> None:
        """Update stats from a single tweet."""
        self.total_tweets += 1

        if tweet.reply_to_tweet_id:
            self.total_replies += 1
            if tweet.author_id:
                self.replied_to_users[tweet.author_id] += 1

        if tweet.metadata.is_retweet:
            self.total_retweets += 1
            if tweet.metadata.retweet_of_id:
                self.retweeted_users[tweet.metadata.retweet_of_id] += 1

        if tweet.metadata.quoted_tweet_id:
            self.total_quotes += 1

        # Update temporal stats
        if tweet.created_at:
            if not self.first_tweet_date or tweet.created_at < self.first_tweet_date:
                self.first_tweet_date = tweet.created_at
            if not self.last_tweet_date or tweet.created_at > self.last_tweet_date:
                self.last_tweet_date = tweet.created_at

            self.tweets_by_hour[tweet.created_at.hour] += 1
            self.tweets_by_dow[tweet.created_at.strftime('%A')] += 1
            self.tweets_by_month[tweet.created_at.strftime('%Y-%m')] += 1

        # Content stats
        self.tweet_lengths.append(len(tweet.text))
        self.hashtag_usage.update(tweet.metadata.hashtags)
        self.mentioned_users.update(tweet.metadata.mentioned_users)

        # Process URLs
        for url in tweet.metadata.urls:
            try:
                domain = urlparse(url).netloc
                self.domains_shared[domain] += 1
            except Exception:
                continue

        # Update likes
        self.total_likes += len(tweet.liked_by)

    def generate_summary(self) -> dict:
        """Generate a summary dictionary of the collected stats."""
        active_days = (
            (self.last_tweet_date - self.first_tweet_date).days + 1
            if self.first_tweet_date and self.last_tweet_date
            else 0
        )

        return {
            "tweet_counts": {
                "total": self.total_tweets,
                "replies": self.total_replies,
                "retweets": self.total_retweets,
                "quotes": self.total_quotes,
                "original": (
                    self.total_tweets - self.total_replies
                    - self.total_retweets - self.total_quotes
                ),
                "total_likes": self.total_likes
            },
            "activity_period": {
                "first_tweet": self.first_tweet_date.isoformat() if self.first_tweet_date else None,
                "last_tweet": self.last_tweet_date.isoformat() if self.last_tweet_date else None,
                "active_days": active_days,
                "tweets_per_day": round(self.total_tweets / active_days, 2) if active_days else 0
            },
            "content_metrics": {
                "avg_tweet_length": round(sum(self.tweet_lengths) / len(self.tweet_lengths), 2) if self.tweet_lengths else 0,
                "top_hashtags": dict(self.hashtag_usage.most_common(10)),
                "top_domains": dict(self.domains_shared.most_common(10)),
                "top_mentioned_users": dict(self.mentioned_users.most_common(10))
            },
            "engagement_metrics": {
                "total_likes_received": sum(self.likes_received.values()),
                "total_retweets_received": sum(self.retweets_received.values()),
                "avg_likes_per_tweet": round(
                    sum(self.likes_received.values()) / self.total_tweets, 2
                ) if self.total_tweets else 0
            },
            "temporal_patterns": {
                "busiest_hours": dict(self.tweets_by_hour.most_common(5)),
                "busiest_days": dict(self.tweets_by_dow.most_common()),
                "tweets_by_month": dict(sorted(self.tweets_by_month.items()))
            }
        }

class StatsManager:
    """Manages collection and storage of archive statistics."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.stats_dir = output_dir / "stats"
        self.stats_dir.mkdir(parents=True, exist_ok=True)

    def process_archive(self, archive_path: Path, tweets: List[CanonicalTweet]) -> None:
        """Process an archive and generate statistics."""
        stats = ArchiveStats()

        for tweet in tweets:
            stats.update_from_tweet(tweet)

        # Write individual archive stats
        stats_file = self.stats_dir / f"{archive_path.stem}_stats.json"
        with stats_file.open('wb') as f:
            f.write(orjson.dumps(stats.generate_summary()))

        logger.info(f"Generated stats for {archive_path.stem}: {stats.total_tweets} tweets processed")

    def generate_aggregate_stats(self) -> dict:
        """Generate aggregate statistics across all processed archives."""
        aggregate_stats = ArchiveStats()

        for stats_file in self.stats_dir.glob('*_stats.json'):
            try:
                with stats_file.open('rb') as f:
                    archive_stats = orjson.loads(f.read())
                    # TODO: Implement proper aggregation logic
                    # This would need careful consideration of how to
                    # meaningfully combine stats across archives

            except Exception as e:
                logger.error(f"Error processing stats file {stats_file}: {e}")
                continue

        return aggregate_stats.generate_summary()
