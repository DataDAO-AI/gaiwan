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
import json

from gaiwan.models import CanonicalTweet

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

    # Add fields for different tweet types
    total_community_tweets: int = 0
    total_note_tweets: int = 0

    # Add media stats
    media_counts: Counter = field(default_factory=Counter)  # type -> count
    media_by_type: DefaultDict[str, List[dict]] = field(
        default_factory=lambda: defaultdict(list)
    )
    
    # Add URL stats
    urls_by_domain: Counter = field(default_factory=Counter)
    t_co_to_expanded: Dict[str, str] = field(default_factory=dict)
    
    # Add community stats
    communities: Counter = field(default_factory=Counter)

    def update_from_tweet(self, tweet: CanonicalTweet) -> None:
        """Update stats from a single tweet."""
        # Track tweet type
        if tweet.source_type == 'tweet':
            self.total_tweets += 1
        elif tweet.source_type == 'community_tweet':
            self.total_community_tweets += 1
            if tweet.community_id:
                self.communities[tweet.community_id] += 1
        elif tweet.source_type == 'note':
            self.total_note_tweets += 1
        elif tweet.source_type == 'like':
            self.total_likes += 1

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

        # Process media
        for media in tweet.metadata.media:
            media_type = media.get('type', 'unknown')
            self.media_counts[media_type] += 1
            self.media_by_type[media_type].append(media)

        # Update likes
        self.total_likes += len(tweet.liked_by)

    def generate_summary(self) -> dict:
        """Generate a summary dictionary of the collected stats."""
        active_days = (
            (self.last_tweet_date - self.first_tweet_date).days + 1
            if self.first_tweet_date and self.last_tweet_date
            else 0
        )

        summary = {
            "tweet_counts": {
                "total": str(self.total_tweets),
                "replies": str(self.total_replies),
                "retweets": str(self.total_retweets),
                "quotes": str(self.total_quotes),
                "original": str(
                    self.total_tweets - self.total_replies
                    - self.total_retweets - self.total_quotes
                ),
                "total_likes": str(self.total_likes)
            },
            "activity_period": {
                "first_tweet": self.first_tweet_date.isoformat() if self.first_tweet_date else None,
                "last_tweet": self.last_tweet_date.isoformat() if self.last_tweet_date else None,
                "active_days": str(active_days),
                "tweets_per_day": str(round(self.total_tweets / active_days, 2) if active_days else 0)
            },
            "content_metrics": {
                "avg_tweet_length": str(round(sum(self.tweet_lengths) / len(self.tweet_lengths), 2) if self.tweet_lengths else 0),
                "top_hashtags": {str(k): str(v) for k, v in dict(self.hashtag_usage.most_common(10)).items()},
                "top_domains": {str(k): str(v) for k, v in dict(self.domains_shared.most_common(10)).items()},
                "top_mentioned_users": {str(k): str(v) for k, v in dict(self.mentioned_users.most_common(10)).items()}
            },
            "engagement_metrics": {
                "total_likes_received": str(sum(self.likes_received.values())),
                "total_retweets_received": str(sum(self.retweets_received.values())),
                "avg_likes_per_tweet": str(round(
                    sum(self.likes_received.values()) / self.total_tweets, 2
                ) if self.total_tweets else 0)
            },
            "temporal_patterns": {
                "busiest_hours": {str(k): str(v) for k, v in dict(self.tweets_by_hour.most_common(5)).items()},
                "busiest_days": {str(k): str(v) for k, v in dict(self.tweets_by_dow.most_common()).items()},
                "tweets_by_month": {str(k): str(v) for k, v in dict(sorted(self.tweets_by_month.items())).items()}
            },
            "tweet_types": {
                "regular": str(self.total_tweets),
                "community": str(self.total_community_tweets),
                "notes": str(self.total_note_tweets),
                "likes": str(self.total_likes)
            },
            "media_stats": {
                "counts": {
                    str(k): str(v) 
                    for k, v in self.media_counts.most_common()
                },
                "sample_urls": {
                    media_type: [
                        m.get('media_url_https', m.get('media_url', ''))
                        for m in media_list[:5]
                    ]
                    for media_type, media_list in self.media_by_type.items()
                }
            },
            "url_stats": {
                "top_domains": {
                    str(k): str(v)
                    for k, v in self.urls_by_domain.most_common(10)
                }
            },
            "community_stats": {
                "total_communities": str(len(self.communities)),
                "top_communities": {
                    str(k): str(v)
                    for k, v in self.communities.most_common(10)
                }
            }
        }

        return summary

@dataclass
class ExportConfig:
    """Configuration for stats export"""
    formats: Set[str] = field(default_factory=lambda: {'json', 'markdown'})
    media_dir: Optional[Path] = None
    
class StatsManager:
    """Collects and manages statistics about processed archives."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.current_stats = {}
        
    def process_tweet(self, tweet: CanonicalTweet) -> None:
        """Process a tweet and update statistics."""
        username = tweet.screen_name
        if username not in self.current_stats:
            self.current_stats[username] = {
                "tweet_count": 0,
                "reply_count": 0,
                "retweet_count": 0,
                "quote_count": 0,
                "like_count": 0,
                "media_count": 0,
                "hashtag_count": 0,
                "mention_count": 0,
                "url_count": 0,
                "first_tweet_at": None,
                "last_tweet_at": None,
                "hashtags": set(),
                "mentioned_users": set(),
                "domains": set()
            }
            
        stats = self.current_stats[username]
        stats["tweet_count"] += 1
        
        if tweet.reply_to_tweet_id:
            stats["reply_count"] += 1
            
        if tweet.metadata.is_retweet:
            stats["retweet_count"] += 1
            
        if tweet.metadata.quoted_tweet_id:
            stats["quote_count"] += 1
            
        if tweet.source_type == "like":
            stats["like_count"] += 1
            
        stats["media_count"] += len(tweet.media_urls)
        stats["hashtag_count"] += len(tweet.metadata.hashtags)
        stats["mention_count"] += len(tweet.metadata.mentioned_users)
        stats["url_count"] += len(tweet.metadata.urls)
        
        # Update timestamp ranges
        if tweet.created_at:
            if not stats["first_tweet_at"] or tweet.created_at < stats["first_tweet_at"]:
                stats["first_tweet_at"] = tweet.created_at
            if not stats["last_tweet_at"] or tweet.created_at > stats["last_tweet_at"]:
                stats["last_tweet_at"] = tweet.created_at
                
        # Update sets
        stats["hashtags"].update(tweet.metadata.hashtags)
        stats["mentioned_users"].update(tweet.metadata.mentioned_users)
        stats["domains"].update(
            self._extract_domain(url) for url in tweet.metadata.urls
        )
        
    def save_stats(self, username: str) -> None:
        """Save statistics for a user to a file."""
        if username not in self.current_stats:
            return
            
        stats = self.current_stats[username]
        # Convert sets to lists for JSON serialization
        stats_copy = stats.copy()
        stats_copy["hashtags"] = list(stats["hashtags"])
        stats_copy["mentioned_users"] = list(stats["mentioned_users"])
        stats_copy["domains"] = list(stats["domains"])
        
        # Convert timestamps to ISO format
        if stats_copy["first_tweet_at"]:
            stats_copy["first_tweet_at"] = stats_copy["first_tweet_at"].isoformat()
        if stats_copy["last_tweet_at"]:
            stats_copy["last_tweet_at"] = stats_copy["last_tweet_at"].isoformat()
            
        output_file = self.output_dir / f"{username}_archive_stats.json"
        with output_file.open('w') as f:
            json.dump(stats_copy, f, indent=2)
            
    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc
        except:
            return url

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

    def export_analysis(self, config: ExportConfig) -> None:
        """Export analysis in multiple formats"""
        if 'markdown' in config.formats:
            self._export_markdown(config)
            
        if 'json' in config.formats:
            self._export_json(config)
    
    def _export_markdown(self, config: ExportConfig) -> None:
        """Export analysis as markdown"""
        for archive_name, stats in self.archive_stats.items():
            output_path = self.output_dir / f"{archive_name}_analysis.md"
            
            with output_path.open('w') as f:
                f.write(f"# Analysis: {archive_name}\n\n")
                
                # Basic stats
                f.write("## Overview\n")
                f.write(f"Total Tweets: {stats.total_tweets}\n")
                f.write(f"Conversation Threads: {len(stats.conversation_participants)}\n")
                
                # Add media gallery if configured
                if config.media_dir:
                    f.write("\n## Media Gallery\n")
                    for tweet in stats.tweets_with_media:
                        for media_file in tweet.media_files:
                            f.write(f"![{media_file}]({media_file})\n")
