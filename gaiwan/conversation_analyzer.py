"""Build and analyze conversation threads from normalized tweet data."""

import argparse
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from .models import CanonicalTweet, TweetMetadata, MixPRConfig, RetrievalResult
from .mixpr import MixPR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ConversationThread:
    """Represents a complete conversation thread."""
    root_tweet_id: str
    tweets: List[CanonicalTweet]
    reply_structure: Dict[str, List[str]]  # parent_id -> child_ids

    def to_dict(self) -> dict:
        """Convert thread to dictionary for serialization."""
        return {
            "root_tweet_id": self.root_tweet_id,
            "tweets": [
                {
                    "id": t.id,
                    "author_id": t.author_id,
                    "text": t.text,
                    "created_at": t.created_at.isoformat() if t.created_at else None,
                    "level": self._get_tweet_level(t.id)
                }
                for t in sorted(
                    self.tweets,
                    key=lambda x: x.created_at or datetime.min
                )
            ],
            "reply_structure": self.reply_structure
        }

    def _get_tweet_level(self, tweet_id: str) -> int:
        """Calculate the depth level of a tweet in the conversation."""
        level = 0
        current_id = tweet_id
        while True:
            parent_found = False
            for parent_id, children in self.reply_structure.items():
                if current_id in children:
                    current_id = parent_id
                    level += 1
                    parent_found = True
                    break
            if not parent_found:
                break
        return level

@dataclass
class SearchCriteria:
    """Encapsulates conversation search criteria."""
    contains_words: Set[str] = field(default_factory=set)
    exact_phrases: Set[str] = field(default_factory=set)
    exclude_words: Set[str] = field(default_factory=set)
    hashtags: Set[str] = field(default_factory=set)
    from_accounts: Set[str] = field(default_factory=set)
    to_accounts: Set[str] = field(default_factory=set)
    mentions: Set[str] = field(default_factory=set)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    min_replies: Optional[int] = None
    min_likes: Optional[int] = None
    has_links: Optional[bool] = None

    @classmethod
    def from_query(cls, query: str) -> 'SearchCriteria':
        """Parse Twitter-style search query into SearchCriteria."""
        criteria = cls()
        terms = query.split()
        i = 0
        while i < len(terms):
            term = terms[i]

            if term.startswith('"'):
                phrase_terms = [term.strip('"')]
                i += 1
                while i < len(terms) and not terms[i].endswith('"'):
                    phrase_terms.append(terms[i])
                    i += 1
                if i < len(terms):
                    phrase_terms.append(terms[i].strip('"'))
                criteria.exact_phrases.add(' '.join(phrase_terms))

            elif term.startswith('from:'):
                criteria.from_accounts.add(term[5:])
            elif term.startswith('to:'):
                criteria.to_accounts.add(term[3:])
            elif term.startswith('@'):
                criteria.mentions.add(term[1:])
            elif term.startswith('#'):
                criteria.hashtags.add(term[1:])
            elif term.startswith('-'):
                criteria.exclude_words.add(term[1:])
            elif term == 'filter:links':
                criteria.has_links = True
            else:
                criteria.contains_words.add(term)

            i += 1

        return criteria

class ConversationAnalyzer:
    """Builds and analyzes conversation threads from normalized tweet data."""

    def __init__(self, tweets_file: Path, replies_file: Path):
        """Initialize analyzer with data files."""
        self.tweets: Dict[str, CanonicalTweet] = {}
        self.reply_edges: Dict[str, List[str]] = defaultdict(list)
        self._load_data(tweets_file, replies_file)

        # Initialize MixPR
        self.mixpr = MixPR(MixPRConfig())
        self.mixpr.fit(list(self.tweets.values()))

    def _load_data(self, tweets_file: Path, replies_file: Path) -> None:
        """Load normalized tweet and reply data."""
        logger.info("Loading tweets from %s", tweets_file)
        with tweets_file.open('rb') as f:
            for line in f:
                if line.strip():
                    tweet_data = json.loads(line)
                    metadata = TweetMetadata(
                        mentioned_users=set(tweet_data['metadata']['mentioned_users']),
                        hashtags=set(tweet_data['metadata']['hashtags']),
                        urls=set(tweet_data['metadata']['urls']),
                        quoted_tweet_id=tweet_data['metadata']['quoted_tweet_id'],
                        is_retweet=tweet_data['metadata']['is_retweet'],
                        retweet_of_id=tweet_data['metadata']['retweet_of_id']
                    )

                    created_at = (
                        datetime.fromisoformat(tweet_data['created_at'])
                        if tweet_data.get('created_at')
                        else None
                    )

                    tweet = CanonicalTweet(
                        id=tweet_data['id'],
                        text=tweet_data['text'],
                        author_id=tweet_data.get('author_id'),
                        created_at=created_at,
                        reply_to_tweet_id=tweet_data.get('reply_to_tweet_id'),
                        liked_by=set(tweet_data.get('liked_by', [])),
                        source_type=tweet_data.get('source_type', 'tweet'),
                        metadata=metadata
                    )
                    self.tweets[tweet.id] = tweet

        logger.info("Loading reply structure from %s", replies_file)
        with replies_file.open('rb') as f:
            for line in f:
                if line.strip():
                    edge = json.loads(line)
                    self.reply_edges[edge['parent_id']].append(edge['child_id'])

    def _find_root_tweet(self, tweet_id: str) -> str:
        """Find the root tweet of a conversation."""
        current_id = tweet_id
        while True:
            tweet = self.tweets.get(current_id)
            if not tweet or not tweet.reply_to_tweet_id:
                return current_id
            current_id = tweet.reply_to_tweet_id

    def get_conversation(self, tweet_id: str) -> Optional[ConversationThread]:
        """Build complete conversation thread from any tweet in the thread."""
        root_id = self._find_root_tweet(tweet_id)
        if root_id not in self.tweets:
            return None

        # Collect all tweets in thread
        thread_tweets = set()
        to_process = {root_id}

        while to_process:
            current_id = to_process.pop()
            if current_id in thread_tweets or current_id not in self.tweets:
                continue

            thread_tweets.add(current_id)
            to_process.update(self.reply_edges[current_id])

        # Build reply structure
        reply_structure = {
            tweet_id: children
            for tweet_id, children in self.reply_edges.items()
            if tweet_id in thread_tweets
        }

        return ConversationThread(
            root_tweet_id=root_id,
            tweets=[self.tweets[tid] for tid in thread_tweets],
            reply_structure=reply_structure
        )

    def find_related_tweets(
        self,
        tweet_id: str,
        k: int = 10,
        mode: Optional[str] = None
    ) -> List[RetrievalResult]:
        """Find related tweets using MixPR."""
        if tweet_id not in self.tweets:
            return []

        query_tweet = self.tweets[tweet_id]
        return self.mixpr.retrieve(query_tweet, k=k, force_mode=mode)

    def _tweet_matches_criteria(self, tweet: CanonicalTweet, criteria: SearchCriteria) -> bool:
        """Check if tweet matches search criteria."""
        text = tweet.text.lower()

        if criteria.contains_words and not all(
            word.lower() in text
            for word in criteria.contains_words
        ):
            return False

        if criteria.exact_phrases and not all(
            phrase.lower() in text
            for phrase in criteria.exact_phrases
        ):
            return False

        if any(
            word.lower() in text
            for word in criteria.exclude_words
        ):
            return False

        if criteria.hashtags and not any(
            tag in tweet.metadata.hashtags
            for tag in criteria.hashtags
        ):
            return False

        if criteria.from_accounts and tweet.author_id not in criteria.from_accounts:
            return False

        if criteria.mentions and not any(
            mention in tweet.metadata.mentioned_users
            for mention in criteria.mentions
        ):
            return False

        if criteria.start_date and tweet.created_at and tweet.created_at < criteria.start_date:
            return False
        if criteria.end_date and tweet.created_at and tweet.created_at > criteria.end_date:
            return False

        if criteria.min_likes is not None and len(tweet.liked_by) < criteria.min_likes:
            return False

        if criteria.has_links is not None and bool(tweet.metadata.urls) != criteria.has_links:
            return False

        return True

    def search_conversations(self, criteria: SearchCriteria) -> List[ConversationThread]:
        """Search for conversations matching criteria."""
        matching_conversations = []
        processed_roots = set()

        # Find tweets matching criteria
        matching_tweets = set()
        for tweet in self.tweets.values():
            if self._tweet_matches_criteria(tweet, criteria):
                matching_tweets.add(tweet.id)

        # Build complete threads for matching tweets
        for tweet_id in matching_tweets:
            root_id = self._find_root_tweet(tweet_id)
            if root_id not in processed_roots:
                thread = self.get_conversation(root_id)
                if thread:
                    matching_conversations.append(thread)
                    processed_roots.add(root_id)

        return matching_conversations

def main():
    """Search and analyze conversation threads."""
    parser = argparse.ArgumentParser()
    parser.add_argument('tweets_file', type=Path, help="Path to canonical tweets file")
    parser.add_argument('replies_file', type=Path, help="Path to reply edges file")
    parser.add_argument('--search', help="Search query in Twitter format")
    parser.add_argument(
        '--output',
        type=Path,
        help="Output file for matching conversations"
    )
    parser.add_argument(
        '--related',
        help="Find related tweets for given tweet ID"
    )
    parser.add_argument(
        '--mode',
        choices=['local', 'global'],
        help="Force specific retrieval mode"
    )
    parser.add_argument(
        '--k',
        type=int,
        default=10,
        help="Number of related tweets to retrieve"
    )

    args = parser.parse_args()
    analyzer = ConversationAnalyzer(args.tweets_file, args.replies_file)

    if args.search:
        criteria = SearchCriteria.from_query(args.search)
        conversations = analyzer.search_conversations(criteria)

        logger.info("Found %d matching conversations", len(conversations))

        if args.output:
            with args.output.open('wb') as f:
                for conv in conversations:
                    f.write(json.dumps(conv.to_dict()).encode() + b'\n')
        else:
            for conv in conversations:
                print("\nConversation:")
                for tweet in sorted(
                    conv.tweets,
                    key=lambda x: x.created_at or datetime.min
                ):
                    print(f"@{tweet.author_id}: {tweet.text}")

    if args.related:
        results = analyzer.find_related_tweets(
            args.related,
            k=args.k,
            mode=args.mode
        )
        print(f"\nRelated tweets for {args.related}:")
        for i, result in enumerate(results, 1):
            print(f"\n{i}. Score: {result.score:.3f}")
            print(f"Text: {result.tweet.text}")
            print(f"Author: {result.tweet.author_id}")
            print(f"Time: {result.tweet.created_at}")

if __name__ == '__main__':
    main()
