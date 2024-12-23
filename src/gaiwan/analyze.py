"""Analyze tweet patterns to improve reconstruction."""

import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Counter
from collections import defaultdict
import pyarrow.parquet as pq
import pandas as pd
import networkx as nx
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class ThreadPattern:
    """Pattern analysis for a conversation thread."""
    root_id: str
    depth: int
    participant_count: int
    mention_patterns: Dict[str, Counter]  # tweet -> {pattern: count}
    orphaned_positions: Set[str]  # tweet IDs we're missing
    reconstruction_hints: Dict[str, Dict]  # tweet_id -> potential data

@dataclass
class ReconstructionPattern:
    """Analysis of how we might reconstruct a tweet."""
    tweet_id: str
    available_texts: List[str]  # from likes/quotes
    potential_authors: List[tuple[str, float]]  # (username, confidence)
    mention_context: Dict[str, List[str]]  # upstream/downstream mentions
    thread_position: Optional[str]  # root/middle/leaf
    confidence_factors: Dict[str, float]

def analyze_thread_patterns(
    tweets_file: Path,
    orphaned_file: Path,
    min_thread_size: int = 3
) -> Dict[str, List[ThreadPattern]]:
    """Analyze conversation patterns focusing on reconstructible tweets."""
    
    # Load data - use existing Parquet files
    tweets_df = pq.read_table(tweets_file).to_pandas()
    orphaned_df = pq.read_table(orphaned_file).to_pandas()
    
    # Build reply graph from existing reply_ids
    G = nx.DiGraph()
    for _, tweet in tweets_df.iterrows():
        # Use the reply_ids we already collected
        if tweet['reply_ids']:
            for reply_id in tweet['reply_ids']:
                G.add_edge(tweet['id'], reply_id)
        # Also add the parent relationship
        if tweet['in_reply_to_status_id']:
            G.add_edge(tweet['in_reply_to_status_id'], tweet['id'])
    
    # Find significant threads
    threads = []
    for component in nx.weakly_connected_components(G):
        if len(component) >= min_thread_size:
            # Find root
            root = [n for n in component if G.in_degree(n) == 0][0]
            
            # Analyze thread
            thread = analyze_single_thread(
                root, 
                component,
                G,
                tweets_df,
                orphaned_df
            )
            threads.append(thread)
    
    return threads

def analyze_single_thread(
    root_id: str,
    thread_ids: Set[str],
    reply_graph: nx.DiGraph,
    tweets_df: pd.DataFrame,
    orphaned_df: pd.DataFrame
) -> ThreadPattern:
    """Analyze patterns in a single thread."""
    
    # Get basic metrics
    depth = max(nx.shortest_path_length(reply_graph, root_id).values())
    participants = set(
        tweets_df[tweets_df['id'].isin(thread_ids)]['author_username']
    )
    
    # Analyze mention patterns
    mention_patterns = defaultdict(Counter)
    for _, tweet in tweets_df[tweets_df['id'].isin(thread_ids)].iterrows():
        pattern = analyze_mention_pattern(
            tweet,
            tweets_df,
            reply_graph
        )
        mention_patterns[tweet['id']].update(pattern)
    
    # Find orphaned positions
    orphaned = set()
    for tweet_id in thread_ids:
        if tweet_id not in tweets_df['id'].values:
            orphaned.add(tweet_id)
    
    # Gather reconstruction hints
    hints = {}
    for tweet_id in orphaned:
        if tweet_id in orphaned_df['tweet_id'].values:
            orphan = orphaned_df[orphaned_df['tweet_id'] == tweet_id].iloc[0]
            hints[tweet_id] = gather_reconstruction_hints(
                orphan,
                tweets_df,
                reply_graph
            )
    
    return ThreadPattern(
        root_id=root_id,
        depth=depth,
        participant_count=len(participants),
        mention_patterns=dict(mention_patterns),
        orphaned_positions=orphaned,
        reconstruction_hints=hints
    )

def analyze_mention_pattern(
    tweet: pd.Series,
    tweets_df: pd.DataFrame,
    reply_graph: nx.DiGraph
) -> Counter:
    """Analyze how mentions are used in a tweet."""
    patterns = Counter()
    
    # Get parent tweet if exists
    parent_id = tweet['in_reply_to_status_id']
    if parent_id:
        parent = tweets_df[tweets_df['id'] == parent_id].iloc[0]
        parent_author = parent['author_username']
        
        # Check mention patterns
        mentions = set(m['screen_name'] for m in tweet['entities']['user_mentions'])
        if parent_author in mentions:
            patterns['mentions_parent'] += 1
            if len(mentions) == 1:
                patterns['only_mentions_parent'] += 1
        
        # Look for conversation patterns
        if len(mentions - {parent_author}) > 0:
            patterns['adds_new_participants'] += 1
    
    return patterns

def gather_reconstruction_hints(
    orphan: pd.Series,
    tweets_df: pd.DataFrame,
    reply_graph: nx.DiGraph
) -> Dict:
    """Gather hints for reconstructing an orphaned tweet."""
    tweet_id = orphan['tweet_id']
    hints = {
        'texts': [],  # Available text versions
        'potential_authors': Counter(),  # username -> confidence score
        'context_clues': []
    }
    
    # Get available texts
    if orphan['text']:
        hints['texts'].append(orphan['text'])
    
    # Analyze reply context
    children = list(reply_graph.successors(tweet_id))
    for child_id in children:
        child = tweets_df[tweets_df['id'] == child_id].iloc[0]
        
        # Analyze mentions in replies
        mentions = child['entities']['user_mentions']
        if len(mentions) == 1:
            # Single mention likely the parent author
            hints['potential_authors'][mentions[0]['screen_name']] += 3
        elif len(mentions) > 1:
            # First mention often parent author
            hints['potential_authors'][mentions[0]['screen_name']] += 1
        
        # Look for quoted content
        if 'RT @' in child['text']:
            hints['texts'].append(extract_retweet_text(child['text']))
            
        hints['context_clues'].append({
            'reply_text': child['text'],
            'reply_author': child['author_username']
        })
    
    return hints

def extract_retweet_text(text: str) -> str:
    """Extract original text from a retweet."""
    if 'RT @' not in text:
        return text
    
    # Find the actual content after "RT @username:"
    import re
    match = re.match(r'RT @\w+: (.*)', text)
    if match:
        return match.group(1)
    return text

def analyze_reconstruction_confidence(
    threads: List[ThreadPattern]
) -> Dict[str, ReconstructionPattern]:
    """Analyze confidence in reconstruction possibilities."""
    patterns = {}
    
    for thread in threads:
        for tweet_id in thread.orphaned_positions:
            if tweet_id not in thread.reconstruction_hints:
                continue
                
            hints = thread.reconstruction_hints[tweet_id]
            
            # Analyze text confidence
            texts = hints['texts']
            text_confidence = len(set(texts)) / len(texts) if texts else 0
            
            # Analyze author confidence
            total_author_weight = sum(hints['potential_authors'].values())
            potential_authors = [
                (author, weight/total_author_weight)
                for author, weight in hints['potential_authors'].most_common()
            ] if total_author_weight else []
            
            # Position in thread
            position = None
            if tweet_id == thread.root_id:
                position = 'root'
            elif not list(reply_graph.successors(tweet_id)):
                position = 'leaf'
            else:
                position = 'middle'
            
            patterns[tweet_id] = ReconstructionPattern(
                tweet_id=tweet_id,
                available_texts=texts,
                potential_authors=potential_authors,
                mention_context={
                    'upstream': [],  # TODO: Add context
                    'downstream': []
                },
                thread_position=position,
                confidence_factors={
                    'text': text_confidence,
                    'author': potential_authors[0][1] if potential_authors else 0,
                    'context': len(hints['context_clues']) / 5  # Normalize
                }
            )
    
    return patterns

def main():
    """CLI entry point for analysis."""
    import argparse
    parser = argparse.ArgumentParser(description="Analyze tweet patterns")
    parser.add_argument('tweets', type=Path, help="Tweets parquet file")
    parser.add_argument('orphaned', type=Path, help="Orphaned tweets parquet file")
    parser.add_argument('--min-thread', type=int, default=3,
                       help="Minimum thread size to analyze")
    args = parser.parse_args()
    
    # Run analysis
    threads = analyze_thread_patterns(
        args.tweets,
        args.orphaned,
        args.min_thread
    )
    
    # Analyze reconstruction confidence
    patterns = analyze_reconstruction_confidence(threads)
    
    # Report findings
    print(f"\nAnalyzed {len(threads)} threads")
    print(f"Found {len(patterns)} reconstructible tweets")
    
    # Show some statistics
    confidences = [
        sum(p.confidence_factors.values()) / len(p.confidence_factors)
        for p in patterns.values()
    ]
    print(f"\nConfidence scores:")
    print(f"  Mean: {sum(confidences) / len(confidences):.2f}")
    print(f"  High confidence (>0.8): {sum(1 for c in confidences if c > 0.8)}")
    
    # Show some examples
    print("\nSample reconstructible tweets:")
    for tweet_id, pattern in list(sorted(
        patterns.items(),
        key=lambda x: sum(x[1].confidence_factors.values()),
        reverse=True
    ))[:5]:
        print(f"\nTweet {tweet_id}:")
        print(f"  Position: {pattern.thread_position}")
        print(f"  Confidence factors: {pattern.confidence_factors}")
        if pattern.potential_authors:
            print(f"  Most likely author: {pattern.potential_authors[0]}")
        if pattern.available_texts:
            print(f"  Sample text: {pattern.available_texts[0][:100]}...")

if __name__ == '__main__':
    main() 