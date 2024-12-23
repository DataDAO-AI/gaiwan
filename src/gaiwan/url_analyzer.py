from pathlib import Path
import re
from urllib.parse import urlparse
from collections import Counter
from typing import Dict, List, Set, Optional
import orjson
from tqdm import tqdm
import logging
from itertools import groupby
from operator import itemgetter
import pandas as pd
from datetime import datetime, timezone
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import functools

logger = logging.getLogger(__name__)

"""URL Analyzer for Twitter Archives.

This module analyzes URLs found in Twitter archive data, providing insights into link sharing
patterns across the community. It handles URL shorteners (t.co, bit.ly, etc.), normalizes
domains, and creates a queryable DataFrame of all URLs.

Usage:
    Basic analysis:
        python -m gaiwan.url_analyzer archives

    Force reanalysis of all archives:
        python -m gaiwan.url_analyzer archives --force

    Save to specific output file:
        python -m gaiwan.url_analyzer archives --output my_analysis.parquet

    Enable debug logging:
        python -m gaiwan.url_analyzer archives --debug

Features:
    - Resolves shortened URLs (t.co, bit.ly, buff.ly, etc.)
    - Normalizes domains (e.g., youtu.be -> youtube.com)
    - Incremental processing (only analyzes new archives)
    - Creates backups of existing analysis
    - Produces a pandas DataFrame with detailed URL data

Output DataFrame columns:
    - username: Who shared the URL
    - tweet_id: Source tweet ID
    - tweet_created_at: When the URL was shared
    - url: Full URL
    - domain: Normalized domain name
    - raw_domain: Original domain before normalization
    - protocol: URL protocol (http/https)
    - path: URL path
    - query: Query parameters
    - fragment: URL fragment
    - is_resolved: Whether URL was expanded from a shortener

Example pandas queries:
    # Load the data
    import pandas as pd
    df = pd.read_parquet('urls.parquet')

    # Most shared domains
    df['domain'].value_counts().head(10)

    # URLs by user
    df.groupby('username')['url'].count()

    # YouTube links
    youtube_links = df[df['domain'] == 'youtube.com']

    # URLs over time
    df.set_index('tweet_created_at')['domain'].resample('M').count()
"""

class URLAnalyzer:
    """Analyzes URLs in Twitter archive data.
    
    This class processes Twitter archive files to extract and analyze URLs, handling:
    - URL extraction from tweet text and entities
    - Resolution of shortened URLs (t.co, bit.ly, etc.)
    - Domain normalization (grouping related domains)
    - Creation of analyzable DataFrame
    
    Args:
        archive_dir (Path): Directory containing Twitter archive files
            (expected format: username_archive.json)
    
    Example usage:
        analyzer = URLAnalyzer(Path('archives'))
        df = analyzer.analyze_archives()
        
        # Get URLs from specific archive
        user_df = analyzer.analyze_archive(Path('archives/username_archive.json'))
    
    Domain normalization rules:
        - twitter.com includes x.com and www.twitter.com
        - youtube.com includes youtu.be
        - wikipedia.org includes language variants
        - *.substack.com -> substack.com
        - *.medium.com -> medium.com
        - github.com includes raw.githubusercontent.com
    
    URL shorteners handled:
        - t.co (Twitter)
        - bit.ly
        - buff.ly
        - tinyurl.com
        - ow.ly
        - goo.gl
        - tiny.cc
        - is.gd
    """

    def __init__(self, archive_dir: Path):
        self.archive_dir = archive_dir
        # Improved URL pattern to better match Twitter URLs
        self.url_pattern = re.compile(
            r'https?://(?:(?:www\.)?twitter\.com/[a-zA-Z0-9_]+/status/[0-9]+|'
            r'(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
        )

        # Add domain normalization rules
        self.domain_groups = {
            'twitter.com': ['twitter.com', 'x.com', 'www.twitter.com'],
            'youtube.com': ['youtube.com', 'www.youtube.com', 'youtu.be'],
            'wikipedia.org': ['wikipedia.org', 'en.wikipedia.org', 'fr.wikipedia.org', 'de.wikipedia.org'],
            'substack.com': lambda domain: domain.endswith('.substack.com'),
            'medium.com': lambda domain: domain.endswith('.medium.com'),
            'github.com': ['github.com', 'raw.githubusercontent.com', 'gist.github.com'],
        }

        # Add known URL shorteners
        self.shortener_domains = {
            't.co',
            'bit.ly',
            'buff.ly',
            'tinyurl.com',
            'ow.ly',
            'goo.gl',
            'tiny.cc',
            'is.gd',
        }

        # Set up requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Cache for resolved URLs
        self._url_cache: Dict[str, Optional[str]] = {}

    def normalize_domain(self, domain: str) -> str:
        """Normalize domain names to group related sites."""
        # Remove www. prefix for consistency
        domain = domain.lower().replace('www.', '')
        
        # Check each domain group
        for main_domain, matchers in self.domain_groups.items():
            if callable(matchers):
                # Function matcher for pattern matching (e.g., *.substack.com)
                if matchers(domain):
                    return main_domain
            elif domain in matchers:
                return main_domain
        
        return domain

    @functools.lru_cache(maxsize=10000)
    def resolve_url(self, short_url: str) -> Optional[str]:
        """Resolve a shortened URL by following redirects."""
        if short_url in self._url_cache:
            return self._url_cache[short_url]

        try:
            response = self.session.head(
                short_url, 
                allow_redirects=True,
                timeout=5
            )
            resolved_url = response.url
            self._url_cache[short_url] = resolved_url
            return resolved_url
        except Exception as e:
            logger.debug(f"Failed to resolve {short_url}: {e}")
            self._url_cache[short_url] = None
            return None

    def should_resolve_url(self, url: str) -> bool:
        """Check if URL should be resolved."""
        try:
            parsed = urlparse(url)
            return parsed.netloc in self.shortener_domains
        except Exception:
            return False

    def extract_urls_from_tweet(self, tweet_data: Dict) -> Set[str]:
        """Extract URLs from a tweet object."""
        urls = set()
        
        # Extract from tweet text
        if 'text' in tweet_data:
            urls.update(self.url_pattern.findall(tweet_data['text']))
        
        # Extract from entities if present
        if 'entities' in tweet_data:
            entities = tweet_data['entities']
            if 'urls' in entities:
                for url_entity in entities['urls']:
                    # Use expanded_url if available (Twitter's pre-resolved version)
                    if 'expanded_url' in url_entity:
                        urls.add(url_entity['expanded_url'])
                    elif 'url' in url_entity:
                        short_url = url_entity['url']
                        if self.should_resolve_url(short_url):
                            resolved = self.resolve_url(short_url)
                            if resolved:
                                urls.add(resolved)
                        else:
                            urls.add(short_url)

        return urls

    def analyze_archive(self, archive_path: Path) -> pd.DataFrame:
        """Analyze URLs in a single archive file."""
        try:
            with open(archive_path, 'rb') as f:
                data = orjson.loads(f.read())
            
            url_data = []
            username = archive_path.stem.replace('_archive', '')
            
            # Process tweets section
            for section in ['tweets', 'community-tweet', 'note-tweet']:
                for tweet_data in data.get(section, []):
                    if 'tweet' in tweet_data:
                        tweet = tweet_data['tweet']
                        tweet_id = tweet.get('id_str')
                        created_at = datetime.strptime(
                            tweet.get('created_at', ''), 
                            "%a %b %d %H:%M:%S %z %Y"
                        ) if tweet.get('created_at') else None
                        
                        urls = self.extract_urls_from_tweet(tweet)
                        for url in urls:
                            parsed = urlparse(url)
                            url_data.append({
                                'username': username,
                                'tweet_id': tweet_id,
                                'tweet_created_at': created_at,
                                'url': url,
                                'domain': self.normalize_domain(parsed.netloc),
                                'raw_domain': parsed.netloc,
                                'protocol': parsed.scheme,
                                'path': parsed.path,
                                'query': parsed.query,
                                'fragment': parsed.fragment,
                                'is_resolved': not url.startswith('https://t.co/')
                            })
            
            return pd.DataFrame(url_data)
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return pd.DataFrame()

    def analyze_archives(self) -> pd.DataFrame:
        """Analyze all archives and return a DataFrame."""
        archives = list(self.archive_dir.glob("*_archive.json"))
        logger.info(f"Found {len(archives)} archives to analyze")
        
        # Process each archive and collect DataFrames
        dfs = []
        for archive in tqdm(archives, desc="Analyzing archives"):
            df = self.analyze_archive(archive)
            if not df.empty:
                dfs.append(df)
        
        # Combine all DataFrames
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"\nAnalysis complete. DataFrame shape: {combined_df.shape}")
            return combined_df
        return pd.DataFrame()

def main():
    """Command-line interface for URL analysis.
    
    This function provides a CLI for analyzing URLs in Twitter archives.
    It supports incremental processing, meaning it will only analyze new
    archives not present in existing output file.
    
    Arguments:
        archive_dir: Directory containing Twitter archives
        --debug: Enable debug logging
        --output: Custom output file path (default: urls.parquet)
        --force: Force reanalysis of all archives
    
    The function will:
    1. Check for existing analysis file (urls.parquet)
    2. Load and process only new archives
    3. Merge results with existing data
    4. Create backup of existing analysis
    5. Save updated analysis
    6. Print summary statistics
    
    Output format is Parquet by default, optimized for further analysis
    with pandas.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Analyze URLs in Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument('--output', type=Path, help="Save DataFrame to CSV/Parquet file")
    parser.add_argument('--force', action='store_true', help="Force reanalysis of all archives")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    output_file = args.output or Path('urls.parquet')
    existing_df = None
    processed_archives = set()

    # Load existing data if available
    if not args.force and output_file.exists():
        try:
            existing_df = pd.read_parquet(output_file)
            processed_archives = set(existing_df['username'].unique())
            logger.info(f"Loaded existing data with {len(existing_df)} URLs from {len(processed_archives)} archives")
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
            existing_df = None

    analyzer = URLAnalyzer(args.archive_dir)
    archives = list(analyzer.archive_dir.glob("*_archive.json"))
    
    # Filter out already processed archives
    if existing_df is not None and not args.force:
        new_archives = [
            a for a in archives 
            if a.stem.replace('_archive', '') not in processed_archives
        ]
        if not new_archives:
            logger.info("No new archives to process")
            return
        logger.info(f"Found {len(new_archives)} new archives to process")
        archives = new_archives

    # Analyze new archives
    df = analyzer.analyze_archives()
    
    if df.empty:
        logger.error("No data found in new archives")
        return

    # Merge with existing data if available
    if existing_df is not None and not args.force:
        df = pd.concat([existing_df, df], ignore_index=True)
        logger.info(f"Merged new data. Total URLs: {len(df)}")

    # Print summary statistics
    print("\nOverall Statistics:")
    print(f"Archives analyzed: {df['username'].nunique()}")
    print(f"Total URLs found: {len(df)}")
    print(f"Unique URLs: {df['url'].nunique()}")
    
    print("\nTop 20 domains:")
    print(df['domain'].value_counts().head(20))
    
    print("\nProtocols used:")
    print(df['protocol'].value_counts())

    # Save DataFrame
    if args.output:
        output_path = args.output
    else:
        # Create backup of existing file
        if output_file.exists():
            backup_path = output_file.with_name(
                f"urls_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
            )
            output_file.rename(backup_path)
            logger.info(f"Created backup at {backup_path}")
        output_path = output_file

    df.to_parquet(output_path)
    logger.info(f"Saved data to {output_path}")

if __name__ == '__main__':
    main() 