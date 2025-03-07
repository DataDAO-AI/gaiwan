from pathlib import Path
import re
from urllib.parse import urlparse
from collections import Counter
from typing import Dict, List, Set, Optional, Any
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
    - Resolves shortened URLs (t.co, bit.ly, etc.)
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

class PageMetadata:
    """Container for webpage metadata and fetch status."""
    
    def __init__(self, url: str):
        self.url = url
        self.title: Optional[str] = None
        self.fetch_status: str = 'not_attempted'  # not_attempted, success, failed, skipped
        self.fetch_error: Optional[str] = None
        self.content_type: Optional[str] = None
        self.last_fetch_time: Optional[datetime] = None
        
        # Extensible metadata fields (can be expanded later)
        self.metadata: Dict[str, Any] = {
            'description': None,  # For future meta description
            'keywords': None,     # For future meta keywords
            'og_title': None,     # For future OpenGraph title
            'og_description': None,  # For future OpenGraph description
            # Add more metadata fields as needed
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary for DataFrame storage."""
        return {
            'title': self.title,
            'fetch_status': self.fetch_status,
            'fetch_error': self.fetch_error,
            'content_type': self.content_type,
            'last_fetch_time': self.last_fetch_time,
            **self.metadata
        }

    def mark_skipped(self, reason: str) -> None:
        """Mark URL as skipped with a reason."""
        self.fetch_status = 'skipped'
        self.fetch_error = reason
        self.last_fetch_time = datetime.now(timezone.utc)

    def mark_failed(self, error: str) -> None:
        """Mark URL as failed with error details."""
        self.fetch_status = 'failed'
        self.fetch_error = error
        self.last_fetch_time = datetime.now(timezone.utc)

    def mark_success(self, content_type: str) -> None:
        """Mark URL as successfully processed."""
        self.fetch_status = 'success'
        self.content_type = content_type
        self.fetch_error = None
        self.last_fetch_time = datetime.now(timezone.utc)

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
            'twitter.com': ['twitter.com', 'x.com', 'www.twitter.com', 'm.twitter.com'],
            'youtube.com': ['youtube.com', 'www.youtube.com', 'youtu.be', 'm.youtube.com'],
            'wikipedia.org': [
                'wikipedia.org', 
                'en.wikipedia.org', 'fr.wikipedia.org', 'de.wikipedia.org',
                'en.m.wikipedia.org', 'fr.m.wikipedia.org', 'de.m.wikipedia.org',
                'm.wikipedia.org'
            ],
            'substack.com': lambda domain: domain.endswith('.substack.com'),
            'medium.com': lambda domain: domain.endswith('.medium.com'),
            'github.com': ['github.com', 'raw.githubusercontent.com', 'gist.github.com', 'm.github.com'],
            'deprecated_links': lambda domain: domain in self.shortener_domains,
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
        
        # Cache for resolved URLs and metadata
        self._url_cache: Dict[str, Optional[str]] = {}
        self._metadata_cache: Dict[str, 'PageMetadata'] = {}
        
        # Set a reasonable timeout for requests
        self.timeout = 10
        
        # Add common headers to appear more like a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def normalize_domain(self, domain: str) -> str:
        """Normalize domain names to group related sites."""
        # Remove www. prefix for consistency
        domain = domain.lower().replace('www.', '')
        
        # Handle mobile domains by removing 'm.' prefix if it exists
        parts = domain.split('.')
        if 'm' in parts:
            m_index = parts.index('m')
            # Only remove if it's a subdomain (not the main domain)
            if m_index < len(parts) - 2:
                parts.pop(m_index)
                domain = '.'.join(parts)
        
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
                            logger.debug(f"Attempting to resolve shortened URL: {short_url}")
                            resolved = self.resolve_url(short_url)
                            if resolved:
                                logger.debug(f"Successfully resolved {short_url} -> {resolved}")
                                urls.add(resolved)
                            else:
                                logger.debug(f"Failed to resolve shortened URL: {short_url}")
                                urls.add(short_url)  # Keep the original shortened URL
                        else:
                            urls.add(short_url)

        return urls

    def get_page_metadata(self, url: str) -> 'PageMetadata':
        """Fetch and extract metadata from a webpage.
        
        Args:
            url: The URL to fetch metadata from
            
        Returns:
            PageMetadata object containing the results
        """
        if url in self._metadata_cache:
            return self._metadata_cache[url]
            
        metadata = PageMetadata(url)
        
        try:
            # Don't try to get metadata from certain file types
            if any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip']):
                metadata.mark_skipped(f"Skipping media file")
                self._metadata_cache[url] = metadata
                return metadata

            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            # Check if it's HTML content
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                metadata.mark_skipped(f"Non-HTML content: {content_type}")
                self._metadata_cache[url] = metadata
                return metadata
            
            # Read just enough of the response to find metadata
            content = ''
            for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
                content += chunk
                if '</head>' in content.lower():
                    break
                if len(content) > 100000:  # Don't read more than ~100KB
                    break
            
            # Extract title
            title_match = re.search(r'<title[^>]*>(.*?)</title>', content, re.IGNORECASE | re.DOTALL)
            if title_match:
                metadata.title = ' '.join(title_match.group(1).strip().split())
            
            metadata.mark_success(content_type)
            self._metadata_cache[url] = metadata
            logger.debug(f"Successfully fetched metadata for {url}")
            return metadata
                
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            metadata.mark_failed(error_msg)
            logger.debug(f"Failed to fetch metadata for {url}: {error_msg}")
            self._metadata_cache[url] = metadata
            return metadata

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
                            raw_domain = parsed.netloc
                            # If it's a shortened URL that failed to resolve, mark as unresolved
                            is_resolved = not (raw_domain in self.shortener_domains)
                            
                            # Check if it's a Twitter internal link
                            is_twitter_internal = bool(re.match(
                                r'https?://(?:(?:www\.|m\.)?twitter\.com|x\.com)/\w+/status/',
                                url
                            ))
                            
                            # Get the page metadata if:
                            # 1. It's not a shortened URL or was resolved successfully
                            # 2. It's not a Twitter internal link
                            metadata = None
                            if is_resolved and not is_twitter_internal:
                                metadata = self.get_page_metadata(url)
                            elif is_twitter_internal:
                                # Create metadata object but mark as skipped for Twitter internal links
                                metadata = PageMetadata(url)
                                metadata.mark_skipped("Twitter internal link")
                            
                            url_data.append({
                                'username': username,
                                'tweet_id': tweet_id,
                                'tweet_created_at': created_at,
                                'url': url,
                                'domain': self.normalize_domain(parsed.netloc),
                                'raw_domain': raw_domain,
                                'protocol': parsed.scheme,
                                'path': parsed.path,
                                'query': parsed.query,
                                'fragment': parsed.fragment,
                                'is_resolved': is_resolved,
                                **(metadata.to_dict() if metadata else {
                                    'title': None,
                                    'fetch_status': 'not_attempted',
                                    'fetch_error': None,
                                    'content_type': None,
                                    'last_fetch_time': None
                                })
                            })
            
            return pd.DataFrame(url_data)
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return pd.DataFrame()

    def analyze_archives(self) -> pd.DataFrame:
        """Analyze URLs across all archives in the directory."""
        dfs = []
        archives = list(self.archive_dir.glob("*_archive.json"))
        
        # Configure logging to work with tqdm
        # Create a custom logger that writes above the progress bars
        class TqdmLoggingHandler(logging.Handler):
            def emit(self, record):
                try:
                    msg = self.format(record)
                    tqdm.write(msg)
                    self.flush()
                except Exception:
                    self.handleError(record)
        
        # Replace all handlers with our custom handler
        logger = logging.getLogger()
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        logger.addHandler(TqdmLoggingHandler())
        
        # Main progress bar for archives
        with tqdm(total=len(archives), desc="Analyzing archives", position=0) as archive_pbar:
            for archive_path in archives:
                username = archive_path.stem.replace('_archive', '')
                archive_pbar.set_description(f"Analyzing archive: {username}")
                
                # Extract tweet count first to set up inner progress bar
                try:
                    with open(archive_path, 'rb') as f:
                        data = orjson.loads(f.read())
                    tweets = data.get('tweets', [])
                    
                    # Inner progress bar for tweets/URLs within the current archive
                    # Position=1 ensures it appears below the archive progress bar
                    with tqdm(total=len(tweets), desc=f"Processing tweets", position=1, leave=True) as tweet_pbar:
                        df = self._analyze_archive_with_progress(archive_path, tweet_pbar)
                        if not df.empty:
                            dfs.append(df)
                except Exception as e:
                    logger.error(f"Error processing {archive_path}: {e}")
                
                # Clear the tweet progress bar after finishing each archive
                print("\033[1A\033[K", end="")  # Move up one line and clear it
                
                archive_pbar.update(1)
        
        # Combine all DataFrames
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"\nAnalysis complete. DataFrame shape: {combined_df.shape}")
            return combined_df
        return pd.DataFrame()

    def _analyze_archive_with_progress(self, archive_path: Path, progress_bar: tqdm) -> pd.DataFrame:
        """Analyze URLs in a single archive file with progress tracking."""
        try:
            with open(archive_path, 'rb') as f:
                data = orjson.loads(f.read())
            
            url_data = []
            username = archive_path.stem.replace('_archive', '')
            tweets = data.get('tweets', [])
            
            # Process tweets section
            for i, tweet_data in enumerate(tweets):
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
                            'fragment': parsed.fragment
                        })
                
                # Update progress after each tweet
                progress_bar.update(1)
                # Update description occasionally to show URL count
                if i % 100 == 0:
                    progress_bar.set_description(f"Processing tweets ({len(url_data)} URLs found)")
            
            return pd.DataFrame(url_data)
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
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

    # Add a file handler for debug logging regardless of console level
    if not args.debug:
        debug_handler = logging.FileHandler('url_resolution.log')
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(debug_handler)

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
            df = existing_df
        else:
            logger.info(f"Found {len(new_archives)} new archives to process")
            archives = new_archives
            # Analyze new archives
            df = analyzer.analyze_archives()
            
            if df.empty:
                logger.error("No data found in new archives")
                return

            # Merge with existing data
            df = pd.concat([existing_df, df], ignore_index=True)
            logger.info(f"Merged new data. Total URLs: {len(df)}")
    else:
        # Analyze all archives
        df = analyzer.analyze_archives()
        
        if df.empty:
            logger.error("No data found in archives")
            return

    # Print summary statistics
    print("\nOverall Statistics:")
    print(f"Archives analyzed: {df['username'].nunique()}")
    print(f"Total URLs found: {len(df)}")
    print(f"Unique URLs: {df['url'].nunique()}")
    
    # Metadata fetch statistics
    print("\nMetadata Fetch Statistics:")
    fetch_stats = df['fetch_status'].value_counts()
    print(fetch_stats)
    
    if 'failed' in fetch_stats:
        print("\nTop failure reasons:")
        print(df[df['fetch_status'] == 'failed']['fetch_error'].value_counts().head())
    
    # Create masks for different categories
    unresolved_mask = (~df['is_resolved']) & (df['raw_domain'].isin(analyzer.shortener_domains))
    twitter_internal_mask = df['url'].str.contains(r'https?://(?:(?:www\.|m\.)?twitter\.com|x\.com)/\w+/status/', na=False)
    
    # Separate dataframes
    unresolved_df = df[unresolved_mask]
    twitter_internal_df = df[twitter_internal_mask & ~unresolved_mask]  # Exclude any that are unresolved
    active_df = df[~unresolved_mask & ~twitter_internal_mask]
    
    print("\nUnresolved Shortened URLs:")
    if not unresolved_df.empty:
        print(f"Total unresolved shortened URLs: {len(unresolved_df)}")
        print("\nUnresolved domains breakdown:")
        print(unresolved_df['raw_domain'].value_counts())
        print("\nNote: These are shortened URLs that could not be resolved. Check url_resolution.log for details.")
    else:
        print("No unresolved shortened URLs found")
    
    print("\nTwitter Internal Link Statistics:")
    if not twitter_internal_df.empty:
        print(f"Total internal Twitter links: {len(twitter_internal_df)}")
        print(f"Unique Twitter conversations referenced: {twitter_internal_df['url'].nunique()}")
    else:
        print("No internal Twitter links found")
    
    print("\nTop 20 External Domains:")
    domain_counts = active_df['domain'].value_counts()
    if not domain_counts.empty:
        print(domain_counts.head(20))
    else:
        print("No external domains found")
    
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