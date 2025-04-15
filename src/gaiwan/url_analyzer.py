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
from bs4 import BeautifulSoup
import time
from threading import Semaphore
from .config import config
from .twitter_archive_processor.url_analysis.domain import DomainNormalizer
from .twitter_archive_processor.url_analysis.content import ContentAnalyzer

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

    def extract_metadata(self, soup: BeautifulSoup) -> None:
        """Extract metadata from BeautifulSoup object."""
        # Extract meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            self.metadata['description'] = meta_desc.get('content')
            
        # Extract meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            self.metadata['keywords'] = meta_keywords.get('content')
            
        # Extract OpenGraph title
        og_title = soup.find('meta', property='og:title')
        if og_title:
            self.metadata['og_title'] = og_title.get('content')
            
        # Extract OpenGraph description
        og_desc = soup.find('meta', property='og:description')
        if og_desc:
            self.metadata['og_description'] = og_desc.get('content')

class DomainRetryPolicy:
    """Policy for handling retries for specific domains."""
    def __init__(self):
        self.max_retries = 3
        self.max_retry_time = 3600  # 1 hour in seconds
        self.blacklisted_domains = set()
        self.domain_retry_counts = {}
        
    def should_retry(self, domain: str, error_code: int) -> bool:
        """Determine if we should retry a request for this domain."""
        if domain in self.blacklisted_domains:
            return False
            
        if error_code in [403, 404]:  # Permanent errors
            self.blacklisted_domains.add(domain)
            return False
            
        retry_count = self.domain_retry_counts.get(domain, 0)
        if retry_count >= self.max_retries:
            self.blacklisted_domains.add(domain)
            return False
            
        self.domain_retry_counts[domain] = retry_count + 1
        return True

class RateLimiter:
    """Rate limiter for controlling request frequency."""
    def __init__(self, max_requests_per_second: int):
        self.semaphore = Semaphore(max_requests_per_second)
        self.last_request_time = 0
        self.retry_policy = DomainRetryPolicy()
        
    def acquire(self, domain: str = None):
        """Acquire a permit, waiting if necessary."""
        self.semaphore.acquire()
        current_time = time.time()
        if current_time - self.last_request_time < 1.0:
            time.sleep(1.0 - (current_time - self.last_request_time))
        self.last_request_time = time.time()
        
    def release(self):
        """Release a permit."""
        self.semaphore.release()
        
    def should_retry(self, domain: str, error_code: int) -> bool:
        """Check if we should retry a failed request."""
        return self.retry_policy.should_retry(domain, error_code)

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
        output_file (Path, optional): Path to save the output parquet file
    """

    def __init__(self, archive_dir: Path, content_cache_dir: Path):
        self.archive_dir = archive_dir
        self.domain_normalizer = DomainNormalizer()
        self.url_pattern = None  # Initialize the pattern variable
        self._setup_url_pattern()
        self._setup_http_session()
        self._setup_caches()
        
        # Default to system temp directory if no paths provided
        if content_cache_dir is None and archive_dir is None:
            content_cache_dir = Path.home() / ".cache" / "twitter_archive_processor"
        elif content_cache_dir is None and archive_dir is not None:
            content_cache_dir = archive_dir / '.content_cache'
            
        self.content_analyzer = ContentAnalyzer(content_cache_dir)
        
        # Initialize archives list
        self.archives = []
        if self.archive_dir:
            # Update the glob pattern to match test files
            self.archives = list(self.archive_dir.glob("*.json"))
            logger.debug(f"Found {len(self.archives)} archive files in {self.archive_dir}")
        
        self.batch_size = 100  # Number of URLs to process at once
        self.processed_archives = set()  # Track which archives have been processed
        self.archive_results = {}  # Store results per archive
        self.output_file = None  # Initialize output_file attribute

        # Initialize HTML storage settings from config
        self.store_html = config.store_html
        self.compress_html = config.compress_html
        self.clean_html = config.clean_html

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
            total=config.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Cache for resolved URLs and metadata
        self._url_cache: Dict[str, Optional[str]] = {}
        self._metadata_cache: Dict[str, 'PageMetadata'] = {}
        
        # Set a reasonable timeout for requests
        self.timeout = config.request_timeout
        
        # Add common headers to appear more like a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

        # Rate limiter
        self.rate_limiter = RateLimiter(max_requests_per_second=config.max_requests_per_second)

    def _setup_url_pattern(self):
        """Set up the URL pattern for extracting URLs from tweets."""
        # Improved URL pattern to better match Twitter URLs
        self.url_pattern = re.compile(
            r'https?://(?:(?:www\.)?twitter\.com/[a-zA-Z0-9_]+/status/[0-9]+|'
            r'(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
        )

    def _setup_http_session(self):
        """Set up the HTTP session with retries and headers."""
        # Set up requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=config.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Set a reasonable timeout for requests
        self.timeout = config.request_timeout
        
        # Add common headers to appear more like a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def _setup_caches(self):
        """Set up caches for URL resolution and metadata."""
        # Cache for resolved URLs and metadata
        self._url_cache: Dict[str, Optional[str]] = {}
        self._metadata_cache: Dict[str, 'PageMetadata'] = {}

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
        """Fetch and extract metadata from a webpage."""
        if url in self._metadata_cache:
            return self._metadata_cache[url]
        
        metadata = PageMetadata(url)
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        try:
            # Don't try to get metadata from certain file types
            if any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip']):
                metadata.mark_skipped(f"Skipping media file")
                self._metadata_cache[url] = metadata
                return metadata

            # Special handling for Twitter/X URLs
            if parsed_url.netloc in ['twitter.com', 'x.com']:
                metadata.title = f"Twitter/X post by {parsed_url.path.split('/')[1]}"
                metadata.mark_success('text/html')
                self._metadata_cache[url] = metadata
                return metadata

            self.rate_limiter.acquire(domain)
            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            # Check if it's HTML content
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                metadata.mark_skipped(f"Non-HTML content: {content_type}")
                self._metadata_cache[url] = metadata
                return metadata
            
            # Read the complete content
            content = response.text
            
            # Store HTML if enabled
            if self.store_html:
                # Clean HTML if enabled
                if self.clean_html:
                    content = self.clean_html_content(content)
                    
                # Compress HTML if enabled
                if self.compress_html:
                    content = self.compress_html_content(content)
                    
                metadata.html_content = content
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract title - handle case where title tag doesn't exist
            title_tag = soup.find('title')
            if title_tag and title_tag.text:
                metadata.title = title_tag.text.strip()
            else:
                metadata.title = url  # Fallback to URL if no title found
            
            # Extract all metadata
            metadata.extract_metadata(soup)
            
            metadata.mark_success(content_type)
            self._metadata_cache[url] = metadata
            logger.debug(f"Successfully fetched metadata for {url}")
            return metadata
                
        except requests.exceptions.HTTPError as e:
            error_code = e.response.status_code
            if self.rate_limiter.should_retry(domain, error_code):
                logger.debug(f"Retrying {url} after {error_code} error")
                time.sleep(5)  # Short delay before retry
                return self.get_page_metadata(url)
            else:
                error_msg = f"HTTP {error_code}: {str(e)}"
                metadata.mark_failed(error_msg)
                logger.debug(f"Failed to fetch metadata for {url}: {error_msg}")
                self._metadata_cache[url] = metadata
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
            tweets = data.get('tweets', [])
            
            # Add progress bar for tweets within this archive
            with tqdm(total=len(tweets), desc="Processing tweets", position=1, leave=False) as tweet_pbar:
                for tweet_data in tweets:
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
                            # Get metadata for the URL
                            metadata = self.get_page_metadata(url)
                            metadata_dict = metadata.to_dict()
                            
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
                                'is_resolved': False,  # Will be updated if URL is resolved
                                **metadata_dict  # Add all metadata fields
                            })
                        
                        # Update progress after each tweet
                        tweet_pbar.update(1)
                        # Periodically update description to show URL count
                        if len(url_data) % 100 == 0 and url_data:
                            tweet_pbar.set_description(f"Processing tweets ({len(url_data)} URLs found)")
            
            # Process URL resolution if needed
            for url_data_item in url_data:
                if url_data_item['is_resolved'] == False:
                    url = url_data_item['url']
                    if self.should_resolve_url(url):
                        logger.debug(f"Attempting to resolve shortened URL: {url}")
                        resolved = self.resolve_url(url)
                        if resolved:
                            logger.debug(f"Successfully resolved {url} -> {resolved}")
                            url_data_item['is_resolved'] = True
                            url_data_item['url'] = resolved
                        else:
                            logger.debug(f"Failed to resolve shortened URL: {url}")
                            url_data_item['is_resolved'] = False
                    else:
                        url_data_item['is_resolved'] = False
            
            return pd.DataFrame(url_data)
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return pd.DataFrame()

    def analyze_archives(self) -> pd.DataFrame:
        """Analyze URLs across all archives."""
        dfs = []
        archives = list(self.archive_dir.glob("*_archive.json"))
        
        # Add main progress bar for archives
        with tqdm(total=len(archives), desc="Analyzing archives", position=0) as archive_pbar:
            for archive_path in archives:
                username = archive_path.stem.replace('_archive', '')
                archive_pbar.set_description(f"Analyzing archive: {username}")
                
                df = self.analyze_archive(archive_path)
                if not df.empty:
                    dfs.append(df)
                    
                    # Save incremental results after each archive
                    if hasattr(self, 'output_file') and self.output_file:
                        # Create combined DataFrame with all processed archives so far
                        combined_df = pd.concat(dfs, ignore_index=True)
                        
                        # Create temp file to avoid corrupting the main file if interrupted
                        temp_file = self.output_file.with_name(f"{self.output_file.stem}_temp.parquet")
                        combined_df.to_parquet(temp_file)
                        
                        # Safely rename to the actual output file
                        if temp_file.exists():
                            if self.output_file.exists():
                                self.output_file.unlink()  # Remove existing file
                            temp_file.rename(self.output_file)
                            
                        logger.info(f"Saved incremental results after processing {username}. Total URLs: {len(combined_df)}")
                    
                archive_pbar.update(1)
        
        # Combine all DataFrames
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"\nAnalysis complete. DataFrame shape: {combined_df.shape}")
            return combined_df
        return pd.DataFrame()

class TqdmLoggingHandler(logging.Handler):
    """Logging handler that works with tqdm progress bars."""
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

def main():
    """Command-line interface for URL analysis.
    
    This function provides a CLI for analyzing URLs in Twitter archives.
    It supports incremental processing, meaning it will only analyze new
    archives not present in existing output file.
    
    Arguments:
        archive_path: Path to either a directory containing Twitter archives or a single archive file
        --debug: Enable debug logging
        --output_file: Custom output file path (default: urls.parquet)
        --force: Force reanalysis of all archives
        --content_cache_dir: Directory to store content cache (default: archive_path/.content_cache)
    
    The function will:
    1. Check for existing analysis file (urls.parquet)
    2. Load and process only new archives
    3. Merge results with existing data
    4. Create backup of existing analysis
    5. Save updated analysis
    6. Print summary statistics
    """
    import argparse
    parser = argparse.ArgumentParser(description="Analyze URLs in Twitter archives")
    parser.add_argument('archive_path', type=Path, help="Directory containing archives or path to single archive file")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument('--output_file', type=Path, help="Save DataFrame to Parquet file")
    parser.add_argument('--force', action='store_true', help="Force reanalysis of all archives")
    parser.add_argument('--content_cache_dir', type=Path, help="Directory to store content cache")
    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting URL analysis for {args.archive_path}")
    
    # Initialize analyzer with output file if provided
    content_cache_dir = args.content_cache_dir or args.archive_path / '.content_cache'
    analyzer = URLAnalyzer(archive_dir=args.archive_path, content_cache_dir=content_cache_dir)
    if args.output_file:
        analyzer.output_file = args.output_file
        logger.info(f"Results will be saved to: {args.output_file}")
    
    # Add tqdm-compatible handler
    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(tqdm_handler)

    output_file = args.output_file or Path('urls.parquet')
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

    # Handle both file and directory inputs
    archive_path = args.archive_path
    content_cache_dir = args.content_cache_dir or args.archive_path / '.content_cache'
    if archive_path.is_file() and archive_path.name.endswith('_archive.json'):
        analyzer = URLAnalyzer(archive_dir=archive_path.parent, content_cache_dir=content_cache_dir)
        archives = [archive_path]
    else:
        analyzer = URLAnalyzer(archive_dir=archive_path, content_cache_dir=content_cache_dir)
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
    if args.output_file:
        output_path = args.output_file
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