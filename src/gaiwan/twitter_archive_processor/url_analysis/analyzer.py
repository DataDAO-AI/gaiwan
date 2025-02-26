from pathlib import Path
import re
import logging
from typing import Dict, Set, Optional, List
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import functools
import pandas as pd
from datetime import datetime, timezone
from urllib.parse import urlparse
import orjson
from tqdm import tqdm
import asyncio
import aiohttp
import gc

from .metadata import URLMetadata
from .domain import DomainNormalizer
from .content import ContentAnalyzer, PageContent
from .apis.config import Config

logger = logging.getLogger(__name__)

class URLAnalyzer:
    """Analyzes URLs in Twitter archive data."""
    
    def __init__(self, archive_dir: Optional[Path] = None, content_cache_dir: Optional[Path] = None):
        self.archive_dir = archive_dir
        self.domain_normalizer = DomainNormalizer()
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

    def _setup_url_pattern(self):
        """Initialize URL matching pattern."""
        self.url_pattern = re.compile(
            r'https?://(?:(?:www\.)?twitter\.com/[a-zA-Z0-9_]+/status/[0-9]+|'
            r'(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
        )
        
    def _setup_http_session(self):
        """Configure HTTP session with retries and headers."""
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        self.timeout = 10
        
    def _setup_caches(self):
        """Initialize URL and metadata caches."""
        self._url_cache: Dict[str, Optional[str]] = {}
        self._metadata_cache: Dict[str, URLMetadata] = {}

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

    def extract_urls_from_tweet(self, tweet_data: Dict) -> Set[str]:
        """Extract URLs from a tweet object."""
        urls = set()
        
        # Extract from tweet text using regex
        if 'full_text' in tweet_data:
            text = tweet_data['full_text']
            matches = re.findall(r'https?://[^\s]+', text)
            urls.update(matches)
        
        # Extract from entities if present
        if 'entities' in tweet_data and 'urls' in tweet_data['entities']:
            for url_entity in tweet_data['entities']['urls']:
                if 'expanded_url' in url_entity:
                    urls.add(url_entity['expanded_url'])
                elif 'url' in url_entity:
                    urls.add(url_entity['url'])
        
        return urls
    
    def analyze_archive(self, archive_path: Path) -> pd.DataFrame:
        """Analyze URLs in a single archive file."""
        try:
            with open(archive_path, 'rb') as f:
                data = orjson.loads(f.read())
            
            url_data = []
            username = archive_path.stem.replace('_archive', '')
            
            # Process tweets section
            for tweet_data in data.get('tweets', []):
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
                            'domain': self.domain_normalizer.normalize(parsed.netloc),
                            'raw_domain': parsed.netloc,
                            'protocol': parsed.scheme,
                            'path': parsed.path,
                            'query': parsed.query,
                            'fragment': parsed.fragment
                        })
            
            return pd.DataFrame(url_data)
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return pd.DataFrame()

    async def analyze_content(self, urls: List[str], url_pbar: Optional[tqdm] = None) -> Dict[str, 'PageContent']:
        """Analyze content of URLs concurrently."""
        progress_callback = lambda n: url_pbar.update(n) if url_pbar else None
        return await self.content_analyzer.analyze_urls(urls, progress_callback=progress_callback)

    async def _analyze_archives_async(self) -> pd.DataFrame:
        """Async implementation of analyze_archives."""
        all_urls = set()
        
        # First pass: collect all unique URLs
        for archive in self.archives:
            urls = self._extract_urls_from_archive(archive)
            all_urls.update(urls)
            
        if not all_urls:
            logger.warning("No URLs found in archives")
            return self._create_empty_dataframe()
        
        total_urls = len(all_urls)
        print(f"\nAnalyzing {total_urls} unique URLs...")
        
        url_data = []
        async with aiohttp.ClientSession() as session:
            # Create persistent progress bar
            with tqdm(total=total_urls, desc="Analyzing URLs") as url_pbar:
                content_results = await self.content_analyzer.analyze_urls(
                    list(all_urls), 
                    session=session,
                    progress_callback=lambda n: url_pbar.update(n)
                )
                
                # Convert content results to DataFrame rows
                for url, content in content_results.items():
                    parsed = urlparse(url)
                    url_data.append({
                        'url': url,
                        'domain': self.domain_normalizer.normalize(parsed.netloc),
                        'raw_domain': parsed.netloc,
                        'protocol': parsed.scheme,
                        'path': parsed.path,
                        'query': parsed.query,
                        'fragment': parsed.fragment,
                        'title': content.title,
                        'description': content.description,
                        'content_type': content.content_type,
                        'status_code': content.status_code,
                        'error': content.error
                    })
        
        return pd.DataFrame(url_data) if url_data else self._create_empty_dataframe()

    def analyze_archives(self) -> pd.DataFrame:
        """Analyze URLs in all archives."""
        if not self.archives:
            return self._create_empty_dataframe()
        
        url_data = []
        for archive in self.archives:
            try:
                df = self.analyze_archive(archive)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    url_data.append(df)
            except Exception as e:
                logger.error(f"Error processing archive {archive}: {e}")
                continue
        
        if not url_data:
            return self._create_empty_dataframe()
        
        return pd.concat(url_data, ignore_index=True)

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with standard columns."""
        return pd.DataFrame(columns=[
            'username', 'tweet_id', 'tweet_created_at', 'url',
            'domain', 'raw_domain', 'protocol', 'path',
            'query', 'fragment', 'title', 'description',
            'content_type', 'status_code', 'error'
        ])

    async def _process_archive_in_batches(self, archive_path: Path) -> pd.DataFrame:
        """Process a single archive file in batches."""
        try:
            with open(archive_path, 'rb') as f:
                data = orjson.loads(f.read())
            
            url_data = []
            username = archive_path.stem.replace('_archive', '')
            
            # Extract all tweets first
            tweets = data.get('tweets', [])
            total_tweets = len(tweets)
            
            for i in range(0, total_tweets, self.batch_size):
                batch_tweets = tweets[i:i + self.batch_size]
                batch_urls = set()
                batch_url_data = []
                
                # Extract URLs from batch
                for tweet_data in batch_tweets:
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
                            batch_urls.add(url)
                            batch_url_data.append({
                                'username': username,
                                'tweet_id': tweet_id,
                                'tweet_created_at': created_at,
                                'url': url,
                                'domain': self.domain_normalizer.normalize(parsed.netloc),
                                'raw_domain': parsed.netloc,
                                'protocol': parsed.scheme,
                                'path': parsed.path,
                                'query': parsed.query,
                                'fragment': parsed.fragment
                            })
                
                # Filter out already processed URLs
                new_urls = {url for url in batch_urls 
                          if url not in self.content_analyzer.processed_urls or 
                          datetime.now(timezone.utc) - self.content_analyzer.processed_urls[url] > self.content_analyzer.cache_ttl}
                
                # Process content for new URLs only
                if new_urls:
                    async with aiohttp.ClientSession() as session:
                        content_results = await self.content_analyzer.analyze_urls(
                            list(new_urls),
                            session=session
                        )
                        
                        # Update URL data with content results
                        for url_entry in batch_url_data:
                            if url_entry['url'] in content_results:
                                content = content_results[url_entry['url']]
                                url_entry.update({
                                    'title': content.title,
                                    'description': content.description,
                                    'content_type': content.content_type,
                                    'status_code': content.status_code,
                                    'error': content.error
                                })
                
                url_data.extend(batch_url_data)
                
                # Force garbage collection after each batch
                gc.collect()
            
            return pd.DataFrame(url_data)
            
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {e}")
            return pd.DataFrame()

    def get_domain_stats(self) -> pd.DataFrame:
        """Get statistics about domains in the dataset."""
        all_urls = set()
        for archive_path in self.archive_dir.glob("*_archive.json"):
            df = self.analyze_archive(archive_path)
            if not df.empty:
                all_urls.update(df['url'].unique())
        
        # Parse domains from URLs
        domains = []
        for url in all_urls:
            try:
                parsed = urlparse(url)
                domain = parsed.netloc.lower()
                if domain.startswith('www.'):
                    domain = domain[4:]
                domains.append(domain)
            except Exception as e:
                logger.error(f"Failed to parse URL {url}: {e}")
        
        # Create DataFrame with domain counts
        domain_df = pd.DataFrame(domains, columns=['domain'])
        domain_stats = domain_df['domain'].value_counts().reset_index()
        domain_stats.columns = ['domain', 'count']
        
        return domain_stats

    def _extract_urls_from_archive(self, archive_path: Path) -> Set[str]:
        """Extract all URLs from a single archive file."""
        try:
            with open(archive_path, 'rb') as f:
                data = orjson.loads(f.read())
            
            urls = set()
            if 'tweets' in data:
                for tweet_data in data['tweets']:
                    if 'tweet' in tweet_data:
                        tweet = tweet_data['tweet']
                        # Extract from tweet text
                        if 'full_text' in tweet:
                            text_urls = self.url_pattern.findall(tweet['full_text'])
                            urls.update(text_urls)
                        
                        # Extract from entities
                        if 'entities' in tweet and 'urls' in tweet['entities']:
                            for url_entity in tweet['entities']['urls']:
                                if 'expanded_url' in url_entity:
                                    urls.add(url_entity['expanded_url'])
                                elif 'url' in url_entity:
                                    urls.add(url_entity['url'])
            
            return urls
        except Exception as e:
            logger.error(f"Error extracting URLs from {archive_path}: {e}")
            return set()

    async def process_archive(self, archive_path: Path) -> pd.DataFrame:
        """Process a single archive file."""
        archive_name = archive_path.stem
        if archive_name in self.processed_archives:
            logger.info(f"Skipping already processed archive: {archive_name}")
            return pd.DataFrame()
            
        logger.info(f"Processing archive: {archive_name}")
        
        # Extract URLs and contexts from archive
        archive_data = self._extract_archive_data(archive_path)
        if not archive_data:
            return pd.DataFrame()
            
        # Process URLs for this archive
        async with aiohttp.ClientSession() as session:
            content_results = await self.content_analyzer.analyze_archive_urls(
                archive_name,
                list(archive_data.keys()),
                session=session,
                progress_callback=self._update_progress
            )
            
        # Create DataFrame entries
        url_data = []
        for url, content in content_results.items():
            for context in archive_data[url]:
                url_data.append(self._create_url_entry(url, content, context))
                
        self.processed_archives.add(archive_name)
        df = pd.DataFrame(url_data)
        self.archive_results[archive_name] = df
        return df
        
    def _extract_archive_data(self, archive_path: Path) -> Dict[str, List[dict]]:
        """Extract URLs and their contexts from an archive file."""
        archive_data = {}
        username = archive_path.stem.replace('_archive', '')
        
        try:
            with open(archive_path, 'rb') as f:
                data = orjson.loads(f.read())
                
            for tweet_data in data.get('tweets', []):
                if 'tweet' in tweet_data:
                    tweet = tweet_data['tweet']
                    context = self._create_tweet_context(tweet, username)
                    
                    urls = self.extract_urls_from_tweet(tweet)
                    for url in urls:
                        if url not in archive_data:
                            archive_data[url] = []
                        archive_data[url].append(context)
                        
        except Exception as e:
            logger.error(f"Error processing archive {archive_path}: {e}")
            return {}
            
        return archive_data
        
    def _create_tweet_context(self, tweet: dict, username: str) -> dict:
        """Create context dictionary for a tweet."""
        return {
            'username': username,
            'tweet_id': tweet.get('id_str'),
            'tweet_created_at': datetime.strptime(
                tweet.get('created_at', ''), 
                "%a %b %d %H:%M:%S %z %Y"
            ) if tweet.get('created_at') else None
        }
        
    def _create_url_entry(self, url: str, content: PageContent, context: dict) -> dict:
        """Create a dictionary entry for a URL."""
        parsed = urlparse(url)
        return {
            **context,
            'url': url,
            'domain': self.domain_normalizer.normalize(parsed.netloc),
            'raw_domain': parsed.netloc,
            'protocol': parsed.scheme,
            'path': parsed.path,
            'query': parsed.query,
            'fragment': parsed.fragment,
            'title': content.title,
            'description': content.description,
            'content_type': content.content_type,
            'status_code': content.status_code,
            'error': content.error
        }
        
    def get_archive_stats(self) -> pd.DataFrame:
        """Get statistics about processed archives."""
        stats = []
        for archive_name, df in self.archive_results.items():
            stats.append({
                'archive': archive_name,
                'total_urls': len(df),
                'unique_urls': df['url'].nunique(),
                'domains': df['domain'].nunique(),
                'success_rate': (df['status_code'] == 200).mean(),
                'error_rate': df['error'].notna().mean()
            })
        return pd.DataFrame(stats)


    
    