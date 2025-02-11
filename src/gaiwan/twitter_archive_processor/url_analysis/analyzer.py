from pathlib import Path
import re
import logging
from typing import Dict, Set, Optional, List
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import functools
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
import orjson
from tqdm import tqdm
import asyncio
import aiohttp

from .metadata import URLMetadata
from .domain import DomainNormalizer
from .content import ContentAnalyzer, PageContent
from ..config import Config

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
            self.archives = list(self.archive_dir.glob("*_archive.json"))

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

    async def analyze_content(self, urls: List[str], url_pbar: tqdm) -> Dict[str, 'PageContent']:
        """Analyze content of URLs concurrently."""
        return await self.content_analyzer.analyze_urls(urls, url_pbar)

    async def _analyze_archives_async(self):
        """Async implementation of analyze_archives."""
        all_urls = set()
        
        # First pass: collect all unique URLs
        for archive in self.archives:
            urls = self._extract_urls_from_archive(archive)
            all_urls.update(urls)
            
        if not all_urls:
            logger.warning("No URLs found in archives")
            return {}
            
        total_urls = len(all_urls)
        print(f"\nAnalyzing {total_urls} unique URLs...")
        
        async with aiohttp.ClientSession() as session:
            # Create persistent progress bar
            with tqdm(total=total_urls, desc="Analyzing URLs") as url_pbar:
                content_results = await self.content_analyzer.analyze_urls(
                    list(all_urls), 
                    session=session,
                    progress_callback=lambda n: url_pbar.update(n)
                )
                
        return content_results

    def analyze_archives(self) -> Dict[str, PageContent]:
        """Analyze all URLs in the archives."""
        return asyncio.run(self._analyze_archives_async())

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
        df = self.analyze_archive(archive_path)
        if df.empty:
            return set()
        return set(df['url'].unique())


    
    