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

from .metadata import URLMetadata
from .domain import DomainNormalizer
from .content import ContentAnalyzer

logger = logging.getLogger(__name__)

class URLAnalyzer:
    """Analyzes URLs in Twitter archive data."""
    
    def __init__(self, archive_dir: Path, content_cache_dir: Optional[Path] = None):
        self.archive_dir = archive_dir
        self.domain_normalizer = DomainNormalizer()
        self._setup_url_pattern()
        self._setup_http_session()
        self._setup_caches()
        self.content_analyzer = ContentAnalyzer(
            content_cache_dir or (archive_dir / '.content_cache')
        )
        
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

    async def analyze_content(self, urls: List[str]) -> Dict[str, 'PageContent']:
        """Analyze content of URLs concurrently."""
        return await self.content_analyzer.analyze_urls(urls)

    async def _analyze_archives_async(self):
        """Async implementation of analyze_archives."""
        all_urls = set()
        df_list = []
        
        for archive_path in self.archive_dir.glob("*_archive.json"):
            df = self.analyze_archive(archive_path)
            if not df.empty:
                df_list.append(df)
                all_urls.update(df['url'].unique())
        
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            if all_urls:
                content_results = await self.analyze_content(list(all_urls))
                # Add content analysis results to DataFrame
                content_data = []
                for url, content in content_results.items():
                    content_data.append({
                        'url': url,
                        'page_title': content.title,
                        'page_description': content.description,
                        'content_type': content.content_type,
                        'content_hash': content.content_hash,
                        'linked_urls': len(content.links),
                        'image_count': len(content.images),
                        'fetch_status': 'success' if not content.error else 'error',
                        'fetch_error': content.error,
                        'fetch_time': content.fetch_time
                    })
                
                content_df = pd.DataFrame(content_data)
                combined_df = combined_df.merge(content_df, on='url', how='left')
            
            logger.info(f"\nAnalysis complete. DataFrame shape: {combined_df.shape}")
            return combined_df
        return pd.DataFrame()

    def analyze_archives(self) -> pd.DataFrame:
        """Analyze all archives and return a DataFrame."""
        archives = list(self.archive_dir.glob("*_archive.json"))
        logger.info(f"Found {len(archives)} archives to analyze")
        
        dfs = []
        for archive in tqdm(archives, desc="Analyzing archives"):
            df = self.analyze_archive(archive)
            if not df.empty:
                dfs.append(df)
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Analyze content for all unique URLs
            unique_urls = combined_df['url'].unique().tolist()
            content_results = asyncio.run(self.analyze_content(unique_urls))
            
            # Add content analysis results to DataFrame
            content_data = []
            for url, content in content_results.items():
                content_data.append({
                    'url': url,
                    'page_title': content.title,
                    'page_description': content.description,
                    'content_type': content.content_type,
                    'content_hash': content.content_hash,
                    'linked_urls': len(content.links),
                    'image_count': len(content.images),
                    'fetch_status': 'success' if not content.error else 'error',
                    'fetch_error': content.error,
                    'fetch_time': content.fetch_time
                })
            
            content_df = pd.DataFrame(content_data)
            combined_df = combined_df.merge(content_df, on='url', how='left')
            
            logger.info(f"\nAnalysis complete. DataFrame shape: {combined_df.shape}")
            return combined_df
        return pd.DataFrame()


    
    