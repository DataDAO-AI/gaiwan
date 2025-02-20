import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Set
import logging
from datetime import datetime, timezone, timedelta
import hashlib
from pathlib import Path
import aiofiles
import json
from urllib.parse import urlparse
import re
from tqdm import tqdm
import ssl
import gc
from .apis.youtube import YouTubeAPI
from .apis.config import Config
from .apis.github import GitHubAPI
from .apis.twitter import TwitterAPI
from .models import PageContent
import csv

logger = logging.getLogger(__name__)

class ContentAnalyzer:
    """Asynchronous web content analyzer with caching."""
    
    def __init__(self, cache_dir: Optional[Path] = None, config: Optional[Config] = None):
        # Make cache directory absolute and ensure it exists
        self.cache_dir = Path(cache_dir or Path.home() / ".cache" / "twitter_archive_processor").resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using cache directory: {self.cache_dir}")
        
        self.max_concurrent = 3
        self.batch_size = 50  # Smaller batch size for content analysis
        self.cache_ttl = timedelta(days=30)
        
        # Longer timeout for rate-limited sites
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        # Configure SSL context
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        # Headers to look more like a browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Rate limiting delays (in seconds) by status code
        self.rate_limits = {
            429: 30,  # Too Many Requests
            403: 10,  # Forbidden (might be rate limiting)
            405: 10,  # Method Not Allowed
        }
        
        # Skip binary content types
        self.skip_content_types = {
            'application/pdf', 'application/zip', 'image/', 'video/', 'audio/',
            'application/octet-stream', 'application/x-binary'
        }

        # Load config and initialize APIs
        self.config = config or Config()
        
        # Initialize APIs with keys from config
        self.youtube_api = YouTubeAPI(api_key=self.config.get_api_key('youtube')) if self.config.get_api_key('youtube') else None
        self.twitter_api = TwitterAPI(
            api_key=self.config.get_api_key('twitter_key'),
            api_secret=self.config.get_api_key('twitter_secret'),
            bearer_token=self.config.get_api_key('twitter_bearer')
        ) if all(self.config.get_api_key(k) for k in ['twitter_key', 'twitter_secret', 'twitter_bearer']) else None
        self.github_api = GitHubAPI(api_key=self.config.get_api_key('github')) if self.config.get_api_key('github') else None

        # Setup URL processing log
        self.url_log_path = self.cache_dir / 'processed_urls.csv'
        self.processed_urls = {}
        self._load_processed_urls()
        
        # Create the log file if it doesn't exist
        if not self.url_log_path.exists():
            self.url_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.url_log_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['url', 'timestamp', 'status'])

    def _load_processed_urls(self):
        """Load processed URLs from log file."""
        if not self.url_log_path.exists():
            return
        
        with open(self.url_log_path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[2] == 'success':
                    url, timestamp, _ = row
                    self.processed_urls[url] = datetime.fromisoformat(timestamp)

    async def analyze_urls(self, urls: List[str], session: Optional[aiohttp.ClientSession] = None, progress_callback=None) -> Dict[str, PageContent]:
        """Analyze multiple URLs concurrently with optional progress callback."""
        if session is None:
            async with aiohttp.ClientSession() as new_session:
                return await self._analyze_urls_internal(urls, new_session, progress_callback)
        return await self._analyze_urls_internal(urls, session, progress_callback)

    async def _analyze_urls_internal(self, urls: List[str], session: aiohttp.ClientSession, progress_callback=None) -> Dict[str, PageContent]:
        results = {}
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        for i in range(0, len(urls), self.batch_size):
            batch = urls[i:i + self.batch_size]
            batch_results = await self._process_batch(batch, session, semaphore, progress_callback)
            results.update(batch_results)
            gc.collect()
        
        return results

    async def _process_batch(self, urls: List[str], session: aiohttp.ClientSession, 
                           semaphore: asyncio.Semaphore, progress_callback) -> Dict[str, PageContent]:
        """Process a batch of URLs concurrently."""
        results = {}
        
        async def process_url_with_semaphore(url: str):
            async with semaphore:
                result = await self.analyze_url(session, url)
                results[url] = result
                if progress_callback:
                    progress_callback(1)
                return result
                
        tasks = [process_url_with_semaphore(url) for url in urls]
        await asyncio.gather(*tasks)
        return results

    async def analyze_url(self, session: aiohttp.ClientSession, url: str) -> PageContent:
        """Analyze a single URL, using API if available."""
        # Check if URL was recently processed
        if url in self.processed_urls:
            last_processed = self.processed_urls[url]
            if datetime.now(timezone.utc) - last_processed < self.cache_ttl:
                logger.debug(f"Skipping recently processed URL: {url}")
                cache_path = self._get_cache_path(url)
                cached_content = await self._load_from_cache(cache_path)
                if cached_content:
                    return cached_content

        cache_path = self._get_cache_path(url)
        
        # Check cache first
        cached_content = await self._load_from_cache(cache_path)
        if cached_content:
            await self._log_processed_url(url, 'success')
            logger.debug(f"Cache hit for {url}")
            return cached_content
            
        # Try API-specific handlers first
        domain = urlparse(url).netloc.lower()
        
        if 'youtube.com' in domain or 'youtu.be' in domain:
            if self.youtube_api:
                content = await self.youtube_api.process_url(url)
                if content:
                    await self._save_to_cache(cache_path, content)
                    return content
                    
        if 'twitter.com' in domain or 'x.com' in domain:
            if self.twitter_api:
                content = await self.twitter_api.process_url(url)
                if content:
                    await self._save_to_cache(cache_path, content)
                    return content
                    
        if 'github.com' in domain:
            if self.github_api:
                content = await self.github_api.process_url(url)
                if content:
                    await self._save_to_cache(cache_path, content)
                    return content
        
        # Fall back to regular web scraping if no API available
        logger.debug(f"Cache miss for {url}")
        content = PageContent(url=url)
        for attempt in range(3):
            try:
                async with session.get(
                    url, 
                    timeout=self.timeout,
                    headers=self.headers,
                    allow_redirects=True,
                    max_redirects=5
                ) as response:
                    content.status_code = response.status
                    content.content_type = response.headers.get("content-type", "")
                    
                    # Handle rate limiting
                    if response.status in self.rate_limits:
                        delay = self.rate_limits[response.status]
                        logger.debug(f"Rate limited ({response.status}). Waiting {delay}s before retry...")
                        await asyncio.sleep(delay)
                        if attempt < 2:
                            continue
                    
                    # Skip binary content early
                    if any(ct in content.content_type.lower() for ct in self.skip_content_types):
                        content.error = "Skipped binary content"
                        await self._save_to_cache(cache_path, content)
                        return content

                    if response.status == 200:
                        try:
                            text = await response.text()
                            content = await self._parse_content(url, text, content.content_type)
                            await self._save_to_cache(cache_path, content)
                            await self._log_processed_url(url, 'success')
                            return content
                        except UnicodeDecodeError:
                            content.error = "Text decode error"
                    else:
                        content.error = f"HTTP {response.status}"

            except asyncio.TimeoutError:
                content.error = "Timeout"
            except aiohttp.ClientConnectorError as e:
                content.error = f"Connection error: {e}"
            except Exception as e:
                content.error = str(e)
            
            if attempt < 2:
                await asyncio.sleep(1 * (attempt + 1))
            
        await self._save_to_cache(cache_path, content)
        await self._log_processed_url(url, 'failed')
        return content

    async def _parse_content(self, url: str, content: str, content_type: str) -> PageContent:
        """Parse HTML content and extract metadata."""
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extract title
        title = soup.title.string if soup.title else None
        
        # Extract description
        description = None
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            description = meta_desc.get('content')
        
        # Extract links
        links = {a.get('href') for a in soup.find_all('a', href=True)}
        links = {link for link in links if self._is_valid_url(link)}
        
        # Extract images
        images = {img.get('src') for img in soup.find_all('img', src=True)}
        images = {img for img in images if self._is_valid_url(img)}
        
        # Extract main text content
        text_content = self._extract_main_content(soup)
        
        return PageContent(
            url=url,
            title=title,
            description=description,
            text_content=text_content,
            links=links,
            images=images,
            content_type=content_type
        )

    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main text content, removing boilerplate."""
        # Remove script and style elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()
        
        # Get text and normalize whitespace
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text

    def _get_cache_path(self, url: str) -> Path:
        """Generate a safe cache file path for a URL."""
        # Create a safe filename from the URL
        safe_name = hashlib.sha256(url.encode()).hexdigest()
        return self.cache_dir / f"{safe_name}.json"

    async def _log_processed_url(self, url: str, status: str) -> None:
        """Log URL processing status with timestamp."""
        timestamp = datetime.now(timezone.utc)
        
        # Only store successful URLs in the processed_urls cache
        if status == 'success':
            self.processed_urls[url] = timestamp
        
        async with aiofiles.open(self.url_log_path, 'a', newline='') as f:
            await f.write(f"{url},{timestamp.isoformat()},{status}\n")

    async def _load_from_cache(self, cache_path: Path) -> Optional[PageContent]:
        """Load content from cache if available and not expired."""
        try:
            if not cache_path.exists():
                return None
                
            async with aiofiles.open(cache_path, 'r') as f:
                cache_data = json.loads(await f.read())
                
            fetch_time = datetime.fromisoformat(cache_data['fetch_time'])
            if not fetch_time.tzinfo:
                fetch_time = fetch_time.replace(tzinfo=timezone.utc)
            
            # Check if cache has expired
            if (datetime.now(timezone.utc) - fetch_time) > self.cache_ttl:
                logger.debug(f"Cache expired for {cache_data['url']}")
                return None
                
            # Create PageContent from cache data
            return PageContent(
                url=cache_data['url'],
                title=cache_data.get('title'),
                description=cache_data.get('description'),
                content_type=cache_data.get('content_type'),
                status_code=cache_data.get('status_code'),
                error=cache_data.get('error'),
                fetch_time=fetch_time
            )
        except Exception as e:
            logger.error(f"Failed to load cache from {cache_path}: {e}")
            return None

    async def _save_to_cache(self, cache_path: Path, content: PageContent) -> None:
        """Save content to cache file."""
        try:
            cache_data = {
                'url': content.url,
                'title': content.title,
                'description': content.description,
                'content_type': content.content_type,
                'status_code': content.status_code,
                'error': content.error,
                'fetch_time': content.fetch_time.isoformat() if content.fetch_time else datetime.now(timezone.utc).isoformat()
            }
            
            async with aiofiles.open(cache_path, 'w') as f:
                await f.write(json.dumps(cache_data))
            logger.debug(f"Cached content for {content.url}")
        except Exception as e:
            logger.error(f"Failed to cache content for {content.url}: {e}")

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and absolute."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False 