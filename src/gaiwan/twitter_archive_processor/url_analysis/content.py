import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Set
import logging
from dataclasses import dataclass
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

logger = logging.getLogger(__name__)

@dataclass
class PageContent:
    """Container for scraped webpage content and metadata."""
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    text_content: Optional[str] = None
    links: Set[str] = None
    images: Set[str] = None
    fetch_time: datetime = None
    content_hash: Optional[str] = None
    content_type: Optional[str] = None
    status_code: Optional[int] = None
    error: Optional[str] = None

    def __post_init__(self):
        self.links = set() if self.links is None else self.links
        self.images = set() if self.images is None else self.images
        self.fetch_time = datetime.now(timezone.utc)

    def to_dict(self) -> Dict:
        return {
            'url': self.url,
            'title': self.title,
            'description': self.description,
            'text_content': self.text_content,
            'links': list(self.links),
            'images': list(self.images),
            'fetch_time': self.fetch_time.isoformat(),
            'content_hash': self.content_hash,
            'content_type': self.content_type,
            'status_code': self.status_code,
            'error': self.error
        }

class ContentAnalyzer:
    """Asynchronous web content analyzer with caching."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or Path.home() / ".cache" / "twitter_archive_processor"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = 5
        self.batch_size = 1000  # Process URLs in batches
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

    async def analyze_urls(self, urls: List[str]) -> Dict[str, PageContent]:
        """Analyze multiple URLs concurrently in batches."""
        all_results = {}
        total_urls = len(urls)
        
        with tqdm(total=total_urls, desc="Analyzing URLs") as pbar:
            for i in range(0, total_urls, self.batch_size):
                batch_urls = urls[i:i + self.batch_size]
                results = await self._analyze_url_batch(batch_urls, pbar)
                all_results.update(results)
                
                # Force garbage collection after each batch
                gc.collect()
        
        return all_results
    
    async def _analyze_url_batch(self, urls: List[str], pbar: tqdm) -> Dict[str, PageContent]:
        """Analyze a batch of URLs concurrently."""
        results = {}
        connector = aiohttp.TCPConnector(ssl=self.ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            sem = asyncio.Semaphore(self.max_concurrent)
            
            async def analyze_with_semaphore(url: str):
                async with sem:
                    result = await self.analyze_url(session, url)
                    pbar.update(1)
                    if result.error:
                        pbar.write(f"Error analyzing {url}: {result.error}")
                    return url, result
            
            for url in urls:
                tasks.append(analyze_with_semaphore(url))
            
            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, tuple):  # Successful result
                    results[result[0]] = result[1]
                else:  # Exception occurred
                    logger.error(f"Task failed with error: {result}")
        
        return results

    async def analyze_url(self, session: aiohttp.ClientSession, url: str) -> PageContent:
        """Analyze a single URL with better error handling and rate limiting."""
        cache_path = self._get_cache_path(url)
        cached_content = await self._load_from_cache(cache_path)
        if cached_content:
            return cached_content

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
        """Generate cache file path for URL."""
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        return self.cache_dir / f"{url_hash}.json"

    async def _load_from_cache(self, cache_path: Path) -> Optional[PageContent]:
        """Load cached content if available and not expired."""
        try:
            if not cache_path.exists():
                return None
                
            with open(cache_path) as f:
                data = json.load(f)
                
            content = PageContent(url=data['url'])
            content.title = data.get('title')
            content.description = data.get('description')
            content.links = set(data.get('links', []))
            content.images = set(data.get('images', []))
            content.fetch_time = datetime.fromisoformat(data['fetch_time'])
            
            # Check if cache is expired
            if datetime.now(timezone.utc) - content.fetch_time > self.cache_ttl:
                return None
                
            return content
        except Exception as e:
            logger.error(f"Error loading cache for {cache_path}: {e}")
            return None

    async def _save_to_cache(self, cache_path: Path, content: PageContent):
        """Save content to cache."""
        try:
            with open(cache_path, 'w') as f:
                json.dump(content.to_dict(), f)
        except Exception as e:
            logger.error(f"Error saving cache for {cache_path}: {e}")

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and absolute."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False 