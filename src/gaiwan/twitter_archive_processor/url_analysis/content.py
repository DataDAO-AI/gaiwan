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
        self.cache_ttl = timedelta(days=30)
        
        # Skip binary content types
        self.skip_content_types = {
            'application/pdf', 'application/zip', 'image/', 'video/', 'audio/'
        }

    async def analyze_urls(self, urls: List[str]) -> Dict[str, PageContent]:
        """Analyze multiple URLs concurrently."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            sem = asyncio.Semaphore(self.max_concurrent)
            
            async def analyze_with_semaphore(url: str):
                async with sem:
                    return url, await self.analyze_url(session, url)
            
            for url in urls:
                tasks.append(analyze_with_semaphore(url))
            
            results = await asyncio.gather(*tasks)
            return {url: content for url, content in results}

    async def analyze_url(self, session: aiohttp.ClientSession, url: str) -> PageContent:
        """Analyze a single URL, using cache if available."""
        cache_path = self._get_cache_path(url)
        
        # Check cache first
        cached_content = await self._load_from_cache(cache_path)
        if cached_content:
            return cached_content

        content = PageContent(url=url)
        try:
            async with session.get(url, timeout=30, allow_redirects=True) as response:
                content.status_code = response.status
                content.content_type = response.headers.get("content-type", "")
                
                if "text/html" in content.content_type.lower():
                    html = await response.text()
                    return await self._parse_content(url, html, content.content_type)
                else:
                    content.error = "Skipped binary content"
                    return content
        except Exception as e:
            content.error = str(e)
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