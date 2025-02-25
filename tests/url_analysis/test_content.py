import pytest
import asyncio
from pathlib import Path
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import json
from unittest.mock import Mock, patch, AsyncMock
from contextlib import asynccontextmanager
import csv
import ssl
from aiohttp import web

from gaiwan.twitter_archive_processor.url_analysis.content import ContentAnalyzer, PageContent
from .test_utils import create_mock_response
from gaiwan.twitter_archive_processor.url_analysis.apis.youtube import YouTubeAPI
from gaiwan.twitter_archive_processor.url_analysis.apis.twitter import TwitterAPI
from gaiwan.twitter_archive_processor.url_analysis.apis.github import GitHubAPI

@pytest.fixture
def sample_html():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Page</title>
        <meta name="description" content="Test description">
    </head>
    <body>
        <header>Header content</header>
        <nav>Navigation</nav>
        <main>
            <p>Main content here</p>
            <a href="https://example.com">Link 1</a>
            <a href="invalid-url">Invalid Link</a>
            <img src="https://example.com/image.jpg">
            <img src="invalid-image.jpg">
        </main>
        <footer>Footer content</footer>
        <script>JavaScript code</script>
        <style>CSS styles</style>
    </body>
    </html>
    """

@pytest.fixture
def temp_cache_dir(tmp_path):
    cache_dir = tmp_path / "content_cache"
    cache_dir.mkdir()
    return cache_dir

@pytest.fixture
def content_analyzer(temp_cache_dir):
    analyzer = ContentAnalyzer(cache_dir=temp_cache_dir)
    # Create async mock API instances
    analyzer.youtube_api = AsyncMock()
    analyzer.twitter_api = AsyncMock()
    analyzer.github_api = AsyncMock()
    return analyzer

@pytest.fixture
async def test_session():
    """Create a test session with SSL verification disabled."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        yield session

class AsyncMockResponse:
    """Mock response that properly implements async context manager."""
    def __init__(self, html):
        self.html = html
        self.status = 200
        self.headers = {'content-type': 'text/html'}
        self._text = html
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
        
    async def text(self):
        return self._text

def test_page_content_initialization():
    content = PageContent(url="https://example.com")
    
    assert content.url == "https://example.com"
    assert content.links == set()
    assert content.images == set()
    assert isinstance(content.fetch_time, datetime)
    assert content.fetch_time.tzinfo == timezone.utc

def test_page_content_to_dict():
    content = PageContent(
        url="https://example.com",
        title="Test",
        description="Test description",
        links={"https://link1.com"},
        images={"https://image1.jpg"},
        content_type="text/html",
        status_code=200
    )
    
    data = content.to_dict()
    assert data['url'] == "https://example.com"
    assert data['title'] == "Test"
    assert data['links'] == ["https://link1.com"]
    assert data['images'] == ["https://image1.jpg"]
    assert isinstance(data['fetch_time'], str)

@pytest.mark.asyncio
async def test_content_parsing(content_analyzer, sample_html):
    url = "https://example.com"
    content_type = "text/html"
    
    page_content = await content_analyzer._parse_content(url, sample_html, content_type)
    
    assert page_content.title == "Test Page"
    assert page_content.description == "Test description"
    assert "https://example.com" in page_content.links
    assert "https://example.com/image.jpg" in page_content.images
    assert "invalid-url" not in page_content.links
    assert "invalid-image.jpg" not in page_content.images
    assert "Main content here" in page_content.text_content
    assert "JavaScript code" not in page_content.text_content
    assert "CSS styles" not in page_content.text_content

@pytest.mark.asyncio
async def test_cache_operations(content_analyzer):
    content = PageContent(
        url="https://example.com",
        title="Test",
        description="Test description"
    )
    
    cache_path = content_analyzer._get_cache_path(content.url)
    
    # Test saving to cache
    await content_analyzer._save_to_cache(cache_path, content)
    assert cache_path.exists()
    
    # Test loading from cache
    loaded_content = await content_analyzer._load_from_cache(cache_path)
    assert loaded_content is not None
    assert loaded_content.url == content.url
    assert loaded_content.title == content.title
    
    # Test cache expiration
    loaded_content.fetch_time = datetime.now(timezone.utc) - timedelta(days=31)
    await content_analyzer._save_to_cache(cache_path, loaded_content)
    expired_content = await content_analyzer._load_from_cache(cache_path)
    assert expired_content is None

@pytest.mark.asyncio
async def test_analyze_url(content_analyzer, sample_html):
    url = "https://example.com"
    
    class MockResponse:
        def __init__(self):
            self.status = 200
            self.headers = {"content-type": "text/html"}
            self._text = sample_html
        
        async def text(self):
            return self._text
    
    class MockSession:
        @asynccontextmanager
        async def get(self, *args, **kwargs):
            yield MockResponse()
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
    
    session = MockSession()
    with patch('aiohttp.ClientSession', return_value=session):
        content = await content_analyzer.analyze_url(session, url)
        assert content.url == url
        assert content.title == "Test Page"
        assert content.error is None

@pytest.mark.asyncio
async def test_analyze_urls(content_analyzer, sample_html):
    urls = ["https://example1.com", "https://example2.com"]
    
    class MockResponse:
        def __init__(self):
            self.status = 200
            self.headers = {"content-type": "text/html"}
            self._text = sample_html
        
        async def text(self):
            return self._text
    
    class MockSession:
        @asynccontextmanager
        async def get(self, *args, **kwargs):
            yield MockResponse()
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
    
    session = MockSession()
    with patch('aiohttp.ClientSession', return_value=session):
        results = await content_analyzer.analyze_urls(urls)
        assert len(results) == 2
        assert all(isinstance(content, PageContent) for content in results.values())
        assert all(content.title == "Test Page" for content in results.values())

def test_url_validation(content_analyzer):
    assert content_analyzer._is_valid_url("https://example.com")
    assert content_analyzer._is_valid_url("http://example.com/path?query=1")
    assert not content_analyzer._is_valid_url("invalid-url")
    assert not content_analyzer._is_valid_url("/relative/path")
    assert not content_analyzer._is_valid_url("javascript:void(0)")

@pytest.mark.asyncio
async def test_concurrent_limits(content_analyzer):
    content_analyzer.max_concurrent = 2
    urls = [f"https://example{i}.com" for i in range(5)]
    
    class MockResponse:
        def __init__(self):
            self.status = 200
            self.headers = {"content-type": "text/html"}
            self._text = "<html><title>Test</title></html>"
        
        async def text(self):
            await asyncio.sleep(0.1)
            return self._text
    
    class MockSession:
        @asynccontextmanager
        async def get(self, *args, **kwargs):
            await asyncio.sleep(0.1)
            yield MockResponse()
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
    
    session = MockSession()
    with patch('aiohttp.ClientSession', return_value=session):
        start_time = datetime.now()
        results = await content_analyzer.analyze_urls(urls)
        duration = (datetime.now() - start_time).total_seconds()
        
        # With max_concurrent=2 and 0.2s delay per request, processing 5 URLs should take at least 0.6s
        assert duration >= 0.6
        assert len(results) == 5

class AsyncContextManagerMock:
    """A proper async context manager mock."""
    def __init__(self, response):
        self.response = response
        
    async def __aenter__(self):
        return self.response
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

@pytest.mark.asyncio
async def test_cache_behavior(content_analyzer, tmp_path):
    """Test URL caching behavior."""
    content_analyzer.cache_dir = tmp_path
    url = "https://example.com"
    
    # Create a proper async response mock
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.headers = {'content-type': 'text/html'}
    mock_response.text.return_value = "<html><title>Test Title</title></html>"
    
    # Create async session with proper context manager
    class AsyncSessionMock:
        def __init__(self):
            self.call_count = 0
            
        async def get(self, *args, **kwargs):
            self.call_count += 1
            return AsyncContextManagerMock(mock_response)
    
    session = AsyncSessionMock()
    
    # Disable caching for first request
    content_analyzer._load_from_cache = AsyncMock(return_value=None)
    content_analyzer.max_retries = 0
    
    # First request - should hit the network
    result1 = await content_analyzer.analyze_url(session, url)
    assert session.call_count == 1
    assert result1.title == "Test Title"

@pytest.mark.asyncio
async def test_cache_expiration(content_analyzer, tmp_path):
    content_analyzer.cache_dir = tmp_path
    content_analyzer.cache_ttl = timedelta(seconds=1)
    url = "https://example.com"
    
    # Create and cache initial content
    content = PageContent(url=url, title="Test")
    cache_path = content_analyzer._get_cache_path(url)
    await content_analyzer._save_to_cache(cache_path, content)
    
    # Immediate load should work
    cached = await content_analyzer._load_from_cache(cache_path)
    assert cached is not None
    assert cached.title == "Test"
    
    # Wait for expiration
    await asyncio.sleep(1.1)
    
    # Should return None after expiration
    expired = await content_analyzer._load_from_cache(cache_path)
    assert expired is None

# Add new test cases for URL processing log
@pytest.mark.asyncio
async def test_url_processing_log(content_analyzer, tmp_path):
    """Test URL processing log functionality."""
    # Set the cache directory for the test
    content_analyzer.cache_dir = tmp_path
    content_analyzer.url_log_path = tmp_path / 'processed_urls.csv'
    
    # Create the log file
    content_analyzer.url_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(content_analyzer.url_log_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['url', 'timestamp', 'status'])
    
    url = "https://example.com"
    
    # Test log initialization
    assert content_analyzer.url_log_path.exists()
    assert content_analyzer.processed_urls == {}
    
    # Test logging successful URL
    await content_analyzer._log_processed_url(url, 'success')
    assert url in content_analyzer.processed_urls
    assert isinstance(content_analyzer.processed_urls[url], datetime)
    
    # Test logging failed URL
    failed_url = "https://failed.com"
    await content_analyzer._log_processed_url(failed_url, 'failed')
    assert failed_url not in content_analyzer.processed_urls  # Failed URLs aren't cached
    
    # Test log persistence
    # Create new analyzer instance to test log loading
    new_analyzer = ContentAnalyzer(cache_dir=tmp_path)
    assert url in new_analyzer.processed_urls
    assert failed_url not in new_analyzer.processed_urls

@pytest.mark.asyncio
async def test_url_processing_cache_ttl(content_analyzer, tmp_path):
    """Test URL processing respects cache TTL."""
    url = "https://example.com"
    
    # Set short TTL for testing
    content_analyzer.cache_ttl = timedelta(seconds=1)
    
    # Log URL as processed
    await content_analyzer._log_processed_url(url, 'success')
    assert url in content_analyzer.processed_urls
    
    # Wait for TTL to expire
    await asyncio.sleep(1.1)
    
    # Create mock session for testing
    class MockSession:
        call_count = 0
        
        @asynccontextmanager
        async def get(self, *args, **kwargs):
            self.call_count += 1
            yield create_mock_response("<html><title>Test</title></html>")
    
    session = MockSession()
    
    # Should reprocess URL after TTL expiration
    await content_analyzer.analyze_url(session, url)
    assert session.call_count == 1  # URL should be reprocessed 

@pytest.mark.asyncio
async def test_api_handlers(content_analyzer, test_session):
    """Test API-specific URL handlers."""
    # YouTube test
    youtube_url = "https://www.youtube.com/watch?v=test123"
    youtube_content = PageContent(
        url=youtube_url,
        title="Test Video",
        content_type="video/youtube"
    )
    
    # AsyncMock will automatically wrap the return value in a coroutine
    content_analyzer.youtube_api.process_url.return_value = youtube_content
    result = await content_analyzer.analyze_url(test_session, youtube_url)
    assert result.content_type == "video/youtube"
    assert result.title == "Test Video"
    
    # Twitter test
    tweet_url = "https://twitter.com/user/status/123456"
    tweet_content = PageContent(
        url=tweet_url,
        title="Test Tweet",
        content_type="application/twitter"
    )
    
    content_analyzer.twitter_api.process_url.return_value = tweet_content
    result = await content_analyzer.analyze_url(test_session, tweet_url)
    assert result.content_type == "application/twitter"
    assert result.title == "Test Tweet"
    
    # GitHub test
    github_url = "https://github.com/user/repo"
    github_content = PageContent(
        url=github_url,
        title="Test Repo",
        content_type="application/github"
    )
    
    content_analyzer.github_api.process_url.return_value = github_content
    result = await content_analyzer.analyze_url(test_session, github_url)
    assert result.content_type == "application/github"
    assert result.title == "Test Repo"

@pytest.mark.asyncio
async def test_api_fallback(content_analyzer, test_session):
    """Test fallback to web scraping when API fails."""
    youtube_url = "https://www.youtube.com/watch?v=test123"
    
    # Mock API failure
    content_analyzer.youtube_api.process_url.return_value = None
    
    # Create proper async response mock
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.headers = {'content-type': 'text/html'}
    mock_response.text.return_value = "<html><title>Fallback Title</title></html>"
    
    # Create async session with proper context manager
    class AsyncSessionMock:
        async def get(self, *args, **kwargs):
            return AsyncContextManagerMock(mock_response)
            
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, *args):
            pass
    
    session = AsyncSessionMock()
    
    # Disable caching and retries
    content_analyzer._save_to_cache = AsyncMock()
    content_analyzer._load_from_cache = AsyncMock(return_value=None)
    content_analyzer.max_retries = 0
    
    result = await content_analyzer.analyze_url(session, youtube_url)
    assert result.title == "Fallback Title" 