import pytest
import asyncio
from pathlib import Path
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import json
from unittest.mock import Mock, patch
from contextlib import asynccontextmanager

from gaiwan.twitter_archive_processor.url_analysis.content import ContentAnalyzer, PageContent
from .test_utils import create_mock_response

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
    return ContentAnalyzer(cache_dir=temp_cache_dir)

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

@pytest.mark.asyncio
async def test_cache_behavior(content_analyzer, tmp_path):
    # Override cache directory for testing
    content_analyzer.cache_dir = tmp_path
    urls = [f"https://example{i}.com" for i in range(3)]
    
    # Mock response for initial requests
    class MockResponse:
        def __init__(self):
            self.status = 200
            self.headers = {"content-type": "text/html"}
            self._text = "<html><title>Test</title></html>"
        async def text(self):
            return self._text
            
    class MockSession:
        call_count = 0
        
        @asynccontextmanager
        async def get(self, *args, **kwargs):
            self.call_count += 1
            yield MockResponse()
            
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, *args):
            pass
    
    session = MockSession()
    
    # First run - should make actual requests
    with patch('aiohttp.ClientSession', return_value=session):
        results1 = await content_analyzer.analyze_urls(urls)
        assert session.call_count == 3  # One call per URL
        
        # Second run - should use cache
        results2 = await content_analyzer.analyze_urls(urls)
        assert session.call_count == 3  # No new calls
        
        # Verify results match
        assert results1.keys() == results2.keys()
        for url in urls:
            assert results1[url].title == results2[url].title

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