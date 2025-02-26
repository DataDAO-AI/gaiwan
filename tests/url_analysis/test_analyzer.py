import pytest
from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock, patch
import asyncio

from gaiwan.twitter_archive_processor.url_analysis.analyzer import URLAnalyzer
from gaiwan.twitter_archive_processor.url_analysis.content import PageContent
from .test_utils import create_mock_response, async_mock_coro

@pytest.fixture
def temp_archive_dir(tmp_path):
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    return archive_dir

@pytest.fixture
def sample_tweet_data():
    return {
        "tweet": {
            "id_str": "123456789",
            "created_at": "Wed Oct 10 20:19:24 +0000 2018",
            "full_text": "Check out https://example.com and https://t.co/abc123",
            "entities": {
                "urls": [
                    {
                        "url": "https://t.co/abc123",
                        "expanded_url": "https://longurl.com/page"
                    }
                ]
            }
        }
    }

@pytest.fixture
def analyzer(temp_archive_dir):
    return URLAnalyzer(archive_dir=temp_archive_dir)

def create_archive_file(archive_dir: Path, username: str, tweets: list):
    archive_path = archive_dir / f"{username}_archive.json"
    with open(archive_path, 'w') as f:
        json.dump({"tweets": tweets}, f)
    return archive_path

def test_url_extraction(analyzer, sample_tweet_data):
    urls = analyzer.extract_urls_from_tweet(sample_tweet_data["tweet"])
    assert "https://example.com" in urls
    assert "https://longurl.com/page" in urls
    assert "https://t.co/abc123" in urls

@pytest.mark.asyncio
async def test_content_analysis(analyzer):
    urls = ["https://example.com", "https://test.com"]
    mock_content = PageContent(
        url="https://example.com",
        title="Test Page",
        content_type="text/html"
    )
    
    with patch('gaiwan.twitter_archive_processor.url_analysis.content.ContentAnalyzer.analyze_urls') as mock_analyze:
        mock_analyze.return_value = {url: mock_content for url in urls}
        results = await analyzer.analyze_content(urls)
        assert len(results) == 2

def test_analyze_archive(analyzer, temp_archive_dir, sample_tweet_data):
    archive_path = create_archive_file(temp_archive_dir, "testuser", [sample_tweet_data])
    
    df = analyzer.analyze_archive(archive_path)
    
    assert not df.empty
    assert len(df) == 3  # Three URLs from the sample tweet
    assert "testuser" in df["username"].values
    assert "123456789" in df["tweet_id"].values
    assert all(isinstance(dt, datetime) for dt in df["tweet_created_at"])

@pytest.mark.asyncio
async def test_analyze_archives(analyzer, temp_archive_dir, sample_tweet_data):
    """Test processing archives sequentially while maintaining URL deduplication."""
    # Create archive files with sample data
    archive1 = create_archive_file(temp_archive_dir, "user1", [sample_tweet_data])
    archive2 = create_archive_file(temp_archive_dir, "user2", [sample_tweet_data])
    
    # Create mock content for each URL in sample_tweet_data
    mock_contents = {
        "https://example.com": PageContent(
            url="https://example.com",
            title="Test Page 1",
            content_type="text/html"
        ),
        "https://longurl.com/page": PageContent(
            url="https://longurl.com/page",
            title="Test Page 2",
            content_type="text/html"
        )
    }
    
    processed_urls = set()
    
    async def mock_analyze_archive_urls(archive_name, urls, *args, **kwargs):
        # Track which URLs are processed for each archive
        nonlocal processed_urls
        result = {}
        for url in urls:
            if url in mock_contents:
                result[url] = mock_contents[url]
                processed_urls.add(url)
        return result
    
    with patch('gaiwan.twitter_archive_processor.url_analysis.content.ContentAnalyzer.analyze_archive_urls', 
               side_effect=mock_analyze_archive_urls):
        df = await analyzer._analyze_archives_async()
        
        # Verify sequential processing
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert len(df) == 4  # 2 URLs × 2 archives
        
        # Verify URL deduplication
        assert len(processed_urls) == 2  # Each unique URL should be processed only once
        assert df['url'].nunique() == 2
        
        # Verify archive context is maintained
        assert set(df['username'].unique()) == {"user1", "user2"}
        assert all(df['content_type'] == "text/html")
        assert set(df['title'].unique()) == {"Test Page 1", "Test Page 2"}

@pytest.mark.asyncio
async def test_archive_sequential_processing(analyzer, temp_archive_dir, sample_tweet_data):
    """Test that archives are processed one at a time."""
    # Create multiple archive files
    archives = [
        create_archive_file(temp_archive_dir, f"user{i}", [sample_tweet_data])
        for i in range(3)
    ]
    
    processing_order = []
    
    async def mock_analyze_archive_urls(archive_name, urls, *args, **kwargs):
        processing_order.append(archive_name)
        return {}
    
    with patch('gaiwan.twitter_archive_processor.url_analysis.content.ContentAnalyzer.analyze_archive_urls', 
               side_effect=mock_analyze_archive_urls):
        await analyzer._analyze_archives_async()
        
        # Verify archives were processed sequentially
        assert len(processing_order) == 3
        assert processing_order == sorted(processing_order)  # Archives processed in order

def test_url_resolution(analyzer):
    with patch('requests.Session.head') as mock_head:
        mock_response = Mock()
        mock_response.url = "https://example.com/page"
        mock_head.return_value = mock_response
        
        resolved = analyzer.resolve_url("https://t.co/abc123")
        assert resolved == "https://example.com/page"
        
        # Test caching
        resolved_again = analyzer.resolve_url("https://t.co/abc123")
        assert resolved_again == "https://example.com/page"
        mock_head.assert_called_once()  # Should use cached result

def test_error_handling(analyzer, temp_archive_dir):
    # Test invalid archive file
    invalid_path = temp_archive_dir / "invalid_archive.json"
    with open(invalid_path, 'w') as f:
        f.write("invalid json")
    
    df = analyzer.analyze_archive(invalid_path)
    assert df.empty

    # Test network error in URL resolution
    with patch('requests.Session.head') as mock_head:
        mock_head.side_effect = Exception("Network error")
        resolved = analyzer.resolve_url("https://t.co/error")
        assert resolved is None

@pytest.mark.asyncio
async def test_analyze_archives_empty_result(temp_archive_dir):
    """Test analyze_archives when no valid data is found."""
    analyzer = URLAnalyzer(archive_dir=temp_archive_dir)
    
    # Create an empty archive file
    archive_path = temp_archive_dir / "test_archive.json"
    with open(archive_path, 'w') as f:
        f.write('{"tweets": []}')
    
    result = analyzer.analyze_archives()
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == [
        'username', 'tweet_id', 'tweet_created_at', 'url',
        'domain', 'raw_domain', 'protocol', 'path',
        'query', 'fragment', 'title', 'description',
        'content_type', 'status_code', 'error'
    ] 