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
    create_archive_file(temp_archive_dir, "user1", [sample_tweet_data])
    create_archive_file(temp_archive_dir, "user2", [sample_tweet_data])
    
    mock_content = PageContent(
        url="https://example.com",
        title="Test Page",
        content_type="text/html"
    )
    
    async def mock_analyze(*args, **kwargs):
        return {"https://example.com": mock_content}
    
    with patch('gaiwan.twitter_archive_processor.url_analysis.content.ContentAnalyzer.analyze_urls', 
               new=mock_analyze):
        df = await analyzer._analyze_archives_async()
        assert not df.empty
        assert "user1" in df["username"].values

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