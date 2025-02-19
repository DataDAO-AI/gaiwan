import pytest
from datetime import datetime, timezone, timedelta
import time

from gaiwan.twitter_archive_processor.url_analysis.metadata import URLMetadata

def test_metadata_initialization():
    metadata = URLMetadata(url="https://example.com")
    
    assert metadata.url == "https://example.com"
    assert metadata.fetch_status == "not_attempted"
    assert metadata.fetch_error is None
    assert metadata.title is None
    assert metadata.content_type is None
    assert metadata.last_fetch_time is None

def test_metadata_to_dict():
    metadata = URLMetadata(
        url="https://example.com",
        title="Test Page",
        content_type="text/html",
        fetch_status="success",
        last_fetch_time=datetime.now(timezone.utc)
    )
    
    data = metadata.to_dict()
    assert isinstance(data, dict)
    assert data['url'] == "https://example.com"
    assert data['title'] == "Test Page"
    assert data['content_type'] == "text/html"
    assert data['fetch_status'] == "success"
    assert isinstance(data['last_fetch_time'], str)

def test_mark_skipped():
    metadata = URLMetadata(url="https://example.com")
    metadata.mark_skipped("Binary content")
    
    assert metadata.fetch_status == "skipped"
    assert metadata.fetch_error == "Binary content"
    assert isinstance(metadata.last_fetch_time, datetime)
    assert metadata.last_fetch_time.tzinfo == timezone.utc

def test_mark_failed():
    metadata = URLMetadata(url="https://example.com")
    metadata.mark_failed("Connection timeout")
    
    assert metadata.fetch_status == "failed"
    assert metadata.fetch_error == "Connection timeout"
    assert isinstance(metadata.last_fetch_time, datetime)
    assert metadata.last_fetch_time.tzinfo == timezone.utc

def test_mark_success():
    metadata = URLMetadata(url="https://example.com")
    metadata.mark_success("text/html")
    
    assert metadata.fetch_status == "success"
    assert metadata.content_type == "text/html"
    assert metadata.fetch_error is None
    assert isinstance(metadata.last_fetch_time, datetime)
    assert metadata.last_fetch_time.tzinfo == timezone.utc

def test_metadata_status_transitions():
    metadata = URLMetadata(url="https://example.com")
    
    # Initial state
    assert metadata.fetch_status == "not_attempted"
    
    # Transition to failed
    metadata.mark_failed("Error")
    assert metadata.fetch_status == "failed"
    
    # Transition to success
    metadata.mark_success("text/html")
    assert metadata.fetch_status == "success"
    assert metadata.fetch_error is None
    
    # Transition to skipped
    metadata.mark_skipped("Skip reason")
    assert metadata.fetch_status == "skipped"
    
    # Verify timestamps are updated
    last_time = metadata.last_fetch_time
    time.sleep(0.001)  # Add a small delay
    metadata.mark_success("text/html")
    assert metadata.last_fetch_time > last_time 