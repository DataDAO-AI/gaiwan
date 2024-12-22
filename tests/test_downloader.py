"""Tests for archive downloading functionality."""

import json
import logging
from pathlib import Path

import pytest
import responses

from gaiwan.community_archiver import download_archive, get_archive_metadata, get_all_accounts

@pytest.fixture
def mock_supabase():
    """Mock Supabase responses."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        # Mock metadata request
        rsps.add(
            responses.HEAD,
            "https://fabxmporizzqflnftavs.supabase.co/storage/v1/object/public/archives/testuser/archive.json",
            headers={
                'last-modified': 'Wed, 13 Mar 2024 12:34:56 GMT',
                'content-length': '1000',
                'etag': '"test-etag"'
            }
        )
        
        # Mock archive download
        rsps.add(
            responses.GET,
            "https://fabxmporizzqflnftavs.supabase.co/storage/v1/object/public/archives/testuser/archive.json",
            json={
                "tweets": [
                    {
                        "tweet": {
                            "id_str": "123",
                            "created_at": "Wed Mar 13 12:34:56 +0000 2024",
                            "text": "Test tweet",
                            "entities": {}
                        }
                    }
                ]
            }
        )
        
        # Mock accounts list
        rsps.add(
            responses.GET,
            "https://fabxmporizzqflnftavs.supabase.co/rest/v1/account",
            json=[
                {"username": "testuser"},
                {"username": "otheruser"}
            ]
        )
        
        yield rsps

def test_get_metadata(mock_supabase):
    """Test fetching archive metadata."""
    metadata = get_archive_metadata("testuser")
    assert metadata is not None
    assert metadata['etag'] == '"test-etag"'
    assert metadata['size'] == '1000'

def test_download_archive(mock_supabase, tmp_path):
    """Test downloading an archive."""
    archive_path, metadata = download_archive("testuser", tmp_path)
    assert archive_path is not None
    assert metadata is not None
    assert archive_path.exists()
    
    # Verify content
    with open(archive_path) as f:
        data = json.load(f)
        assert '_metadata' in data
        assert data['tweets'][0]['tweet']['id_str'] == '123'

def test_get_all_accounts(mock_supabase):
    """Test fetching account list."""
    accounts = get_all_accounts()
    assert len(accounts) == 2
    assert accounts[0]['username'] == 'testuser' 