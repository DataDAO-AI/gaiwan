"""Tests for archive downloading functionality."""

import json
import logging
from pathlib import Path
import random
import tempfile
import shutil
from datetime import datetime, timezone

import pytest
import orjson

from gaiwan.community_archiver import (
    download_archive, get_archive_metadata, 
    get_all_accounts, merge_archives, SUPABASE_URL
)

@pytest.fixture(scope="session")
def test_archive(tmp_path_factory) -> Path:
    """Create a test archive from brentbaum's data."""
    fixtures_dir = Path("tests/fixtures")
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    
    archive_path = fixtures_dir / "brentbaum_archive.json"
    if not archive_path.exists():
        # Copy from local archive if available
        local_archive = Path("/Users/george/x_community_archive/_brentbaum_archive.json")
        if local_archive.exists():
            shutil.copy2(local_archive, archive_path)
        else:
            # Download from Supabase as fallback
            metadata = get_archive_metadata("brentbaum")
            if metadata and 'content' in metadata:
                archive_path.write_bytes(metadata['content'])
            else:
                raise RuntimeError("Could not get test archive data")
    
    # Load and ensure _metadata exists
    with open(archive_path, 'rb') as f:
        data = orjson.loads(f.read())
    
    if '_metadata' not in data:
        data['_metadata'] = {
            'size': str(len(archive_path.read_bytes())),
            'url': f"{SUPABASE_URL}/storage/v1/object/public/archives/brentbaum/archive.json"
        }
        archive_path.write_bytes(orjson.dumps(data))
        
    return archive_path

@pytest.fixture(autouse=True)
def setup_logging():
    """Enable debug logging for all tests."""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def test_get_metadata():
    """Test fetching archive metadata using real Supabase data."""
    # Use a known good account that has data
    metadata = get_archive_metadata("brentbaum")  # Will try _brentbaum
    assert metadata is not None
    assert 'etag' in metadata
    assert 'size' in metadata
    assert 'last_modified' in metadata
    assert int(metadata['size']) > 1000  # Real archives are bigger than 1KB

def test_download_archive(tmp_path):
    """Test downloading an archive using real data."""
    archive_path, metadata = download_archive("brentbaum", tmp_path)
    assert archive_path is not None
    assert metadata is not None
    assert archive_path.exists()
    
    # Verify we got real content
    with open(archive_path, 'rb') as f:
        data = orjson.loads(f.read())
        assert '_metadata' in data
        assert 'tweets' in data
        assert len(data['tweets']) > 0
        # Verify tweet structure
        tweet = data['tweets'][0]
        assert 'tweet' in tweet
        assert 'id_str' in tweet['tweet']

def test_get_all_accounts():
    """Test fetching account list from Supabase."""
    accounts = get_all_accounts()
    assert len(accounts) > 0
    # Verify we got real accounts with exact usernames
    assert any(a['username'] == '_brentbaum' for a in accounts)

def test_merge_archives(test_archive):
    """Test merging archives using real data."""
    with open(test_archive, 'rb') as f:
        original_data = orjson.loads(f.read())
    
    # Create two partial archives by removing different tweets
    collections = ['tweets', 'community_tweets', 'note_tweets', 'likes']
    partial_data = original_data.copy()
    partial_data_2 = original_data.copy()
    
    removal_stats = {}
    for collection in collections:
        if collection in original_data:
            items = original_data[collection]
            if not items:  # Skip empty collections
                continue
                
            # Remove different items from each partial archive
            size = len(items)
            remove_count = size // 3  # Remove 1/3 of items
            
            # For first partial: remove first third
            partial_data[collection] = items[remove_count:]
            
            # For second partial: remove last third
            partial_data_2[collection] = items[:-remove_count]
            
            removal_stats[collection] = {
                'original': size,
                'partial_1': len(partial_data[collection]),
                'partial_2': len(partial_data_2[collection])
            }
    
    # Merge the partial archives
    merged_data = merge_archives(partial_data, partial_data_2)
    
    # Verify the merge restored all items
    for collection in collections:
        if collection not in original_data:
            continue
            
        original_items = original_data[collection]
        if not original_items:  # Skip empty collections
            continue
            
        # Sort both lists by ID for comparison
        original_items = sorted(original_items, 
                             key=lambda x: x['tweet']['id_str'])
        merged_items = sorted(merged_data[collection], 
                            key=lambda x: x['tweet']['id_str'])
        
        assert len(merged_items) == len(original_items), (
            f"Merged {collection} count ({len(merged_items)}) "
            f"doesn't match original ({len(original_items)})"
        )
        
        # Verify each item matches exactly
        for orig, merged in zip(original_items, merged_items):
            assert orig == merged, (
                f"Merged {collection} item doesn't match original"
            )

def test_merge_preserves_local_modifications(test_archive):
    """Test that merging preserves local modifications using real data."""
    with open(test_archive, 'rb') as f:
        original_data = orjson.loads(f.read())
    
    # Add local modifications
    original_data['_metadata']['local_notes'] = "Test notes"
    original_data['_metadata']['local_tags'] = ['test', 'archive']
    
    # Create new data with different metadata
    new_data = original_data.copy()
    new_data['_metadata'] = {
        'size': '12345',
        'url': f"{SUPABASE_URL}/storage/v1/object/public/archives/brentbaum/archive.json",
        'remote_change': 'should be preserved',
        'merged_at': datetime.now(timezone.utc).isoformat()
    }
    
    # Merge archives
    merged_data = merge_archives(original_data, new_data)
    
    # Verify local modifications are preserved
    assert merged_data['_metadata']['local_notes'] == "Test notes"
    assert merged_data['_metadata']['local_tags'] == ['test', 'archive']
    
    # Verify new metadata is also present
    assert merged_data['_metadata']['size'] == '12345'
    assert merged_data['_metadata']['url'].endswith('/brentbaum/archive.json')
    assert merged_data['_metadata']['remote_change'] == 'should be preserved'