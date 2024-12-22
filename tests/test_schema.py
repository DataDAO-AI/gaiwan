"""Tests for schema generation."""

import json
from pathlib import Path
import pytest
from genson import SchemaBuilder
from gaiwan.canonicalize import generate_schema
from gaiwan.community_archiver import download_archives

# Test with accounts we know are in the archive
TEST_ACCOUNTS = [
    "visakanv",        # Has lots of tweets and likes
    "eigenrobot",      # Has community tweets
    "selentelechia"    # Has note tweets
]

@pytest.fixture
def live_archives(tmp_path):
    """Download real archives for testing."""
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    # Download test archives
    download_archives(TEST_ACCOUNTS, archive_dir)
    
    # Verify we got some data
    archives = list(archive_dir.glob("*_archive.json"))
    if not archives:
        pytest.skip("Failed to download any test archives")
    
    # Print what we got
    print("\nDownloaded archives:")
    for archive in archives:
        size_mb = archive.stat().st_size / (1024 * 1024)
        print(f"{archive.name}: {size_mb:.1f}MB")
        
    return archive_dir

def test_schema_generation(live_archives, tmp_path):
    """Test schema generation from live archives."""
    schema_file = tmp_path / "schema.json"
    generate_schema(live_archives, schema_file)
    
    # Verify schema was generated
    assert schema_file.exists()
    
    # Load and validate schema
    with open(schema_file) as f:
        schema = json.load(f)
    
    # Basic schema validation
    assert "$schema" in schema
    assert "type" in schema
    assert schema["type"] == "object"
    
    # Print schema structure
    print("\nGenerated schema structure:")
    print_schema_structure(schema)
    
    # Verify key sections are present
    assert "properties" in schema
    properties = schema["properties"]
    
    expected_sections = {
        "tweets",           # Regular tweets
        "community-tweet",  # Community tweets
        "note-tweet",      # Notes
        "profile",         # User profile
        "like"             # Liked tweets
    }
    
    for section in expected_sections:
        assert section in properties, f"Missing {section} section"
        print(f"\nSchema for {section}:")
        print_schema_structure(properties[section], depth=1)

def print_schema_structure(schema: dict, depth: int = 0, max_depth: int = 3):
    """Print a readable view of schema structure."""
    indent = "  " * depth
    
    if depth >= max_depth:
        print(f"{indent}...")
        return
        
    if isinstance(schema, dict):
        for key, value in schema.items():
            if key in ("type", "required"):
                print(f"{indent}{key}: {value}")
            elif isinstance(value, (dict, list)):
                print(f"{indent}{key}:")
                print_schema_structure(value, depth + 1, max_depth)
    elif isinstance(schema, list):
        for item in schema[:2]:  # Only show first 2 items
            print_schema_structure(item, depth, max_depth)
        if len(schema) > 2:
            print(f"{indent}...") 