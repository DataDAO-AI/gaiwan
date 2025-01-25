import pytest
from pathlib import Path
import json
import pandas as pd
from ..processor import ArchiveProcessor

@pytest.fixture
def sample_archives(tmp_path):
    # Create multiple sample archives
    archives_data = {
        "user1": {
            "account": [{"account": {"username": "user1"}}],
            "tweets": [{
                "tweet": {
                    "id_str": "123",
                    "full_text": "Test tweet with https://example.com",
                    "created_at": "Wed Feb 28 21:13:12 +0000 2024"
                }
            }]
        },
        "user2": {
            "account": [{"account": {"username": "user2"}}],
            "tweets": [{
                "tweet": {
                    "id_str": "456",
                    "full_text": "Another test with https://test.com",
                    "created_at": "Wed Feb 28 21:13:12 +0000 2024"
                }
            }]
        }
    }
    
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    
    for username, data in archives_data.items():
        archive_file = archive_dir / f"{username}_archive.json"
        with open(archive_file, 'w') as f:
            json.dump(data, f)
    
    return archive_dir

def test_processor_initialization(sample_archives):
    processor = ArchiveProcessor(sample_archives)
    assert processor.archive_dir == sample_archives
    assert len(processor.archives) == 0

def test_loading_archives(sample_archives):
    processor = ArchiveProcessor(sample_archives)
    processor.load_archives()
    
    assert len(processor.archives) == 2
    usernames = {archive.username for archive in processor.archives}
    assert usernames == {"user1", "user2"}

def test_url_analysis(sample_archives):
    processor = ArchiveProcessor(sample_archives)
    processor.load_archives()
    
    df = processor.analyze_urls()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # One URL from each archive
    assert "example.com" in df["domain"].values
    assert "test.com" in df["domain"].values

def test_export_all(sample_archives, tmp_path):
    processor = ArchiveProcessor(sample_archives)
    processor.load_archives()
    
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    processor.export_all("markdown", output_dir)
    
    # Check that export files were created
    expected_files = {
        output_dir / "user1_markdown",
        output_dir / "user2_markdown"
    }
    created_files = set(output_dir.glob("*_markdown"))
    assert created_files == expected_files

def test_nonexistent_archive_dir():
    processor = ArchiveProcessor(Path("nonexistent_dir"))
    processor.load_archives()  # Should not raise exception but log error
    assert len(processor.archives) == 0 