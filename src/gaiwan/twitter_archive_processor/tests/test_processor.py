import pytest
from pathlib import Path
import json
import pandas as pd
from ..processor import ArchiveProcessor
from ..conversation import ConversationThread
from ..tweets.types import StandardTweet
from ..metadata import TweetMetadata
from datetime import datetime, timezone

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
    markdown_dir = output_dir / "markdown"
    expected_files = {
        markdown_dir / "user1_markdown.md",
        markdown_dir / "user2_markdown.md"
    }
    created_files = set(markdown_dir.glob("*_markdown.md"))
    assert created_files == expected_files

def test_nonexistent_archive_dir():
    processor = ArchiveProcessor(Path("nonexistent_dir"))
    processor.load_archives()  # Should not raise exception but log error
    assert len(processor.archives) == 0 

def test_export_conversations_oai(sample_archives, tmp_path):
    processor = ArchiveProcessor(sample_archives)
    processor.load_archives()
    
    # Create a conversation thread in the first archive
    first_archive = processor.archives[0]
    tweet = StandardTweet(
        id="123",
        text="Hello world!",
        created_at=datetime.now(timezone.utc),
        media=[],
        parent_id=None,
        metadata=TweetMetadata(tweet_type="tweet", raw_data={}, urls=set())
    )
    reply = StandardTweet(
        id="456",
        text="Hello back!",
        created_at=datetime.now(timezone.utc),
        media=[],
        parent_id="123",
        metadata=TweetMetadata(tweet_type="tweet", raw_data={}, urls=set())
    )
    first_archive.tweets.extend([tweet, reply])
    
    output_path = tmp_path / "conversations.jsonl"
    processor.export_conversations_oai(
        output_path,
        system_message="Test message"
    )
    
    assert output_path.exists()
    with open(output_path) as f:
        conversations = [json.loads(line) for line in f]
    
    assert len(conversations) > 0
    for conv in conversations:
        assert 'messages' in conv
        assert conv['messages'][0]['role'] == 'system'
        assert conv['messages'][0]['content'] == 'Test message' 

def test_create_output_dirs(sample_archives, tmp_path):
    processor = ArchiveProcessor(sample_archives)
    
    output_dirs = processor.create_output_dirs(tmp_path)
    
    assert set(output_dirs.keys()) == {'markdown', 'oai', 'chatml'}
    for dir_path in output_dirs.values():
        assert dir_path.exists()
        assert dir_path.is_dir()

def test_export_all_formats(sample_archives, tmp_path):
    processor = ArchiveProcessor(sample_archives)
    processor.load_archives()

    # Add some test tweets to the archives to ensure there's content to export
    for archive in processor.archives:
        tweet = StandardTweet(
            id="123",
            text="Test tweet content",
            created_at=datetime.now(timezone.utc),
            media=[],
            parent_id=None,
            metadata=TweetMetadata(tweet_type="tweet", raw_data={}, urls=set())
        )
        archive.tweets.append(tweet)

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    system_message = "Test system message"

    for format_type in ['markdown', 'oai', 'chatml']:
        processor.export_all(format_type, output_dir, system_message)

        format_dir = output_dir / format_type
        assert format_dir.exists()
        assert format_dir.is_dir()

        # Check files were created with correct extensions
        if format_type == 'markdown':
            files = list(format_dir.glob("*.md"))
        elif format_type == 'oai':
            files = list(format_dir.glob("*.jsonl"))
        else:  # chatml
            files = list(format_dir.glob("*.json"))

        assert len(files) > 0
        # Verify each file matches expected username pattern
        for file in files:
            assert file.stem.startswith(('user1', 'user2'))
            assert file.stem.endswith(format_type)

def test_invalid_format(sample_archives, tmp_path):
    processor = ArchiveProcessor(sample_archives)
    processor.load_archives()
    
    with pytest.raises(ValueError, match="Unsupported format: invalid"):
        processor.export_all("invalid", tmp_path) 