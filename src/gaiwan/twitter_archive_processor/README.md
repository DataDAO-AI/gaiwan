# Twitter Archive Processor

A comprehensive framework for processing, analyzing, and exporting Twitter archive data. This package provides modular components for handling tweets, conversations, URLs, and various export formats.

## Features

- Archive processing:
  - Multiple archive format support
  - Tweet parsing and normalization
  - Conversation thread reconstruction
  - Media attachment handling
- Analysis capabilities:
  - URL extraction and analysis
  - Domain normalization
  - Content fetching and analysis
  - Tweet metadata analysis
- Export formats:
  - Markdown (human-readable)
  - JSONL (line-delimited JSON)
  - ChatML (OpenAI chat format)
  - OpenAI format (training data)

## Installation

```bash
pip install gaiwan-twitter-archive-processor
```

## Quick Start

```python
from gaiwan.twitter_archive_processor import ArchiveProcessor
from pathlib import Path

# Initialize processor
processor = ArchiveProcessor(archive_dir=Path("path/to/archives"))

# Load archives
processor.load_archives()

# Export in different formats
processor.export_all("markdown", Path("output/markdown"))
processor.export_all("chatml", Path("output/chatml"))

# Analyze URLs
url_analysis = processor.analyze_urls()
```

## Architecture

### Class Hierarchy
```
ArchiveProcessor
├── Archive
│   ├── TweetFactory
│   │   ├── StandardTweet
│   │   ├── NoteTweet
│   │   └── BaseTweet
│   └── ConversationThread
├── URLAnalyzer
└── Exporters
    ├── MarkdownExporter
    ├── JSONLExporter
    ├── OpenAIExporter
    └── ChatMLExporter
```

### Core Classes

#### ArchiveProcessor
```python
class ArchiveProcessor:
    def __init__(self, archive_dir: Path):
        """
        Initialize archive processor
        Args:
            archive_dir (Path): Directory containing archives
        """
    
    def load_archives(self) -> None:
        """Load all archives from directory"""
        
    def export_all(
        self, 
        format_type: str,
        output_dir: Path,
        system_message: str = None
    ) -> None:
        """Export archives in specified format"""
        
    def analyze_urls(self) -> pd.DataFrame:
        """Analyze URLs in all archives"""
```

#### Archive
```python
class Archive:
    def __init__(self, file_path: Path):
        """
        Initialize archive
        Args:
            file_path (Path): Path to archive JSON file
        """
    
    def load(self) -> None:
        """Load archive data"""
        
    def get_conversation_threads(self) -> List[ConversationThread]:
        """Extract conversation threads"""
        
    def export(
        self,
        format: str,
        output_path: Path,
        system_message: str = None
    ) -> None:
        """Export archive in specified format"""
```

## Data Models

### BaseTweet
```python
@dataclass
class BaseTweet:
    id: str
    text: str
    created_at: Optional[datetime]
    media: List[Dict]
    parent_id: Optional[str]
    metadata: TweetMetadata
    
    def clean_text(self) -> str: ...
    def get_urls(self) -> Set[str]: ...
    def get_mentions(self) -> Set[str]: ...
    def get_hashtags(self) -> Set[str]: ...
```

### ConversationThread
```python
@dataclass
class ConversationThread:
    root_tweet: BaseTweet
    replies: List[BaseTweet]
    created_at: datetime
    
    @property
    def all_tweets(self) -> List[BaseTweet]: ...
    @property
    def length(self) -> int: ...
```

## Input Format

### Archive JSON Structure
```json
{
    "account": [{
        "account": {
            "username": "string",
            "accountId": "string"
        }
    }],
    "tweets": [{
        "tweet": {
            "id_str": "string",
            "full_text": "string",
            "created_at": "string",
            "entities": {
                "urls": [],
                "user_mentions": [],
                "hashtags": []
            },
            "extended_entities": {
                "media": []
            }
        }
    }]
}
```

## Export Formats

See individual format documentation:
- [Export Package Documentation](export/README.md)
- [URL Analysis Documentation](url_analysis/README.md)
- [Tweet Package Documentation](tweets/README.md)

## Configuration

### Config File Location
```python
DEFAULT_CONFIG_PATH = Path.home() / ".config" / "twitter_archive_processor" / "config.json"
```

### Config Format
```json
{
    "api_keys": {
        "youtube": "string",
        "github": "string"
    }
}
```

## Maintaining This README

This README should be updated when:
1. New components are added
2. Core interfaces change
3. Input/output formats change
4. Configuration options are modified

Update checklist:
- [ ] Class hierarchy diagram
- [ ] Component documentation
- [ ] Input format examples
- [ ] Configuration options
- [ ] API examples

## Error Handling

The package handles:
- Invalid archive formats
- Missing tweet data
- Malformed timestamps
- Media attachment errors
- Export format errors
- Configuration issues
- API rate limits

## Testing

Run tests with:
```bash
pytest tests/
```

Test coverage includes:
- Archive loading
- Tweet parsing
- Thread reconstruction
- URL analysis
- Export formats
- Error cases

## Dependencies

Required:
- Python 3.6+
- pandas
- aiohttp
- beautifulsoup4
- tqdm
- orjson

Optional:
- pytest for testing

## Command Line Interface

```bash
python -m gaiwan.twitter_archive_processor \
    path/to/archives \
    path/to/output \
    --format markdown chatml \
    --system-message "You are a helpful assistant."
```

Options:
- `archive_dir`: Directory containing archives
- `output_dir`: Output directory
- `--format`: Output formats (multiple allowed)
- `--system-message`: System message for AI formats
- `--debug`: Enable debug logging

## License

MIT License - See LICENSE file for details