# Gaiwan

A comprehensive framework for analyzing Twitter/X archives, with support for multiple formats, data analysis, and export capabilities.

## Features

- Archive Processing:
  - Multiple archive format support
  - Tweet parsing and normalization
  - Thread reconstruction
  - Media attachment handling
- Analysis Tools:
  - URL extraction and analysis
  - Domain normalization
  - Content fetching
  - Schema validation
- Export Formats:
  - Markdown
  - JSONL
  - ChatML
  - OpenAI format
- Schema Management:
  - Format detection
  - Version tracking
  - Migration support
  - Validation rules
- Identity Management:
  - User ID tracking
  - Username history tracking
  - Display name changes
  - Avatar tracking
  - Historical identity querying
  - Cross-archive identity merging
  - Point-in-time identity lookup
  - Temporary ID generation
  - Bidirectional mappings

## Installation

```bash
pip install gaiwan
```

## Quick Start

```python
from gaiwan import ArchiveProcessor
from pathlib import Path

# Initialize processor
processor = ArchiveProcessor(archive_dir=Path("path/to/archives"))

# Process archives
processor.load_archives()

# Export conversations
processor.export_all("markdown", Path("output/markdown"))

# Analyze URLs
url_analysis = processor.analyze_urls()
```

## Package Structure

```
gaiwan/
├── twitter_archive_processor/
│   ├── core/               # Core processing functionality
│   │   ├── archive.py
│   │   ├── processor.py
│   │   ├── metadata.py
│   │   └── conversation.py
│   ├── schema/            # Schema validation and management
│   │   ├── validator.py
│   │   ├── inspector.py
│   │   └── generator.py
│   ├── tweets/            # Tweet type implementations
│   │   ├── base.py
│   │   ├── factory.py
│   │   └── types/
│   ├── export/           # Export format handlers
│   │   ├── markdown.py
│   │   ├── chatml.py
│   │   └── oai.py
│   ├── utils/            # Utility functions
│   │   ├── config.py
│   │   ├── logging.py
│   │   ├── rate_limiter.py
│   │   └── cache.py
│   └── url_analysis/     # URL processing
├── Schema Inspection/    # Schema analysis tools
└── README.md

tests/                   # Centralized test directory
├── test_core/          # Core functionality tests
├── test_tweets/        # Tweet handling tests
├── test_export/        # Export format tests
├── test_url_analysis/  # URL analysis tests
└── test_utils/         # Utility function tests
```

## Recent Changes

1. Reorganized package structure:
   - Moved core functionality into `core/` subpackage
   - Created dedicated `schema/` subpackage
   - Added `utils/` subpackage for common utilities

2. Unified testing framework:
   - Centralized all tests in `tests/` directory
   - Organized by component
   - Added comprehensive test fixtures
   - Improved test coverage

3. Enhanced modularity:
   - Separated concerns between subpackages
   - Improved encapsulation
   - Cleaner interfaces between components

## Core Components

### Twitter Archive Processor

Main package for processing Twitter archives. See [detailed documentation](twitter_archive_processor/README.md).

```python
from gaiwan.twitter_archive_processor import ArchiveProcessor

processor = ArchiveProcessor(archive_dir)
processor.load_archives()
processor.export_all("markdown", output_dir)
```

### Schema Inspector

Tools for analyzing and validating archive schemas.

```python
from gaiwan.schema_inspection import inspect_archive

# Analyze archive structure
schema = inspect_archive("path/to/archive.json")

# Validate against known format
is_valid = schema.validate()
```

### URL Analyzer

Advanced URL analysis with content fetching. See [detailed documentation](twitter_archive_processor/url_analysis/README.md).

```python
from gaiwan.twitter_archive_processor.url_analysis import URLAnalyzer

analyzer = URLAnalyzer(archive_dir)
results = analyzer.analyze_archives()
```

## Input Formats

### Archive JSON Structure
```json
{
    "account": [{
        "account": {
            "username": "string",
            "accountId": "string",
            "createdAt": "string",
            "accountDisplayName": "string"
        }
    }],
    "tweets": [{
        "tweet": {
            "id_str": "string",
            "full_text": "string",
            "created_at": "string",
            "entities": {...},
            "extended_entities": {...}
        }
    }],
    "note-tweet": [{
        "noteTweet": {
            "noteTweetId": "string",
            "core": {
                "text": "string",
                "urls": [...],
                "mentions": [...],
                "hashtags": [...]
            },
            "createdAt": "string"
        }
    }],
    "like": [{
        "like": {
            "tweetId": "string",
            "fullText": "string",
            "expandedUrl": "string"
        }
    }]
}
```

## Command Line Tools

### Archive Processor
```bash
python -m gaiwan.twitter_archive_processor \
    path/to/archives \
    path/to/output \
    --format markdown chatml
```

### Schema Inspector
```bash
python -m gaiwan.schema_inspection.inspect_json \
    path/to/archive.json
```

### URL Analyzer
```bash
python -m gaiwan.twitter_archive_processor.url_analysis \
    path/to/archives \
    --output urls.parquet
```

## Configuration

### Default Paths
```python
CONFIG_PATH = Path.home() / ".config" / "gaiwan" / "config.json"
CACHE_PATH = Path.home() / ".cache" / "gaiwan"
```

### Config Format
```json
{
    "api_keys": {
        "youtube": "string",
        "github": "string"
    },
    "cache": {
        "ttl_days": 30,
        "max_size_mb": 1000
    }
}
```

## Maintaining This Package

### Documentation Updates
Update READMEs when:
1. New components are added
2. Core interfaces change
3. Input/output formats change
4. Configuration options are modified

### Version Control
- Follow semantic versioning
- Document breaking changes
- Maintain changelog
- Update migration guides

### Testing Requirements
- Unit tests for new components
- Integration tests for features
- Schema validation tests
- Performance benchmarks

## Error Handling

The package handles:
- Invalid archive formats
- Missing or malformed data
- Network timeouts
- Rate limiting
- Resource constraints
- API errors
- Schema violations

## Dependencies

Required:
- Python 3.6+
- pandas
- aiohttp
- beautifulsoup4
- tqdm
- orjson
- duckdb

Optional:
- pytest for testing
- jupyter for notebooks

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Update documentation
5. Submit pull request

## License

MIT License - See LICENSE file for details