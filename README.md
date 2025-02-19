# Gaiwan - Twitter Archive Analysis Framework

A comprehensive framework for analyzing Twitter/X archives, with support for multiple formats, data analysis, and export capabilities.

## Overview

Gaiwan provides tools for:
- Processing Twitter archive data
- Analyzing conversations and threads
- Extracting and analyzing URLs
- Exporting to various formats
- Schema validation and management

## Key Features

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

## Command Line Usage

```bash
# Process archives
python -m gaiwan.twitter_archive_processor \
    path/to/archives \
    path/to/output \
    --format markdown chatml

# Analyze URLs
python -m gaiwan.twitter_archive_processor.url_analysis \
    path/to/archives \
    --output urls.parquet

# Inspect schema
python -m gaiwan.schema_inspection.inspect_json \
    path/to/archive.json
```

## Project Structure

```
gaiwan/
├── twitter_archive_processor/    # Core processing functionality
│   ├── export/                  # Export format handlers
│   ├── tweets/                  # Tweet processing
│   └── url_analysis/           # URL analysis and APIs
├── schema_inspection/          # Schema analysis tools
└── canonicalize.py            # Data canonicalization
```

## Configuration

Create a config file at `~/.config/gaiwan/config.json`:

```json
{
    "api_keys": {
        "youtube": "YOUR_KEY",
        "github": "YOUR_KEY"
    },
    "cache": {
        "ttl_days": 30,
        "max_size_mb": 1000
    }
}
```

## Documentation

- [Twitter Archive Processor](src/gaiwan/twitter_archive_processor/README.md)
- [URL Analysis](src/gaiwan/twitter_archive_processor/url_analysis/README.md)
- [Schema Inspection](src/gaiwan/schema_inspection/README.md)

## Development

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

### Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=gaiwan

# Run specific test file
pytest tests/test_processor.py
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Update documentation
5. Submit pull request

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

## License

MIT License - See LICENSE file for details