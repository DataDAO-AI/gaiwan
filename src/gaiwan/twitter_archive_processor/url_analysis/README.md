# Twitter Archive URL Analyzer

A Python package for analyzing URLs from Twitter archive data. This package processes Twitter archive JSON files to extract, analyze, and report on URLs shared in tweets.

## Features

- Extracts URLs from Twitter archive JSON files
- Resolves shortened URLs (t.co, bit.ly, etc.)
- Normalizes domain names for consistent analysis
- Performs concurrent content analysis of URLs:
  - Extracts page titles and descriptions
  - Counts embedded links and images
  - Analyzes text content
  - Generates content hashes for duplicate detection
- Generates comprehensive URL statistics and reports
- Supports incremental processing of archives
- Saves results in Parquet format for efficient storage and analysis
- Intelligent caching of web content to avoid redundant fetches

## Installation

```bash
pip install twitter-archive-processor
```

## Usage

### Command Line Interface

```bash
python -m gaiwan.twitter_archive_processor.url_analysis /path/to/archive/directory --output urls.parquet
```

Options:
- `archive_dir`: Directory containing Twitter archive JSON files
- `--output`: Path to save results (default: urls.parquet)
- `--debug`: Enable debug logging
- `--force`: Force reanalysis of all archives

### Python API

```python
from pathlib import Path
from gaiwan.twitter_archive_processor.url_analysis import URLAnalyzer

# Initialize analyzer with optional content cache directory
analyzer = URLAnalyzer(
    archive_dir=Path("/path/to/archive/directory"),
    content_cache_dir=Path("/path/to/cache")  # Optional
)

# Process archives and get results
df = analyzer.analyze_archives()

# DataFrame columns:
# Basic URL Information:
# - username: Twitter username from archive
# - tweet_id: Original tweet ID
# - tweet_created_at: Tweet timestamp
# - url: Original URL from tweet
# - domain: Normalized domain name
# - raw_domain: Original domain before normalization
# - protocol: URL protocol (http/https)
# - path: URL path
# - query: URL query parameters
# - fragment: URL fragment
#
# Content Analysis Results:
# - page_title: Title of the webpage
# - page_description: Meta description or summary
# - content_type: MIME type of the content
# - content_hash: SHA-256 hash of content
# - linked_urls: Number of links found on page
# - image_count: Number of images on page
# - fetch_status: Status of content analysis
# - fetch_error: Error message if fetch failed
# - fetch_time: Timestamp of content analysis
```

## Input Requirements

- Directory containing Twitter archive JSON files
- Each archive file should be named `{username}_archive.json`
- Archive files should follow Twitter's JSON format containing:
  - `tweets` array
  - Each tweet object containing `tweet` with `id_str`, `created_at`, `full_text`, and `entities`

## Outputs

1. **Parquet File**
   - Contains detailed URL and content analysis data in a pandas DataFrame
   - Saved to specified output path (default: urls.parquet)

2. **Analysis Report**
   - Overall statistics
   - URL resolution status
   - Domain analysis
   - Protocol statistics
   - Content analysis metrics

3. **Debug Log**
   - Detailed URL resolution logs saved to `url_resolution.log`
   - Contains information about failed resolutions and errors

4. **Content Cache**
   - Cached web content and metadata
   - Configurable TTL (default: 30 days)
   - Reduces redundant fetches and respects rate limits

## Dependencies

- pandas
- requests
- urllib3
- orjson
- tqdm
- beautifulsoup4
- aiohttp
- aiofiles
- pyarrow

## Package Structure

```
gaiwan/twitter_archive_processor/url_analysis/
├── __init__.py          # Package exports
├── analyzer.py          # Core URL analysis logic
├── cli.py              # Command-line interface
├── content.py          # Content analysis and caching
├── domain.py           # Domain normalization
└── metadata.py         # URL metadata handling
```

## Error Handling

- Failed URL resolutions are logged but don't halt processing
- Backup files are created before overwriting existing results
- Incremental processing supports resuming interrupted analysis
- Content analysis failures are captured and reported
- Rate limiting and retry logic for web requests

## Performance Considerations

- Asynchronous content fetching for improved throughput
- Configurable concurrency limits
- Intelligent caching to reduce network requests
- Streaming response handling for memory efficiency
- Binary content detection and skipping

## License

[Add your license information here]