# Twitter Archive URL Analysis Package

This package provides a comprehensive framework for analyzing URLs found in Twitter archives, including content analysis, domain normalization, and metadata extraction.

## Features

- URL extraction and analysis:
  - Extracts URLs from tweet text and entities
  - Resolves shortened URLs (t.co, bit.ly, etc.)
  - Normalizes domains for consistent analysis
  - Concurrent content fetching and analysis
- Content analysis:
  - Page title and description extraction
  - Link and image counting
  - Text content analysis
  - Content hashing for duplicate detection
- Caching and rate limiting:
  - Disk-based content caching
  - Configurable cache TTL
  - Smart rate limiting
  - Retry mechanisms

## Installation

```bash
pip install gaiwan-twitter-archive-processor
```

## Quick Start

```python
from gaiwan.twitter_archive_processor.url_analysis import URLAnalyzer
from pathlib import Path

# Initialize analyzer
analyzer = URLAnalyzer(
    archive_dir=Path("path/to/archives"),
    content_cache_dir=Path("path/to/cache")
)

# Analyze URLs in archives
df = analyzer.analyze_archives()

# Get domain statistics
domain_stats = analyzer.get_domain_stats()
```

## Architecture

### Class Hierarchy
```
URLAnalyzer
├── ContentAnalyzer
├── DomainNormalizer
└── URLAnalysisReporter
```

### Core Classes

#### URLAnalyzer
```python
class URLAnalyzer:
    def __init__(
        self, 
        archive_dir: Optional[Path] = None,
        content_cache_dir: Optional[Path] = None
    ):
        """
        Initialize URL analyzer
        Args:
            archive_dir (Path): Directory containing archives
            content_cache_dir (Path): Cache directory for content
        """
    
    def analyze_archives(self) -> pd.DataFrame:
        """Analyze URLs in all archives"""
        
    def get_domain_stats(self) -> pd.DataFrame:
        """Get domain frequency statistics"""
        
    async def analyze_content(
        self, 
        urls: List[str],
        url_pbar: tqdm
    ) -> Dict[str, PageContent]:
        """Analyze content of URLs concurrently"""
```

#### ContentAnalyzer
```python
class ContentAnalyzer:
    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Initialize content analyzer
        Args:
            cache_dir (Path): Directory for caching content
        """
    
    async def analyze_url(
        self,
        session: aiohttp.ClientSession,
        url: str
    ) -> PageContent:
        """Analyze content of a single URL"""
```

## Data Models

### PageContent
```python
@dataclass
class PageContent:
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    text_content: Optional[str] = None
    links: Set[str] = None
    images: Set[str] = None
    fetch_time: datetime = None
    content_hash: Optional[str] = None
    content_type: Optional[str] = None
    status_code: Optional[int] = None
    error: Optional[str] = None
```

### URLMetadata
```python
@dataclass
class URLMetadata:
    url: str
    title: Optional[str] = None
    fetch_status: str = 'not_attempted'
    fetch_error: Optional[str] = None
    content_type: Optional[str] = None
    last_fetch_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
```

## Output Formats

### DataFrame Columns
```python
{
    'username': str,          # Twitter username
    'tweet_id': str,          # Tweet ID
    'tweet_created_at': datetime,  # Tweet timestamp
    'url': str,              # Original URL
    'domain': str,           # Normalized domain
    'raw_domain': str,       # Original domain
    'protocol': str,         # URL protocol
    'path': str,             # URL path
    'query': str,            # Query parameters
    'fragment': str,         # URL fragment
    'fetch_status': str,     # Content fetch status
    'fetch_error': str,      # Error message if any
    'content_type': str,     # MIME type
    'title': str,            # Page title
    'description': str       # Page description
}
```

## Configuration

### Rate Limiting
```python
rate_limits = {
    429: 30,  # Too Many Requests
    403: 10,  # Forbidden
    405: 10   # Method Not Allowed
}
```

### Content Types
```python
skip_content_types = {
    'application/pdf',
    'application/zip',
    'image/',
    'video/',
    'audio/',
    'application/octet-stream'
}
```

## URL Processing Log

The package now maintains a persistent log of processed URLs to avoid redundant processing across multiple runs:

```python
analyzer = URLAnalyzer(
    archive_dir=Path("path/to/archives"),
    content_cache_dir=Path("path/to/cache")
)

# URLs are automatically logged when processed
await analyzer.analyze_archives()
```

### Log Format

The URL processing log is stored in CSV format:
```csv
url,timestamp,status
https://example.com,2024-01-20T12:34:56+00:00,success
https://failed.com,2024-01-20T12:34:57+00:00,failed
```

### Features

- Persistent tracking of processed URLs
- Automatic cache expiration based on TTL
- Separate tracking of successful and failed URLs
- Efficient batch processing with memory management
- CSV format for easy inspection and analysis

### Configuration

```python
content_analyzer = ContentAnalyzer(
    cache_dir=Path("path/to/cache"),
    config=Config(
        cache_ttl=timedelta(days=30)  # How long to consider URLs as processed
    )
)
```

### Memory Management

The analyzer now processes URLs in batches with automatic memory cleanup:

```python
# Configure batch size
analyzer.batch_size = 100  # URLs per batch

# Analyze archives with automatic batching
df = await analyzer.analyze_archives()
```

## Performance Improvements

Recent updates include:
- Batched URL processing
- Persistent URL processing log
- Memory optimization
- Concurrent processing with rate limiting
- Automatic garbage collection

## Error Handling

The system handles various scenarios:
- Network timeouts and errors
- Rate limiting responses
- Invalid URLs
- Binary content detection
- Cache read/write errors
- Memory management
- Concurrent request limits
- URL processing status tracking

## Testing

New test cases cover:
- URL processing log functionality
- Cache TTL behavior
- Memory management
- Batch processing
- Error scenarios

Run the new tests with:
```bash
pytest tests/url_analysis/test_content.py -k "test_url_processing"
```

## Maintaining This README

Update checklist:
- [x] URL processing log documentation
- [x] Memory management features
- [x] Configuration options
- [x] Test coverage
- [x] Performance improvements

## Dependencies

Required:
- Python 3.6+
- aiohttp
- beautifulsoup4
- pandas
- tqdm
- orjson

Optional:
- pytest for testing

## License

MIT License - See LICENSE file for details