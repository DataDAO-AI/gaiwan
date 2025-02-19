# Twitter Archive Export Package

This package provides a flexible framework for exporting Twitter archive data into various formats. It implements a clean object-oriented design with a base `Exporter` class and several specialized exporters.

## Features

- Multiple export formats supported:
  - Markdown (human-readable format with media embeds)
  - JSONL (line-delimited JSON format)
  - ChatML (OpenAI-compatible chat format)
  - OpenAI (base format for AI training data)

## Installation

```bash
pip install gaiwan-twitter-archive-processor
```

## Quick Start

```python
from gaiwan.twitter_archive_processor.export import MarkdownExporter, ChatMLExporter
from pathlib import Path

# Export to Markdown
exporter = MarkdownExporter()
exporter.export_tweets(tweets, Path("output.md"))

# Export thread to ChatML
chatml = ChatMLExporter(system_message="You are a helpful assistant.")
chatml.export_thread(conversation, Path("conversation.json"))
```

## Architecture

### Class Hierarchy
```
Exporter (ABC)
├── MarkdownExporter
├── JSONLExporter
└── OpenAIExporter
    └── ChatMLExporter
```

### Base Class Interface

```python
class Exporter:
    def __init__(self, system_message: str = None):
        """
        Initialize exporter with optional system message for AI formats
        Args:
            system_message (str, optional): System message for AI training formats
        """
        
    def export_tweets(self, tweets: List[BaseTweet], output_path: Path) -> None:
        """
        Export a list of tweets to specified format
        Args:
            tweets (List[BaseTweet]): List of tweets to export
            output_path (Path): Output file path
        """
        
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """
        Export a conversation thread to specified format
        Args:
            thread (ConversationThread): Thread to export
            output_path (Path): Output file path
        """
```

## Input Types

### BaseTweet Properties
```python
class BaseTweet:
    id: str                     # Tweet ID
    text: str                   # Tweet text content
    created_at: datetime        # Tweet timestamp (UTC)
    media: List[Dict]          # Media attachments
    metadata: TweetMetadata    # Additional metadata
```

### ConversationThread Properties
```python
class ConversationThread:
    root_tweet: BaseTweet           # First tweet in thread
    replies: List[BaseTweet]        # Subsequent replies
    created_at: datetime            # Thread start time
    all_tweets: List[BaseTweet]     # All tweets in order
```

## Output Formats

### Markdown Format
```markdown
# Tweet {id}

{tweet_text}

Posted on: {YYYY-MM-DD HH:MM:SS}

![{media_type}]({media_url})
```

### JSONL Format
```json
{
  "id": "tweet_id",
  "text": "tweet_text",
  "created_at": "ISO-8601-timestamp",
  "media": [{
    "type": "media_type",
    "media_url": "url"
  }],
  "metadata": {}
}
```

### ChatML Format
```json
{
  "messages": [
    {"role": "system", "content": "system_message"},
    {"role": "user", "content": "tweet_text"},
    {"role": "assistant", "content": "reply_text"}
  ]
}
```

### OpenAI Format
```jsonl
{"messages":[{"role":"system","content":"system_message"},{"role":"user","content":"tweet_text"}]}
```

## Exporter-Specific Features

### MarkdownExporter
- Chronological sorting of tweets
- Headers with tweet IDs
- Formatted timestamps
- Embedded media with alt text
- Thread-specific formatting with timestamps

### JSONLExporter
- One tweet per line
- Complete metadata preservation
- ISO-8601 timestamp format
- Full media information
- Thread export as single conversation

### ChatMLExporter
- Alternating user/assistant roles
- Pretty-printed JSON
- Media URLs included in content
- Timestamp annotations
- System message support

### OpenAIExporter
- JSONL conversation format
- Basic text cleaning
- System message prefix
- Bulk conversation export

## Maintaining This README

This README should be updated when:
1. New exporters are added
2. Input/output formats change
3. New features are added to existing exporters
4. Class interfaces are modified

Update checklist:
- [ ] Class hierarchy diagram
- [ ] Input type documentation
- [ ] Output format examples
- [ ] Exporter-specific features
- [ ] Usage examples
- [ ] Installation instructions

## Error Handling

All exporters handle:
- Missing timestamps (uses UTC min date)
- Missing media information (skips media block)
- UTF-8 encoding issues
- File I/O errors with appropriate logging

## Testing

Run tests with:
```bash
pytest tests/export/
```

Test coverage includes:
- Single tweet exports
- Thread exports
- Media handling
- Missing data handling
- Format validation
- File I/O

## Dependencies

- Python 3.6+
- Standard library only (json, pathlib, datetime)
- Optional: pytest for testing

## License

MIT License - See LICENSE file for details