# Twitter Archive Export Package

This package provides a flexible framework for exporting Twitter archive data into various formats. It implements a clean object-oriented design with a base `Exporter` class and several specialized exporters.

## Features

- Multiple export formats supported:
  - Markdown (human-readable format with media embeds)
  - JSONL (line-delimited JSON format)
  - ChatML (OpenAI-compatible chat format)
  - OpenAI (base format for AI training data)

- Support for both individual tweets and conversation threads
- Consistent handling of media attachments
- Chronological ordering of tweets
- Configurable system messages for AI-training formats

## Architecture

The package follows a clean object-oriented design with:

- Abstract base class (`Exporter`) defining the interface
- Concrete implementations for each format
- Inheritance hierarchy for related formats (OpenAI -> ChatML)

### Class Hierarchy
Exporter (ABC)├── MarkdownExporter
├── JSONLExporter
└── OpenAIExporter
└── ChatMLExporter
```

## Usage

```python
from gaiwan.twitter_archive_processor.export import MarkdownExporter, ChatMLExporter

# Export to Markdown
markdown_exporter = MarkdownExporter()
markdown_exporter.export_tweets(tweets, "output.md")
markdown_exporter.export_thread(conversation_thread, "thread.md")

# Export to ChatML
chatml_exporter = ChatMLExporter(system_message="Custom system message")
chatml_exporter.export_tweets(tweets, "output.json")
chatml_exporter.export_thread(conversation_thread, "thread.json")
```

## Format Details

### Markdown Format
- Chronologically ordered tweets
- Headers with timestamps
- Embedded media with alt text
- Thread-specific formatting

### JSONL Format
- One JSON object per line
- Preserves all tweet metadata
- ISO-formatted timestamps
- Complete media information

### ChatML Format
- OpenAI-compatible chat format
- Alternating user/assistant roles for threads
- Pretty-printed JSON output
- Configurable system message

### OpenAI Format
- Base format for AI training
- JSONL-based conversation format
- System message support
- Clean text processing

## Testing

The package includes comprehensive tests for all exporters, covering:
- Single tweet exports
- Thread exports
- Chronological ordering
- Media handling
- Missing timestamp handling
- Format-specific features

## Dependencies

- Python 3.6+
- Standard library (json, pathlib, datetime)
- No external dependencies required

## Notes

- All exporters handle missing timestamps gracefully
- Media attachments are preserved across all formats
- Thread structure is maintained in conversational formats
- UTF-8 encoding is used consistently