# Gaiwan
Twitter/X Archive Analysis Framework

## Package Structure
gaiwan/
├── schema_inspection/ # Schema analysis and validation
├── twitter_archive_processor/
│ ├── export/ # Export format handlers
│ ├── tweets/ # Tweet processing
│ └── url_analysis/ # URL analysis and APIs
└── README.md
## Core Features
Reference implementation:
## Usage Context for AI
When building new packages:

1. Package Organization:
   - Follow modular architecture pattern
   - Implement clear class hierarchies
   - Use consistent file naming

2. Code Standards:
   - Include type hints
   - Add comprehensive docstrings
   - Follow error handling patterns
   - Implement logging

3. Testing Requirements:
   - Unit tests for new classes
   - Integration tests for features
   - Test error conditions
   - Document test coverage

4. Documentation:
   - Update package README
   - Include usage examples
   - Document class relationships
   - Maintain API documentation

## Implementation Patterns
- Error handling: See url_analyzer.py
- Async processing: See url_analysis/analyzer.py
- Class hierarchy: See export/README.md
- Data processing: See archive.py

# Schema Inspection Package
Tools for analyzing and validating Twitter archive schemas.

## Features
- Schema validation
- Format detection
- Version tracking
- Migration support

## Usage Context for AI
When extending this package:

1. Schema Handling:
   - Support multiple archive formats
   - Implement version detection
   - Add migration paths
   - Validate schema integrity

2. Validation Rules:
   - Check required fields
   - Validate data types
   - Handle optional fields
   - Support custom validators

3. Error Reporting:
   - Detailed validation errors
   - Schema mismatch reports
   - Migration recommendations
   - Format detection results

## Input Requirements
- Twitter archive JSON files
- Schema definition files
- Version information
# Twitter Archive Processor

Core archive processing functionality.

## Components
Reference implementation:

1. ArchiveProcessor: Main processing class
2. ExportHandler: Handles different archive formats
3. TweetProcessor: Processes tweets and metadata
4. URLAnalyzer: Analyzes URLs and content
## Usage Context for AI
When extending this package:

1. Archive Processing:
   - Follow ArchiveProcessor patterns
   - Implement incremental processing
   - Handle multiple archive formats
   - Support concurrent processing

2. Data Flow:
   - Archive loading
   - Tweet extraction
   - URL analysis
   - Export formatting

3. Error Handling:
   - Archive validation errors
   - Processing failures
   - Export errors
   - Resource cleanup

## Subpackages
- export/: Format-specific exporters
- tweets/: Tweet processing logic
- url_analysis/: URL analysis tools
# Export Package

Reference implementation:

1. ExportHandler: Base class for format-specific exporters
2. JSONExporter: Handles JSON export
3. CSVExporter: Handles CSV export
4. HTMLExporter: Handles HTML export
## Usage Context for AI
When extending this package:

1. New Exporters:
   - Inherit from base Exporter
   - Implement required methods
   - Handle all tweet types
   - Support thread structure

2. Format Requirements:
   - Document format specification
   - Include example output
   - List supported features
   - Note any limitationst
# URL Analysis Package

Reference implementation:

1. URLAnalyzer: Base class for URL analysis
2. ContentAnalyzer: Analyzes URL content
3. MetadataExtractor: Extracts metadata from URLs
## API Subpackage
The APIs package provides interfaces for:
- URL resolution
- Content analysis
- Metadata extraction
- Cache management

### Usage Context for AI
When extending APIs:
1. Follow existing patterns for:
   - Rate limiting
   - Error handling
   - Caching
   - Async operations
2. Document all endpoints
3. Include usage examples
4. Ensure backward compatibility
# Tweets Package

Tweet processing and analysis functionality.

## Features
- Tweet parsing
- Thread reconstruction
- Quote tweet handling
- Media attachment processing

## Usage Context for AI
When extending this package:

1. Tweet Processing:
   - Handle all tweet types
   - Support thread linking
   - Process attachments
   - Maintain metadata

2. Data Structures:
   - Follow Tweet class pattern
   - Implement thread hierarchy
   - Support quote tweets
   - Handle missing data