# Gaiwan TODO

# Instructions for AI Assistant
When updating this TODO list:
1. Use checkboxes [x] for completed items, [ ] for incomplete items
2. For completed items, add implementation location in parentheses:
   - Filename
   - Line numbers
   - Git commit number (if available)
3. Keep existing categories organized
4. Add new categories as needed
5. Maintain hierarchy of tasks and subtasks
6. Keep implementation references up to date

## Content Analysis
- [x] URL extraction and normalization (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 76-81)
- [x] Domain grouping and pattern matching (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 47-52)
- [x] Shortened URL resolution (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 206-215)
- [x] Page title extraction (src/gaiwan/url_analyzer.py, lines 412-431)
- [x] Content type detection (src/gaiwan/url_analyzer.py, lines 424-429)
- [ ] Meta description extraction
  - [ ] Implement OpenGraph parsing
  - [ ] Add fallback to first paragraph
  - [ ] Handle different meta description formats
- [ ] Keyword Analysis
  - [ ] Implement keyword extraction
  - [ ] Add relevance scoring
  - [ ] Consider language detection
  - [ ] Add topic clustering

## Thread Analysis
- [ ] Quote Tweet Investigation
  - [ ] Run schema generator on personal archive
  - [ ] Analyze URL entities to detect quote tweets
  - [ ] Extract quoted tweet IDs from URLs
  - [ ] Handle edge cases (deleted/private tweets)
  - [ ] Update CanonicalTweet implementation
  - [ ] Add comprehensive tests
- [x] Basic thread reconstruction (src/gaiwan/twitter_archive_processor/archive.py, lines 63-84)
- [ ] Enhanced Thread Features
  - [ ] Implement thread visualization
  - [ ] Add conversation statistics
  - [ ] Track thread participation metrics
  - [ ] Consider thread categorization

## User Identity & Graph Features
- [ ] User ID Implementation
  - [ ] Switch to user ID as primary key
  - [ ] Find user ID field in archive schemas
  - [ ] Update username-based lookups to use IDs
  - [ ] Create ID->username mappings
  - [ ] Handle missing user ID cases

- [ ] Identity Change Tracking
  - [ ] Build timeline of username changes
  - [ ] Track display name changes
  - [ ] Store avatar/profile pic history
  - [ ] Add API to query historical identities
  - [ ] Implement change detection logic

- [ ] Graph Analysis
  - [ ] Build user ID -> thread roots index
  - [ ] Build user ID -> mentions index
  - [ ] Implement bidirectional mappings
  - [ ] Add timeline views
  - [ ] Track deleted/suspended accounts
  - [ ] Add relationship strength metrics

## Schema & Validation
- [x] Basic archive schema handling (src/gaiwan/twitter_archive_processor/url_analysis/README.md, lines 84-86)
- [ ] Schema Management
  - [ ] Compare archive formats
  - [ ] Document format differences
  - [ ] Implement schema versioning
  - [ ] Add format auto-detection

## Performance & Infrastructure
- [x] Asynchronous content fetching (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 157-159)
- [x] Intelligent caching (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 71-74)
- [x] Rate limiting (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 54-69)
- [x] Concurrent processing (src/gaiwan/twitter_archive_processor/url_analysis/analyzer.py, lines 177-184)

## Testing & Documentation
- [x] Export format testing (src/gaiwan/twitter_archive_processor/url_analysis/README.md, lines 90-108)
- [x] Basic API documentation (src/gaiwan/twitter_archive_processor/url_analysis/README.md, lines 40-78)
- [x] CLI documentation (src/gaiwan/twitter_archive_processor/url_analysis/README.md, lines 28-38)
- [ ] Additional Testing
  - [ ] Add identity resolution tests
  - [ ] Create performance benchmarks
  - [ ] Add schema validation tests
  - [ ] Implement integration tests
  - [ ] Add stress testing

- [ ] Enhanced Documentation
  - [ ] Document archive formats
  - [ ] Add identity resolution guide
  - [ ] Document performance tradeoffs
  - [ ] Create troubleshooting guide
  - [ ] Add migration guides