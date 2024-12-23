# Gaiwan TODO

## Quote Tweet Investigation
1. Run schema generator on personal Twitter archive (`tweets.js`) to verify field structure
2. Analyze URL entities to detect quote tweets:
   - Look for `twitter.com/*/status/*` patterns in expanded URLs
   - Extract quoted tweet IDs from URLs
   - Consider edge cases (deleted tweets, private accounts)
3. Update CanonicalTweet to handle quote tweets via URLs:
   - Add URL-based quote detection
   - Remove old quote tweet field assumptions
   - Add tests with sample quote tweet URLs

## Schema Improvements
1. Compare archive schema between:
   - Community archive format
   - Personal Twitter archive format 
   - Document differences
2. Update schema handling to work with both formats
3. Consider schema versioning for different archive types

## General Improvements
1. Add more validation and error handling
2. Improve logging and debugging output
3. Add documentation about archive formats
4. Consider adding archive format detection/validation 

## User Identity & Graph Features
1. Switch to user ID as primary key:
   - Find user ID field in archive schemas
   - Update all username-based lookups to use IDs
   - Create ID->username mappings
   - Handle cases where user ID is missing

2. Track identity changes over time:
   - Build timeline of username changes
   - Track display name changes
   - Consider storing avatar/profile pic history
   - Add API to query historical identities

3. Enhanced Graph Features:
   - Build user ID -> thread roots index
   - Build user ID -> mentions index
   - Consider bidirectional mappings (who mentioned this user?)
   - Add timeline views (all interactions with user X)
   - Consider tracking deleted/suspended accounts

4. Identity Resolution:
   - Handle username recycling edge cases
   - Build confidence scoring for identity matches
   - Consider fuzzy matching for display names
   - Document identity resolution strategy

5. Performance Considerations:
   - Evaluate index size impacts
   - Consider caching strategies
   - Benchmark large graph operations
   - Document memory/storage tradeoffs