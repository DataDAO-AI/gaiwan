"""
Twitter Archive Schema Documentation

This file documents the structure of Twitter archive files as discovered through 
inspection of multiple archives. It serves as a reference for future development
and maintenance of the thread_builder and related tools.
"""

SCHEMA_NOTES = """
Twitter Archive Structure
=========================

Top-Level Structure
------------------
Archives typically contain these top-level keys:
- 'upload-options': Archive creation options
- 'profile': User profile information
- 'account': Account-specific data
- 'tweets': Regular tweets
- 'community-tweet': Tweets posted in Twitter communities
- 'note-tweet': Longer-form note tweets (Twitter Notes feature)
- 'like': Tweets liked by the user
- 'follower': Follower information
- 'following': Following information
- '_metadata': Archive metadata

User Profile Structure
---------------------
The 'profile' section contains:
- 'userInformations': Basic user data
  - 'id': User ID
  - 'screenName': Twitter handle
  - 'displayName': Display name
  - 'description': Bio text
  - 'location': User-provided location
  - 'accountCreationDate': When account was created
- 'accountDisplayInformation':
  - 'followersCount': Number of followers
  - 'followingCount': Number of accounts followed
  - 'tweetCount': Total tweet count

Tweet Type Structures
--------------------

1. Regular Tweets ('tweets' array)
   Container key: 'tweet'
   Key fields:
   - 'id_str': String ID of the tweet (primary identifier)
   - 'id': Numeric ID (redundant with id_str)
   - 'full_text': The tweet content
   - 'created_at': Creation timestamp in Twitter format (e.g., "Wed Oct 10 20:19:24 +0000 2018")
   - 'in_reply_to_status_id_str': ID of tweet being replied to
   - 'in_reply_to_user_id_str': ID of user being replied to
   - 'in_reply_to_screen_name': Screen name of user being replied to
   - 'favorite_count': Number of likes
   - 'retweet_count': Number of retweets
   - 'entities': Contains hashtags, mentions, URLs, etc.
   - 'extended_entities': May contain media attachments
   - 'possibly_sensitive': Flag for potentially sensitive content
   - 'source': Client used to post the tweet
   - 'lang': Language code

2. Community Tweets ('community-tweet' array)
   Container key: 'tweet'
   Contains all regular tweet fields plus:
   - 'community_id': Numeric community ID
   - 'community_id_str': String community ID
   - 'scopes': Community-specific scopes
   Timestamp format: Same as regular tweets

3. Note Tweets ('note-tweet' array)
   Container key: 'noteTweet'  (DIFFERENT FROM OTHER TYPES)
   Structure may differ from regular tweets, often containing:
   - 'noteText': The note content
   - 'createdAt': Timestamp in ISO format (e.g., "2022-08-19T22:22:42.000Z")
                  NOT the same format as regular tweets
   - Along with standard tweet metadata

4. Likes ('like' array)
   Container key: 'like'
   Key fields:
   - 'tweetId': ID of the liked tweet
   - 'fullText': Content of the liked tweet
   - 'expandedUrl': URL to the tweet
   Note: No timestamp information is available in likes

Timestamp Formats
----------------
Twitter uses different timestamp formats:

1. Regular/Community tweets: "Wed Oct 10 20:19:24 +0000 2018"
   Format: "%a %b %d %H:%M:%S %z %Y"

2. Note tweets: "2022-08-19T22:22:42.000Z"
   ISO 8601 format: "%Y-%m-%dT%H:%M:%S.%fZ"

These different formats require different parsing approaches.

Entity Structure
---------------
Entities are nested within tweets and contain:
- 'urls': Array of URL objects
  - 'expanded_url': The expanded URL
  - 'url': The t.co shortened URL
- 'hashtags': Array of hashtag objects
  - 'text': The hashtag text without #
- 'user_mentions': Array of mention objects
  - 'screen_name': The mentioned user's handle
  - 'id_str': The mentioned user's ID
- 'media': Array of media objects (in extended_entities)
  - 'media_url': URL to the media
  - 'type': Media type (photo, video, etc.)
"""

IMPLEMENTATION_NOTES = """
Critical Implementation Considerations
======================================

Timestamp Handling
-----------------
1. Regular/Community Tweet format: "%a %b %d %H:%M:%S %z %Y"
   Example: "Wed Oct 10 20:19:24 +0000 2018"
   
2. Note Tweet format: ISO 8601 format
   Example: "2022-08-19T22:22:42.000Z"
   
3. Implementation must detect and handle both formats:
   ```python
   def parse_twitter_timestamp(timestamp_str):
       if not timestamp_str:
           return None
           
       if 'T' in timestamp_str and timestamp_str.endswith('Z'):
           # ISO format for note tweets
           try:
               return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
           except:
               return None
       else:
           # Regular Twitter format
           try:
               return datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %z %Y")
           except:
               return None
   ```

DuckDB Compatibility
-------------------
1. IMPORTANT: DuckDB in some environments doesn't support partial indexes
   - Instead of: CREATE INDEX ... WHERE condition
   - Use: Create full indexes and filter in queries

2. SQL Reserved Words Conflicts
   - Avoid using 'to' as an alias (use 'tr', 'target', etc.)
   - Other commonly problematic aliases: 'from', 'select', 'order', 'group'

Processing Approaches
--------------------
1. Bidirectional Thread Reconstruction
   - Bottom-up: From replies to root tweets
   - Top-down: From root tweets to all replies
   - Merge both approaches to handle missing tweets

2. Memory Optimization
   - Process in batches (500,000 tweets recommended)
   - Use appropriate DuckDB memory limits based on system

3. Type Handling
   - Always convert ID fields to strings for consistency
   - Parse dates with error handling (some tweets may have invalid dates)

4. Community and Note Tweet Processing
   - Different container key for Note Tweets ('noteTweet' vs 'tweet')
   - Note: When processing note tweets, look for the 'noteTweet' container key
   - Community tweets contain community_id fields that regular tweets don't have
   - Note tweets use a different timestamp format (ISO 8601)

5. User Integration
   - Cache user information from profile sections
   - Match liked tweets to their authors where possible
"""

def get_schema_reference():
    """Return the schema notes as a structured reference."""
    return SCHEMA_NOTES

def get_implementation_notes():
    """Return implementation notes and best practices."""
    return IMPLEMENTATION_NOTES

if __name__ == "__main__":
    print("Twitter Archive Schema Reference")
    print("===============================")
    print(get_schema_reference())
    print("\nImplementation Notes")
    print("===================")
    print(get_implementation_notes()) 