# Twitter Identity Tracking

A package for tracking and managing Twitter user identity changes over time, including usernames, display names, and avatars.

## Features

- User identity management:
  - User ID as primary key
  - Username to ID mapping
  - Temporary ID generation
  - Bidirectional lookups
- Identity change tracking:
  - Username history
  - Display name changes
  - Avatar/profile picture history
  - Point-in-time identity querying
- Integration with archive processing:
  - Automatic identity extraction
  - Cross-archive identity merging
  - Historical identity reconstruction

## Quick Start python
from gaiwan.twitter_archive_processor.identity import UserIdentityManager, IdentityChangeTracker
Initialize managers
identity_manager = UserIdentityManager()
identity_tracker = IdentityChangeTracker()
Add a user
user = identity_manager.add_user(username="original_name", user_id="123456")
Track identity changes
identity_tracker.record_username_change(
user_id="123456",
username="new_name",
timestamp=datetime.now()
)
Query historical identity
history = identity_tracker.get_username_history("123456")
identity_at_date = identity_tracker.get_identity_at_date(
user_id="123456",
timestamp=datetime(2023, 6, 1)
)
python
from gaiwan import ArchiveProcessor
from pathlib import Path
processor = ArchiveProcessor(archive_dir=Path("path/to/archives"))
processor.load_archives()
Get identity history as DataFrame
history_df = processor.get_user_identity_history(username="example_user")
Print identity changes
print(history_df.to_string())
Hierarchy
UserIdentityManager
└── UserIdentity
IdentityChangeTracker
└── IdentityChange
## Data Models

- `UserIdentity`: Current identity snapshot
- `IdentityChange`: Individual identity change record

## Best Practices

1. Always use user IDs as primary keys
2. Track all identity changes chronologically
3. Use temporary IDs for legacy data
4. Maintain bidirectional mappings
5. Record timestamps for all changese