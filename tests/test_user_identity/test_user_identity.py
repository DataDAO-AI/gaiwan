import pytest
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass
from gaiwan.twitter_archive_processor.identity import (
    UserIdentity,
    IdentityChangeTracker,
    UserIdentityManager
)

@dataclass
class UserIdentity:
    user_id: str
    username: str
    display_name: str
    timestamp: datetime
    avatar_url: str = None

class TestUserIdentityImplementation:
    def test_user_id_as_primary_key(self):
        """Test that user_id is properly used as the primary key for user lookups"""
        user_manager = UserIdentityManager()
        user_id = "123456"
        username = "original_user"
        
        user_manager.add_user(user_id=user_id, username=username)
        retrieved_user = user_manager.get_user(user_id)
        
        assert retrieved_user.user_id == user_id
        assert retrieved_user.username == username

    def test_username_to_id_mapping(self):
        """Test the bidirectional mapping between usernames and user IDs"""
        user_manager = UserIdentityManager()
        user_id = "123456"
        username = "test_user"
        
        user_manager.add_user(user_id=user_id, username=username)
        
        assert user_manager.get_user_id_from_username(username) == user_id
        assert user_manager.get_username_from_id(user_id) == username

    def test_handle_missing_user_id(self):
        """Test graceful handling of cases where user ID is not available"""
        user_manager = UserIdentityManager()
        username = "legacy_user"
        
        # Should generate a temporary ID when user_id is not available
        temp_user = user_manager.add_user(username=username, user_id=None)
        assert temp_user.user_id.startswith("TEMP_")
        assert user_manager.is_temporary_id(temp_user.user_id)

class TestIdentityChangeTracking:
    def test_username_change_tracking(self):
        """Test tracking of username changes over time"""
        tracker = IdentityChangeTracker()
        user_id = "123456"
        
        changes = [
            ("original_name", "2023-01-01T00:00:00Z"),
            ("new_name", "2023-06-01T00:00:00Z"),
            ("final_name", "2024-01-01T00:00:00Z")
        ]
        
        for username, timestamp in changes:
            tracker.record_username_change(
                user_id=user_id,
                username=username,
                timestamp=datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            )
        
        history = tracker.get_username_history(user_id)
        assert len(history) == 3
        assert history[0].username == "original_name"
        assert history[-1].username == "final_name"

    def test_display_name_tracking(self):
        """Test tracking of display name changes"""
        tracker = IdentityChangeTracker()
        user_id = "123456"
        
        tracker.record_display_name_change(
            user_id=user_id,
            display_name="Original Display",
            timestamp=datetime.now()
        )
        
        tracker.record_display_name_change(
            user_id=user_id,
            display_name="New Display Name",
            timestamp=datetime.now()
        )
        
        history = tracker.get_display_name_history(user_id)
        assert len(history) == 2
        assert history[-1].display_name == "New Display Name"

    def test_avatar_history_tracking(self):
        """Test tracking of avatar/profile picture changes"""
        tracker = IdentityChangeTracker()
        user_id = "123456"
        
        avatars = [
            ("http://example.com/avatar1.jpg", "2023-01-01T00:00:00Z"),
            ("http://example.com/avatar2.jpg", "2023-06-01T00:00:00Z")
        ]
        
        for url, timestamp in avatars:
            tracker.record_avatar_change(
                user_id=user_id,
                avatar_url=url,
                timestamp=datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            )
        
        history = tracker.get_avatar_history(user_id)
        assert len(history) == 2
        assert history[-1].avatar_url == "http://example.com/avatar2.jpg"

    def test_identity_query_by_date(self):
        """Test querying historical identity information at a specific point in time"""
        tracker = IdentityChangeTracker()
        user_id = "123456"
        
        # Add various identity changes
        changes = [
            {
                "username": "user_v1",
                "display_name": "User Version 1",
                "avatar_url": "http://example.com/av1.jpg",
                "timestamp": "2023-01-01T00:00:00Z"
            },
            {
                "username": "user_v2",
                "display_name": "User Version 2",
                "avatar_url": "http://example.com/av2.jpg",
                "timestamp": "2023-06-01T00:00:00Z"
            }
        ]
        
        for change in changes:
            tracker.record_identity_change(
                user_id=user_id,
                **{k: v if k != 'timestamp' else datetime.fromisoformat(v.replace('Z', '+00:00'))
                   for k, v in change.items()}
            )
        
        # Query identity at specific date
        identity = tracker.get_identity_at_date(
            user_id=user_id,
            timestamp=datetime.fromisoformat("2023-03-01T00:00:00+00:00")
        )
        
        assert identity.username == "user_v1"
        assert identity.display_name == "User Version 1"
        assert identity.avatar_url == "http://example.com/av1.jpg" 