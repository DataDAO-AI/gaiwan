# IdentityChangeTracker implementation
from typing import List, Dict, Optional
from datetime import datetime
from .models import UserIdentity, IdentityChange
from collections import defaultdict

class IdentityChangeTracker:
    """Tracks changes in user identities over time"""
    
    def __init__(self):
        self._changes: Dict[str, List[IdentityChange]] = defaultdict(list)

    def record_identity_change(self, user_id: str, username: str = None,
                             display_name: str = None, avatar_url: str = None,
                             timestamp: datetime = None) -> None:
        """Record a change in any aspect of a user's identity"""
        if timestamp is None:
            timestamp = datetime.now()
            
        change = IdentityChange(
            user_id=user_id,
            timestamp=timestamp,
            username=username,
            display_name=display_name,
            avatar_url=avatar_url
        )
        
        self._changes[user_id].append(change)
        # Sort changes by timestamp to maintain chronological order
        self._changes[user_id].sort(key=lambda x: x.timestamp)

    def record_username_change(self, user_id: str, username: str,
                             timestamp: datetime = None) -> None:
        """Record a username change"""
        self.record_identity_change(user_id=user_id, username=username,
                                  timestamp=timestamp)

    def record_display_name_change(self, user_id: str, display_name: str,
                                 timestamp: datetime = None) -> None:
        """Record a display name change"""
        self.record_identity_change(user_id=user_id, display_name=display_name,
                                  timestamp=timestamp)

    def record_avatar_change(self, user_id: str, avatar_url: str,
                           timestamp: datetime = None) -> None:
        """Record an avatar change"""
        self.record_identity_change(user_id=user_id, avatar_url=avatar_url,
                                  timestamp=timestamp)

    def get_username_history(self, user_id: str) -> List[IdentityChange]:
        """Get the history of username changes"""
        return [change for change in self._changes[user_id]
                if change.username is not None]

    def get_display_name_history(self, user_id: str) -> List[IdentityChange]:
        """Get the history of display name changes"""
        return [change for change in self._changes[user_id]
                if change.display_name is not None]

    def get_avatar_history(self, user_id: str) -> List[IdentityChange]:
        """Get the history of avatar changes"""
        return [change for change in self._changes[user_id]
                if change.avatar_url is not None]

    def get_identity_at_date(self, user_id: str, timestamp: datetime) -> Optional[UserIdentity]:
        """Get the user's identity at a specific point in time"""
        if user_id not in self._changes:
            return None
            
        # Initialize with None values
        latest_username = None
        latest_display_name = None
        latest_avatar_url = None
        
        # Find the most recent changes before the specified timestamp
        for change in self._changes[user_id]:
            if change.timestamp > timestamp:
                break
                
            if change.username is not None:
                latest_username = change.username
            if change.display_name is not None:
                latest_display_name = change.display_name
            if change.avatar_url is not None:
                latest_avatar_url = change.avatar_url
        
        if latest_username is None:
            return None
            
        return UserIdentity(
            user_id=user_id,
            username=latest_username,
            display_name=latest_display_name or latest_username,
            timestamp=timestamp,
            avatar_url=latest_avatar_url
        )