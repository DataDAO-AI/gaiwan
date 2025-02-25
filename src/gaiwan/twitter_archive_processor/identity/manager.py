# UserIdentityManager implementation
from typing import Optional, Dict
from .models import UserIdentity
from datetime import datetime
import uuid

class UserIdentityManager:
    """Manages user identities and their mappings"""
    
    def __init__(self):
        self._users: Dict[str, UserIdentity] = {}
        self._username_to_id: Dict[str, str] = {}
        self._temp_id_prefix = "TEMP_"

    def add_user(self, username: str, user_id: Optional[str] = None) -> UserIdentity:
        """
        Add a new user to the manager.
        If no user_id is provided, generates a temporary one.
        """
        if user_id is None:
            user_id = f"{self._temp_id_prefix}{uuid.uuid4()}"
        
        user = UserIdentity(
            user_id=user_id,
            username=username,
            display_name=username,  # Default to username if no display name provided
            timestamp=datetime.now()
        )
        
        self._users[user_id] = user
        self._username_to_id[username] = user_id
        
        return user

    def get_user(self, user_id: str) -> Optional[UserIdentity]:
        """Retrieve a user by their ID"""
        return self._users.get(user_id)

    def get_user_id_from_username(self, username: str) -> Optional[str]:
        """Get user ID from username"""
        return self._username_to_id.get(username)

    def get_username_from_id(self, user_id: str) -> Optional[str]:
        """Get username from user ID"""
        user = self.get_user(user_id)
        return user.username if user else None

    def is_temporary_id(self, user_id: str) -> bool:
        """Check if a user ID is temporary"""
        return user_id.startswith(self._temp_id_prefix)

    def update_username(self, user_id: str, new_username: str) -> None:
        """Update a user's username"""
        if user_id in self._users:
            old_username = self._users[user_id].username
            self._users[user_id].username = new_username
            self._users[user_id].timestamp = datetime.now()
            
            # Update mappings
            del self._username_to_id[old_username]
            self._username_to_id[new_username] = user_id