# UserIdentity and related models
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class UserIdentity:
    """Represents a user's identity at a specific point in time"""
    user_id: str
    username: str
    display_name: str
    timestamp: datetime
    avatar_url: Optional[str] = None

@dataclass
class IdentityChange:
    """Represents a change in any aspect of a user's identity"""
    user_id: str
    timestamp: datetime
    username: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None