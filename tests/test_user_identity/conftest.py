import pytest
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

@pytest.fixture
def user_manager():
    """Fixture providing a clean UserIdentityManager instance"""
    return UserIdentityManager()

@pytest.fixture
def identity_tracker():
    """Fixture providing a clean IdentityChangeTracker instance"""
    return IdentityChangeTracker()

@pytest.fixture
def sample_user_data():
    """Fixture providing sample user data for testing"""
    return {
        "user_id": "123456",
        "username": "test_user",
        "display_name": "Test User",
        "avatar_url": "http://example.com/avatar.jpg",
        "timestamp": datetime.now()
    } 