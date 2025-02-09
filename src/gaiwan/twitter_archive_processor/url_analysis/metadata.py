from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any

@dataclass
class URLMetadata:
    """Container for webpage metadata and fetch status."""
    url: str
    title: Optional[str] = None
    fetch_status: str = 'not_attempted'  # not_attempted, success, failed, skipped
    fetch_error: Optional[str] = None
    content_type: Optional[str] = None
    last_fetch_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=lambda: {
        'description': None,
        'keywords': None,
        'og_title': None,
        'og_description': None,
    })

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary for DataFrame storage."""
        return {
            'url': self.url,
            'title': self.title,
            'content_type': self.content_type,
            'fetch_status': self.fetch_status,
            'fetch_error': self.fetch_error,
            'last_fetch_time': self.last_fetch_time.isoformat() if self.last_fetch_time else None
        }

    def mark_skipped(self, reason: str) -> None:
        """Mark URL as skipped with a reason."""
        self.fetch_status = 'skipped'
        self.fetch_error = reason
        self.last_fetch_time = datetime.now(timezone.utc)

    def mark_failed(self, error: str) -> None:
        """Mark URL as failed with error details."""
        self.fetch_status = 'failed'
        self.fetch_error = error
        self.last_fetch_time = datetime.now(timezone.utc)

    def mark_success(self, content_type: str) -> None:
        """Mark URL as successfully processed."""
        self.fetch_status = 'success'
        self.content_type = content_type
        self.fetch_error = None
        self.last_fetch_time = datetime.now(timezone.utc)