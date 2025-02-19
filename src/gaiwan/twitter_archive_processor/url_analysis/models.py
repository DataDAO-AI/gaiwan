from dataclasses import dataclass
from typing import Optional, Set, Dict
from datetime import datetime, timezone

@dataclass
class PageContent:
    """Container for scraped webpage content and metadata."""
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    text_content: Optional[str] = None
    links: Set[str] = None
    images: Set[str] = None
    fetch_time: datetime = None
    content_hash: Optional[str] = None
    content_type: Optional[str] = None
    status_code: Optional[int] = None
    error: Optional[str] = None

    def __post_init__(self):
        self.links = set() if self.links is None else self.links
        self.images = set() if self.images is None else self.images
        self.fetch_time = datetime.now(timezone.utc)

    def to_dict(self) -> Dict:
        return {
            'url': self.url,
            'title': self.title,
            'description': self.description,
            'text_content': self.text_content,
            'links': list(self.links),
            'images': list(self.images),
            'fetch_time': self.fetch_time.isoformat(),
            'content_hash': self.content_hash,
            'content_type': self.content_type,
            'status_code': self.status_code,
            'error': self.error
        } 