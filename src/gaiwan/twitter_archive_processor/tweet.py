from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict

@dataclass
class Tweet:
    """Represents a single tweet with its metadata."""
    id: str
    text: str
    created_at: Optional[datetime]
    media: List[Dict]
    parent_id: Optional[str]
    metadata: 'TweetMetadata'

    def clean_text(self):
        # Implement text cleaning logic here
        pass

    def get_media_files(self):
        # Logic to retrieve media files associated with the tweet
        pass