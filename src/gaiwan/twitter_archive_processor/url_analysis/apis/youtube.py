from typing import Optional
from datetime import datetime, timezone
import logging
from ..models import PageContent
from ...url_analysis.domain import DomainNormalizer
import re
import aiohttp

logger = logging.getLogger(__name__)

try:
    from googleapiclient.discovery import build
    YOUTUBE_API_AVAILABLE = True
except ImportError:
    YOUTUBE_API_AVAILABLE = False
    logger.warning("Google API client not installed. YouTube metadata extraction will be disabled.")

class YouTubeAPI:
    """Handles YouTube API interactions."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.youtube = None
        if api_key and YOUTUBE_API_AVAILABLE:
            self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def extract_video_id(self, url: str) -> Optional[str]:
        """Extract video ID from various YouTube URL formats."""
        patterns = [
            r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]+)',
            r'youtube\.com/shorts/([a-zA-Z0-9_-]+)',
        ]
        
        for pattern in patterns:
            if match := re.search(pattern, url):
                return match.group(1)
        return None
    
    async def process_url(self, url: str) -> Optional[PageContent]:
        """Process a YouTube URL to extract metadata."""
        if not YOUTUBE_API_AVAILABLE or not self.youtube:
            return None
            
        video_id = self.extract_video_id(url)
        if not video_id:
            return None
            
        try:
            response = self.youtube.videos().list(
                part='snippet',
                id=video_id
            ).execute()
            
            if not response['items']:
                return None
                
            video = response['items'][0]['snippet']
            return PageContent(
                url=url,
                title=video['title'],
                description=video['description'],
                content_type='video/youtube',
                status_code=200,
                fetch_time=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"YouTube API error for {url}: {e}")
            return None 