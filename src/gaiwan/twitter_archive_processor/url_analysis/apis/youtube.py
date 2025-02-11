from dataclasses import dataclass
from typing import Optional
import re
import aiohttp
from ..models import PageContent
from ...url_analysis.domain import DomainNormalizer
import logging

logger = logging.getLogger(__name__)

@dataclass
class YouTubeAPI:
    """YouTube API client."""
    api_key: str
    
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
    
    async def get_video_info(self, video_id: str) -> Optional[PageContent]:
        """Get video metadata using YouTube Data API."""
        url = f"https://www.googleapis.com/youtube/v3/videos"
        params = {
            'key': self.api_key,
            'id': video_id,
            'part': 'snippet,statistics'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return None
                    
                data = await response.json()
                if not data.get('items'):
                    return None
                    
                video = data['items'][0]
                snippet = video['snippet']
                
                return PageContent(
                    url=f"https://youtube.com/watch?v={video_id}",
                    title=snippet.get('title'),
                    description=snippet.get('description'),
                    content_type='video/youtube',
                    status_code=200
                )
    
    async def process_url(self, url: str) -> Optional[PageContent]:
        """Process a YouTube URL and return page content."""
        if video_id := self.extract_video_id(url):
            return await self.get_video_info(video_id)
        return None 