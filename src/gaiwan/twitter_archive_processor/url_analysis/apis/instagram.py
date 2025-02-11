from dataclasses import dataclass
from typing import Optional
import re
import aiohttp
from ..models import PageContent
from ...url_analysis.domain import DomainNormalizer
import logging

logger = logging.getLogger(__name__)

@dataclass
class InstagramAPI:
    """Instagram API client."""
    access_token: str
    
    def extract_post_id(self, url: str) -> Optional[str]:
        """Extract post ID from Instagram URL."""
        patterns = [
            r'instagram\.com/p/([^/]+)',
            r'instagram\.com/reel/([^/]+)'
        ]
        
        for pattern in patterns:
            if match := re.search(pattern, url):
                return match.group(1)
        return None
    
    async def get_post_info(self, post_id: str) -> Optional[PageContent]:
        """Get post metadata using Instagram Graph API."""
        url = f"https://graph.instagram.com/{post_id}"
        params = {
            'access_token': self.access_token,
            'fields': 'caption,media_type,permalink,thumbnail_url'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return None
                    
                data = await response.json()
                return PageContent(
                    url=data.get('permalink', f"https://instagram.com/p/{post_id}"),
                    title=f"Instagram {data.get('media_type', 'Post')}",
                    description=data.get('caption'),
                    content_type='application/instagram',
                    status_code=200
                )
    
    async def process_url(self, url: str) -> Optional[PageContent]:
        """Process an Instagram URL and return page content."""
        if post_id := self.extract_post_id(url):
            return await self.get_post_info(post_id)
        return None 