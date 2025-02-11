from dataclasses import dataclass
from typing import Optional
import re
import aiohttp
from ..models import PageContent
from ...url_analysis.domain import DomainNormalizer
import logging

logger = logging.getLogger(__name__)

@dataclass
class TwitterAPI:
    """Twitter/X API client."""
    api_key: str
    api_secret: str
    bearer_token: str
    
    def extract_tweet_id(self, url: str) -> Optional[str]:
        """Extract tweet ID from Twitter/X URL."""
        patterns = [
            r'(?:twitter|x)\.com/\w+/status/(\d+)',
            r'(?:twitter|x)\.com/\w+/statuses/(\d+)'
        ]
        
        for pattern in patterns:
            if match := re.search(pattern, url):
                return match.group(1)
        return None
    
    async def get_tweet_info(self, tweet_id: str) -> Optional[PageContent]:
        """Get tweet metadata using Twitter API v2."""
        url = f"https://api.twitter.com/2/tweets/{tweet_id}"
        headers = {
            'Authorization': f'Bearer {self.bearer_token}',
            'Accept': 'application/json'
        }
        params = {
            'expansions': 'author_id',
            'tweet.fields': 'created_at,text,entities'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status != 200:
                    return None
                    
                data = await response.json()
                tweet = data.get('data', {})
                
                return PageContent(
                    url=f"https://twitter.com/i/web/status/{tweet_id}",
                    title=f"Tweet by @{tweet.get('author_id')}",
                    description=tweet.get('text'),
                    content_type='application/twitter',
                    status_code=200
                )
    
    async def process_url(self, url: str) -> Optional[PageContent]:
        """Process a Twitter/X URL and return page content."""
        if tweet_id := self.extract_tweet_id(url):
            return await self.get_tweet_info(tweet_id)
        return None 