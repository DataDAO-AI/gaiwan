from dataclasses import dataclass
from typing import Optional
import re
import aiohttp
from ..models import PageContent

@dataclass
class GitHubAPI:
    """GitHub API client."""
    api_key: str
    
    def extract_repo_info(self, url: str) -> Optional[tuple[str, str]]:
        """Extract owner and repo from GitHub URL."""
        pattern = r'github\.com/([^/]+)/([^/]+)'
        if match := re.search(pattern, url):
            return match.group(1), match.group(2)
        return None
    
    async def get_repo_info(self, owner: str, repo: str) -> Optional[PageContent]:
        """Get repository metadata using GitHub API."""
        url = f"https://api.github.com/repos/{owner}/{repo}"
        headers = {
            'Authorization': f'token {self.api_key}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    return None
                    
                data = await response.json()
                return PageContent(
                    url=data['html_url'],
                    title=data['full_name'],
                    description=data['description'],
                    content_type='application/github',
                    status_code=200
                )
    
    async def process_url(self, url: str) -> Optional[PageContent]:
        """Process a GitHub URL and return page content."""
        if repo_info := self.extract_repo_info(url):
            return await self.get_repo_info(*repo_info)
        return None 