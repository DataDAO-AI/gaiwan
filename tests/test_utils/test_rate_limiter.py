"""Tests for rate limiting functionality."""
import pytest
import asyncio
from gaiwan.twitter_archive_processor.utils import RateLimiter

@pytest.mark.asyncio
async def test_rate_limiting():
    """Test rate limiter behavior."""
    limiter = RateLimiter(requests_per_second=2)
    
    start = asyncio.get_event_loop().time()
    
    async with limiter:
        await asyncio.sleep(0)  # First request
    async with limiter:
        await asyncio.sleep(0)  # Second request
    async with limiter:
        await asyncio.sleep(0)  # Third request should wait
        
    duration = asyncio.get_event_loop().time() - start
    assert duration >= 0.5  # Should take at least 500ms