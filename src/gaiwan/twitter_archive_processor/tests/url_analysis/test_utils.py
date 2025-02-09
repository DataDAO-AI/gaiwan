from unittest.mock import Mock
from contextlib import asynccontextmanager

class AsyncContextManagerMock:
    """Generic mock for async context managers."""
    def __init__(self, return_value):
        self.return_value = return_value

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, *args):
        pass

class AsyncMockResponse:
    """Mock specifically for aiohttp response."""
    def __init__(self, content, status=200, content_type="text/html"):
        self.content = content
        self.status = status
        self.headers = {"content-type": content_type}

    async def text(self):
        return self.content

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

def create_mock_response(content, status=200, content_type="text/html"):
    """Create a mock response that properly implements async context manager."""
    return AsyncMockResponse(content, status, content_type)

async def async_mock_coro(return_value):
    """Create an async coroutine that returns a value."""
    return return_value 