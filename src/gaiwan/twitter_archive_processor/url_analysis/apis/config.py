import json
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for API keys and settings."""
    
    DEFAULT_CONFIG_PATH = Path.home() / ".config" / "twitter_archive_processor" / "config.json"
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = Path(config_path or self.DEFAULT_CONFIG_PATH)
        self.api_keys = {}
        
        if self.config_path.exists():
            self.load_config()
    
    def load_config(self) -> None:
        """Load configuration from file."""
        try:
            with open(self.config_path) as f:
                config = json.load(f)
                self.api_keys = config.get('api_keys', {})
        except Exception as e:
            logger.warning(f"Failed to load config from {self.config_path}: {e}")
            self.api_keys = {}
    
    def save_config(self) -> None:
        """Save configuration to file."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w') as f:
            json.dump({'api_keys': self.api_keys}, f, indent=2)
    
    def get_api_key(self, service: str) -> Optional[str]:
        """Get API key for a specific service."""
        return self.api_keys.get(service) 