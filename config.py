import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Set

class Config:
    """Unified configuration manager for the Twitter Archive Processor.
    
    This class consolidates all configuration settings from various modules into a single,
    well-organized configuration file. It handles settings for:
    - Directory structure
    - URL analysis
    - API configuration
    - Automation settings
    - Process management
    - Logging
    """
    
    def __init__(self):
        # Base directories
        self.base_dir = Path("src")
        self.archives_dir = self.base_dir / "archives"
        self.output_dir = self.base_dir / "output"
        self.split_dir = self.archives_dir / "split"
        
        # Create necessary directories
        for dir_path in [self.output_dir, self.split_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        # Folder splitting settings
        self.max_folder_size = 1024 * 1024 * 1024  # 1GB in bytes
        self.max_files_per_folder = 100  # Maximum number of archive files per folder
        
        # URL analyzer settings
        self.max_requests_per_second = 5  # Rate limiting
        self.max_retries = 3  # Number of retries for failed requests
        self.retry_delay = 5  # Seconds between retries
        self.request_timeout = 10  # Seconds
        self.batch_size = 10  # Number of URLs to process in parallel
        
        # Connection pooling
        self.pool_connections = 100
        self.pool_maxsize = 100
        
        # HTML processing
        self.store_html = True
        self.compress_html = True
        self.clean_html = True
        
        # API Configuration
        self.api_config_path = Path.home() / ".config" / "twitter_archive_processor" / "config.json"
        self.api_keys: Dict[str, str] = {}
        
        # Automation settings
        self.memory_threshold = 0.8  # Memory usage threshold for process management
        self.blacklist_file = self.output_dir / "blacklist.json"
        self.status_file = self.output_dir / "status.json"
        
        # Process management
        self.max_concurrent_processes = 4  # Maximum number of parallel analyzers
        self.min_memory_per_process = 512 * 1024 * 1024  # 512MB minimum memory per process
        
        # Logging
        self.log_level = logging.INFO
        self.log_file = self.output_dir / "url_analyzer.log"
        
        # Load API keys if config exists
        if self.api_config_path.exists():
            self.load_api_config()
    
    def load_api_config(self) -> None:
        """Load API configuration from file."""
        try:
            with open(self.api_config_path) as f:
                config = json.load(f)
                self.api_keys = config.get('api_keys', {})
        except Exception as e:
            logging.warning(f"Failed to load API config from {self.api_config_path}: {e}")
            self.api_keys = {}
    
    def save_api_config(self) -> None:
        """Save API configuration to file."""
        self.api_config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.api_config_path, 'w') as f:
            json.dump({'api_keys': self.api_keys}, f, indent=2)
    
    def get_api_key(self, service: str) -> Optional[str]:
        """Get API key for a specific service."""
        return self.api_keys.get(service)
    
    def load_blacklist(self) -> Set[str]:
        """Load blacklisted domains."""
        if not self.blacklist_file.exists():
            return set()
            
        try:
            with open(self.blacklist_file) as f:
                return set(json.load(f))
        except Exception as e:
            logging.warning(f"Failed to load blacklist: {e}")
            return set()
    
    def save_blacklist(self, blacklist: Set[str]) -> None:
        """Save blacklisted domains."""
        try:
            with open(self.blacklist_file, 'w') as f:
                json.dump(list(blacklist), f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save blacklist: {e}")
    
    def load_status(self) -> Dict[str, Any]:
        """Load processing status."""
        if not self.status_file.exists():
            return {
                'completed': [],
                'failed': [],
                'in_progress': [],
                'last_update': None
            }
            
        try:
            with open(self.status_file) as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load status: {e}")
            return {
                'completed': [],
                'failed': [],
                'in_progress': [],
                'last_update': None
            }
    
    def save_status(self, status: Dict[str, Any]) -> None:
        """Save processing status."""
        try:
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save status: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for serialization."""
        return {
            'max_folder_size': self.max_folder_size,
            'max_files_per_folder': self.max_files_per_folder,
            'max_requests_per_second': self.max_requests_per_second,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay,
            'request_timeout': self.request_timeout,
            'batch_size': self.batch_size,
            'pool_connections': self.pool_connections,
            'pool_maxsize': self.pool_maxsize,
            'store_html': self.store_html,
            'compress_html': self.compress_html,
            'clean_html': self.clean_html,
            'max_concurrent_processes': self.max_concurrent_processes,
            'min_memory_per_process': self.min_memory_per_process,
            'memory_threshold': self.memory_threshold,
            'log_level': self.log_level
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'Config':
        """Create Config instance from dictionary."""
        config = cls()
        for key, value in config_dict.items():
            if hasattr(config, key):
                setattr(config, key, value)
        return config

# Create a global config instance
config = Config() 