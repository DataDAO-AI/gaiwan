import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Config:
    """Configuration settings for the URL analyzer and automation."""
    
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
        self.request_timeout = 10  # Seconds
        self.batch_size = 10  # Number of URLs to process in parallel
        
        # Connection pooling
        self.pool_connections = 100
        self.pool_maxsize = 100
        
        # HTML processing
        self.store_html = False  # Changed to False to avoid memory issues
        self.compress_html = False  # Changed to False to avoid memory issues
        self.clean_html = False  # Changed to False to avoid memory issues
        
        # Logging
        self.log_level = logging.INFO
        self.log_file = self.output_dir / "url_analyzer.log"
        
        # Process management
        self.max_concurrent_processes = 4  # Maximum number of parallel analyzers
        self.min_memory_per_process = 512 * 1024 * 1024  # 512MB minimum memory per process
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for serialization."""
        return {
            'max_folder_size': self.max_folder_size,
            'max_files_per_folder': self.max_files_per_folder,
            'max_requests_per_second': self.max_requests_per_second,
            'max_retries': self.max_retries,
            'request_timeout': self.request_timeout,
            'batch_size': self.batch_size,
            'pool_connections': self.pool_connections,
            'pool_maxsize': self.pool_maxsize,
            'store_html': self.store_html,
            'compress_html': self.compress_html,
            'clean_html': self.clean_html,
            'max_concurrent_processes': self.max_concurrent_processes,
            'min_memory_per_process': self.min_memory_per_process
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