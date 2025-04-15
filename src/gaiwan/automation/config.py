from ..config import config

# This file is kept for backward compatibility and to maintain the module structure
# All configuration is now handled by the main config.py file

import json
from pathlib import Path
from typing import Dict, Any, Optional

class Config:
    """Configuration manager for automation settings."""
    
    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: int = 5,
        batch_size: int = 100,
        output_dir: str = "results",
        log_level: str = "INFO",
        max_processes: Optional[int] = None,
        memory_threshold: float = 0.8,
        blacklist_file: str = "blacklist.json",
        status_file: str = "status.json",
        archives_dir: str = "archives",
        max_folder_size: int = 1073741824,  # 1GB in bytes
        split_dir: str = "temp_splits",
        partition_dir: str = "partitions",
        store_html: bool = True,
        compress_html: bool = True,
        clean_html: bool = True
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        self.output_dir = Path(output_dir)
        self.log_level = log_level
        self.max_processes = max_processes
        self.memory_threshold = memory_threshold
        self.blacklist_file = Path(blacklist_file)
        self.status_file = Path(status_file)
        self.archives_dir = Path(archives_dir)
        self.max_folder_size = max_folder_size
        self.split_dir = Path(split_dir)
        self.partition_dir = Path(partition_dir)
        self.store_html = store_html
        self.compress_html = compress_html
        self.clean_html = clean_html
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.split_dir.mkdir(parents=True, exist_ok=True)
        self.partition_dir.mkdir(parents=True, exist_ok=True)
        
    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """Load configuration from file."""
        config_path = Path(config_path)
        if not config_path.exists():
            return cls()
            
        with open(config_path) as f:
            config_data = json.load(f)
            return cls(**config_data)
            
    def to_file(self, config_path: str) -> None:
        """Save configuration to file."""
        config_path = Path(config_path)
        config_data = {
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay,
            'batch_size': self.batch_size,
            'output_dir': str(self.output_dir),
            'log_level': self.log_level,
            'max_processes': self.max_processes,
            'memory_threshold': self.memory_threshold,
            'blacklist_file': str(self.blacklist_file),
            'status_file': str(self.status_file),
            'archives_dir': str(self.archives_dir),
            'max_folder_size': self.max_folder_size,
            'split_dir': str(self.split_dir),
            'partition_dir': str(self.partition_dir),
            'store_html': self.store_html,
            'compress_html': self.compress_html,
            'clean_html': self.clean_html
        }
        
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)
            
    def get_output_path(self, folder: str) -> Path:
        """Get output path for a specific folder."""
        return self.output_dir / f"{folder}_results.parquet"
        
    def load_blacklist(self) -> set:
        """Load blacklisted domains."""
        if not self.blacklist_file.exists():
            return set()
            
        with open(self.blacklist_file) as f:
            return set(json.load(f))
            
    def save_blacklist(self, blacklist: set) -> None:
        """Save blacklisted domains."""
        with open(self.blacklist_file, 'w') as f:
            json.dump(list(blacklist), f, indent=2)
            
    def load_status(self) -> Dict[str, Any]:
        """Load processing status."""
        if not self.status_file.exists():
            return {
                'completed': [],
                'failed': [],
                'in_progress': [],
                'last_update': None
            }
            
        with open(self.status_file) as f:
            return json.load(f)
            
    def save_status(self, status: Dict[str, Any]) -> None:
        """Save processing status."""
        with open(self.status_file, 'w') as f:
            json.dump(status, f, indent=2)