import subprocess
import time
import logging
from pathlib import Path
from typing import Dict, List, Set
from ..config import Config
from .resource_monitor import ResourceMonitor
from .folder_splitter import FolderSplitter

logger = logging.getLogger(__name__)

class ProcessManager:
    """Manages URL analyzer processes and handles automation."""
    
    def __init__(self, config: Config):
        self.config = config
        self.resource_monitor = ResourceMonitor(config)
        self.folder_splitter = FolderSplitter(config)
        self.active_processes: Dict[str, subprocess.Popen] = {}
        self.completed_folders: Set[str] = set()
        self.failed_folders: Set[str] = set()
        self.retry_counts: Dict[str, int] = {}
        
    def start_analysis(self, folder: str) -> bool:
        """Start analysis for a folder if resources allow."""
        if not self.resource_monitor.can_start_new_process():
            return False
            
        # Check if folder needs to be split
        split_folders = self.folder_splitter.split_folder(folder)
        if len(split_folders) > 1:
            logger.info(f"Split {folder} into {len(split_folders)} smaller folders")
            # Start analysis for each split folder
            for split_folder in split_folders:
                if not self._start_single_analysis(split_folder):
                    return False
            return True
        else:
            return self._start_single_analysis(folder)
            
    def _start_single_analysis(self, folder: str) -> bool:
        """Start analysis for a single folder."""
        output_path = self.config.output_dir / f"{folder}_results.parquet"
        archive_path = self.config.archives_dir / folder
        
        if not archive_path.exists():
            logger.error(f"Archive directory not found: {archive_path}")
            return False
            
        try:
            process = subprocess.Popen(
                [
                    "python", "-m", "gaiwan.url_analyzer",
                    str(archive_path),
                    "--output", str(output_path),
                    "--debug",
                    "--store-html" if self.config.store_html else "--no-html",
                    "--compress-html" if self.config.compress_html else "--no-compress",
                    "--clean-html" if self.config.clean_html else "--no-clean"
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.active_processes[folder] = process
            self.retry_counts[folder] = self.retry_counts.get(folder, 0) + 1
            
            logger.info(f"Started analysis for {folder}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start analysis for {folder}: {e}")
            return False
            
    def monitor_processes(self) -> None:
        """Monitor running processes and handle completion/failures."""
        for folder, process in list(self.active_processes.items()):
            if process.poll() is not None:  # Process finished
                stdout, stderr = process.communicate()
                
                if process.returncode == 0:
                    self.completed_folders.add(folder)
                    logger.info(f"Completed analysis for {folder}")
                else:
                    self.failed_folders.add(folder)
                    logger.error(f"Analysis failed for {folder}: {stderr}")
                    
                del self.active_processes[folder]
                
    def handle_failures(self) -> None:
        """Handle failed folders with retry logic."""
        for folder in list(self.failed_folders):
            if self._should_retry(folder):
                if self.start_analysis(folder):
                    self.failed_folders.remove(folder)
                    
    def _should_retry(self, folder: str) -> bool:
        """Determine if a folder should be retried."""
        return (
            self.retry_counts.get(folder, 0) < self.config.max_retries and
            not self.resource_monitor.is_system_overloaded()
        )
        
    def get_status(self) -> dict:
        """Get current processing status."""
        return {
            'active': list(self.active_processes.keys()),
            'completed': list(self.completed_folders),
            'failed': list(self.failed_folders),
            'resources': self.resource_monitor.get_resource_usage()
        }
        
    def cleanup(self) -> None:
        """Clean up any remaining processes."""
        for process in self.active_processes.values():
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
                
        self.active_processes.clear()