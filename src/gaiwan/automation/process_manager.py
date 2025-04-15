import subprocess
import time
import logging
from pathlib import Path
from typing import Dict, List, Set
from ..config import Config
from .resource_monitor import ResourceMonitor
from .folder_partitioner import FolderPartitioner

logger = logging.getLogger(__name__)

class ProcessManager:
    """Manages URL analyzer processes and handles automation."""
    
    def __init__(self, config: Config):
        self.config = config
        self.resource_monitor = ResourceMonitor(config)
        self.folder_partitioner = FolderPartitioner(config)
        self.active_processes: Dict[str, subprocess.Popen] = {}
        self.completed_partitions: Set[str] = set()
        self.failed_partitions: Set[str] = set()
        self.retry_counts: Dict[str, int] = {}
        
    def start_analysis(self, num_partitions: int) -> bool:
        """Start analysis for all partitions if resources allow."""
        # First, partition the files
        partition_folders = self.folder_partitioner.partition_files(num_partitions)
        if not partition_folders:
            return False
            
        # Start analysis for each partition
        for partition in partition_folders:
            if not self._start_single_analysis(partition):
                return False
        return True
            
    def _start_single_analysis(self, partition: str) -> bool:
        """Start analysis for a single partition folder."""
        output_path = self.config.output_dir / f"{partition}_results.parquet"
        partition_path = self.config.partition_dir / partition
        content_cache_dir = partition_path / '.content_cache'
        
        if not partition_path.exists():
            logger.error(f"Partition directory not found: {partition_path}")
            return False
            
        try:
            logger.info(f"Starting URL analyzer for partition {partition}")
            logger.info(f"Output will be written to: {output_path}")
            
            process = subprocess.Popen(
                [
                    "python", "-m", "gaiwan.url_analyzer",
                    str(partition_path),
                    "--output_file", str(output_path),
                    "--content_cache_dir", str(content_cache_dir),
                    "--debug"
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.active_processes[partition] = process
            self.retry_counts[partition] = self.retry_counts.get(partition, 0) + 1
            
            logger.info(f"Started analysis for partition {partition}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start analysis for partition {partition}: {e}")
            return False
            
    def monitor_processes(self) -> None:
        """Monitor running processes and handle completion/failures."""
        for partition, process in list(self.active_processes.items()):
            if process.poll() is not None:  # Process finished
                stdout, stderr = process.communicate()
                
                if process.returncode == 0:
                    self.completed_partitions.add(partition)
                    logger.info(f"Completed analysis for partition {partition}")
                else:
                    self.failed_partitions.add(partition)
                    logger.error(f"Analysis failed for partition {partition}: {stderr}")
                    
                del self.active_processes[partition]
                
    def handle_failures(self) -> None:
        """Handle failed partitions with retry logic."""
        for partition in list(self.failed_partitions):
            if self._should_retry(partition):
                if self._start_single_analysis(partition):
                    self.failed_partitions.remove(partition)
                    
    def _should_retry(self, partition: str) -> bool:
        """Determine if a partition should be retried."""
        return (
            self.retry_counts.get(partition, 0) < self.config.max_retries and
            not self.resource_monitor.is_system_overloaded()
        )
        
    def get_status(self) -> dict:
        """Get current processing status."""
        return {
            'active': list(self.active_processes.keys()),
            'completed': list(self.completed_partitions),
            'failed': list(self.failed_partitions),
            'resources': self.resource_monitor.get_resource_usage()
        }
        
    def cleanup(self) -> None:
        """Clean up any remaining processes and partitions."""
        for process in self.active_processes.values():
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
                
        self.active_processes.clear()
        self.folder_partitioner.cleanup_partitions()