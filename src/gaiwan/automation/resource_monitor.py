import psutil
import multiprocessing
from typing import Optional
from .config import Config

class ResourceMonitor:
    """Monitors system resources and determines optimal processing parameters."""
    
    def __init__(self, config: Config):
        self.config = config
        self.max_processes = (
            config.max_processes or 
            max(1, multiprocessing.cpu_count() - 1)
        )
        
    def can_start_new_process(self) -> bool:
        """Check if system can handle another process."""
        return (
            len(self.get_active_processes()) < self.max_processes and
            psutil.virtual_memory().percent < self.config.memory_threshold * 100
        )
        
    def get_optimal_batch_size(self) -> int:
        """Calculate optimal batch size based on available memory."""
        memory_available = psutil.virtual_memory().available
        # Estimate memory needed per URL (100KB as a conservative estimate)
        memory_per_url = 1024 * 100
        max_urls = memory_available // memory_per_url
        
        return min(
            self.config.batch_size,
            max(10, max_urls // self.max_processes)
        )
        
    def get_active_processes(self) -> list:
        """Get list of active URL analyzer processes."""
        active = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info['cmdline']
                if cmdline and 'url_analyzer' in ' '.join(cmdline):
                    active.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return active
        
    def get_resource_usage(self) -> dict:
        """Get current resource usage statistics."""
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'active_processes': len(self.get_active_processes()),
            'max_processes': self.max_processes,
            'batch_size': self.get_optimal_batch_size()
        }
        
    def is_system_overloaded(self) -> bool:
        """Check if system is overloaded and needs to reduce load."""
        return (
            psutil.cpu_percent() > 90 or
            psutil.virtual_memory().percent > self.config.memory_threshold * 100
        )
        
    def should_reduce_load(self) -> bool:
        """Determine if we should reduce the number of active processes."""
        return (
            self.is_system_overloaded() or
            len(self.get_active_processes()) >= self.max_processes
        )