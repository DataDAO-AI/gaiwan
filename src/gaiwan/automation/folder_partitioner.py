import logging
import shutil
from pathlib import Path
from typing import List, Dict
import json
from ..config import Config

logger = logging.getLogger(__name__)

class FolderPartitioner:
    """Handles partitioning JSON files into folders for parallel processing."""
    
    def __init__(self, config: Config):
        self.config = config
        self.partition_dir = config.partition_dir
        self.partition_dir.mkdir(parents=True, exist_ok=True)
        self.partition_mapping_file = config.output_dir / "partition_mapping.json"
        self.partition_mapping = self._load_mapping()
        
    def _load_mapping(self) -> Dict[str, List[str]]:
        """Load the mapping of original files to partition folders."""
        if not self.partition_mapping_file.exists():
            return {}
            
        with open(self.partition_mapping_file) as f:
            return json.load(f)
            
    def _save_mapping(self) -> None:
        """Save the mapping of original files to partition folders."""
        with open(self.partition_mapping_file, 'w') as f:
            json.dump(self.partition_mapping, f, indent=2)
            
    def partition_files(self, num_partitions: int) -> List[str]:
        """Partition JSON files into folders for parallel processing."""
        # Get all JSON files
        json_files = list(self.config.archives_dir.glob('*_archive.json'))
        if not json_files:
            logger.error(f"No JSON files found in {self.config.archives_dir}")
            return []
            
        # Calculate files per partition
        files_per_partition = len(json_files) // num_partitions
        if len(json_files) % num_partitions != 0:
            files_per_partition += 1
            
        logger.info(f"Partitioning {len(json_files)} files into {num_partitions} folders")
        
        partition_folders = []
        for i in range(num_partitions):
            partition_name = f"partition_{i+1}"
            partition_path = self.partition_dir / partition_name
            partition_path.mkdir(exist_ok=True)
            
            # Get files for this partition
            start_idx = i * files_per_partition
            end_idx = min(start_idx + files_per_partition, len(json_files))
            partition_files = json_files[start_idx:end_idx]
            
            # Copy files to partition folder
            for file in partition_files:
                dest_path = partition_path / file.name
                shutil.copy2(file, dest_path)
                
            partition_folders.append(partition_name)
            
        # Save mapping
        self.partition_mapping = {
            folder: [f.name for f in (self.partition_dir / folder).glob('*_archive.json')]
            for folder in partition_folders
        }
        self._save_mapping()
        
        return partition_folders
        
    def cleanup_partitions(self) -> None:
        """Clean up all partition folders."""
        if self.partition_dir.exists():
            shutil.rmtree(self.partition_dir)
            self.partition_dir.mkdir(exist_ok=True)
        
    def get_original_files(self, partition_folder: str) -> List[str]:
        """Get the original files in a partition folder."""
        return self.partition_mapping.get(partition_folder, [])
        
    def is_partition_folder(self, folder: str) -> bool:
        """Check if a folder is a partition folder."""
        return folder.startswith('partition_')
        
    def get_partition_folders(self) -> List[str]:
        """Get all partition folders."""
        return list(self.partition_mapping.keys()) 