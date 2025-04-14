import logging
import shutil
from pathlib import Path
from typing import List, Dict
import json
from ..config import Config

logger = logging.getLogger(__name__)

class FolderSplitter:
    """Handles splitting large folders into smaller, manageable chunks."""
    
    def __init__(self, config: Config):
        self.config = config
        self.split_dir = config.split_dir
        self.split_dir.mkdir(exist_ok=True)
        self.split_mapping_file = config.output_dir / "split_mapping.json"
        self.split_mapping = self._load_mapping()
        
    def _load_mapping(self) -> Dict[str, List[str]]:
        """Load the mapping of original folders to split folders."""
        if not self.split_mapping_file.exists():
            return {}
            
        with open(self.split_mapping_file) as f:
            return json.load(f)
            
    def _save_mapping(self) -> None:
        """Save the mapping of original folders to split folders."""
        with open(self.split_mapping_file, 'w') as f:
            json.dump(self.split_mapping, f, indent=2)
            
    def split_folder(self, folder: str) -> List[str]:
        """Split a folder into smaller folders if it exceeds the size limit."""
        source_path = self.config.archives_dir / folder
        
        if not source_path.exists():
            logger.error(f"Source folder not found: {source_path}")
            return [folder]
            
        # Get total size of folder
        total_size = sum(f.stat().st_size for f in source_path.rglob('*') if f.is_file())
        
        # If folder is small enough, don't split
        if total_size <= self.config.max_folder_size:
            return [folder]
            
        # Calculate number of splits needed
        num_splits = (total_size // self.config.max_folder_size) + 1
        logger.info(f"Splitting {folder} into {num_splits} parts")
        
        # Get all files in folder
        all_files = list(source_path.rglob('*'))
        files_per_split = len(all_files) // num_splits
        
        split_folders = []
        for i in range(num_splits):
            split_name = f"{folder}_split_{i+1}"
            split_path = self.split_dir / split_name
            split_path.mkdir(exist_ok=True)
            
            # Get files for this split
            start_idx = i * files_per_split
            end_idx = start_idx + files_per_split if i < num_splits - 1 else len(all_files)
            split_files = all_files[start_idx:end_idx]
            
            # Copy files to split folder
            for file in split_files:
                if file.is_file():
                    rel_path = file.relative_to(source_path)
                    dest_path = split_path / rel_path
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(file, dest_path)
                    
            split_folders.append(split_name)
            
        # Save mapping
        self.split_mapping[folder] = split_folders
        self._save_mapping()
        
        return split_folders
        
    def cleanup_splits(self) -> None:
        """Clean up all split folders."""
        if self.split_dir.exists():
            shutil.rmtree(self.split_dir)
            self.split_dir.mkdir(exist_ok=True)
        
    def get_original_folder(self, split_folder: str) -> str:
        """Get the original folder name from a split folder name."""
        if '_split' not in split_folder:
            return split_folder
            
        base_folder = split_folder.split('_split')[0]
        if base_folder in self.split_mapping:
            return base_folder
        return split_folder
        
    def is_split_folder(self, folder: str) -> bool:
        """Check if a folder is a split folder."""
        return '_split' in folder
        
    def get_split_folders(self, folder: str) -> List[str]:
        """Get all split folders for a given folder."""
        return self.split_mapping.get(folder, [folder])