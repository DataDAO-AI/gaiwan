from pathlib import Path
import logging
from typing import List
import pandas as pd

from .archive import Archive

logger = logging.getLogger(__name__)

class ArchiveProcessor:
    """Processes multiple Twitter archives."""
    
    def __init__(self, archive_dir: Path):
        self.archive_dir = archive_dir
        self.archives: List[Archive] = []
        
    def load_archives(self) -> None:
        """Load all archives from the directory."""
        for archive_file in self.archive_dir.glob("*_archive.json"):
            try:
                archive = Archive(archive_file)
                archive.load()
                self.archives.append(archive)
            except Exception as e:
                logger.error(f"Failed to load {archive_file}: {e}")
    
    def analyze_urls(self) -> pd.DataFrame:
        """Analyze URLs across all archives."""
        dfs = []
        for archive in self.archives:
            df = archive.analyze_urls()
            if not df.empty:
                dfs.append(df)
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()
    
    def export_all(self, format: str, output_dir: Path) -> None:
        """Export all archives in specified format."""
        for archive in self.archives:
            try:
                output_path = output_dir / f"{archive.username}_{format}"
                archive.export(format, output_path)
            except Exception as e:
                logger.error(f"Failed to export {archive.username}: {e}")