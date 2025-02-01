from pathlib import Path
import logging
from typing import List
import pandas as pd
import json

from .archive import Archive
from .export.oai import OpenAIExporter

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
    
    def export_conversations_oai(self, output_path: Path, system_message: str) -> None:
        """Export conversations in OpenAI format."""
        exporter = OpenAIExporter()
        all_threads = []
        
        for archive in self.archives:
            threads = archive.get_conversation_threads()
            all_threads.extend(threads)
        
        # Sort threads by creation date
        all_threads.sort(key=lambda t: t.created_at)
        
        try:
            exporter.export_conversations(all_threads, output_path, system_message)
            logger.info(f"Exported conversations to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export conversations: {e}")
            raise