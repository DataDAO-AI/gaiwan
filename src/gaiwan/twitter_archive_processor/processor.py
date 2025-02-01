from pathlib import Path
import logging
from typing import List, Dict, Type
import pandas as pd
import json

from .export.base import Exporter
from .export.oai import OpenAIExporter
from .export.chatml import ChatMLExporter
from .export.markdown import MarkdownExporter
from .archive import Archive
from .url_analyzer import URLAnalyzer

logger = logging.getLogger(__name__)

class ArchiveProcessor:
    """Processes multiple Twitter archives."""
    
    EXPORTERS: Dict[str, Type[Exporter]] = {
        'markdown': MarkdownExporter,
        'oai': OpenAIExporter,
        'chatml': ChatMLExporter
    }
    
    def __init__(self, archive_dir: Path):
        self.archive_dir = archive_dir
        self.archives: List[Archive] = []
        self.url_analyzer = URLAnalyzer()
        
    def load_archives(self) -> None:
        """Load all archives from the directory."""
        if not self.archive_dir.exists():
            logger.error(f"Archive directory does not exist: {self.archive_dir}")
            return
        
        for archive_file in self.archive_dir.glob("*_archive.json"):
            try:
                archive = Archive(archive_file)
                archive.load()
                self.archives.append(archive)
            except Exception as e:
                logger.error(f"Failed to load archive {archive_file}: {e}")
    
    def analyze_urls(self) -> pd.DataFrame:
        """Analyze URLs in all archives."""
        return self.url_analyzer.analyze_archives(self.archives)
    
    def create_output_dirs(self, base_dir: Path) -> dict[str, Path]:
        """Create output directories for different formats."""
        dirs = {fmt: base_dir / fmt for fmt in self.EXPORTERS}
        for dir_path in dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        return dirs
    
    def export_all(self, format_type: str, output_dir: Path, system_message: str = None) -> None:
        """Export all archives in specified format."""
        if format_type not in self.EXPORTERS:
            raise ValueError(f"Unsupported format: {format_type}")
        
        format_dir = output_dir / format_type
        format_dir.mkdir(parents=True, exist_ok=True)
        
        for archive in self.archives:
            if not archive.username:
                continue
            
            # Create filename with format: username_format.extension
            output_path = format_dir / f"{archive.username}_{format_type}"
            if format_type == 'markdown':
                output_path = output_path.with_suffix('.md')
            elif format_type == 'oai':
                output_path = output_path.with_suffix('.jsonl')
            else:  # chatml
                output_path = output_path.with_suffix('.json')
            
            try:
                archive.export(format_type, output_path, system_message)
            except Exception as e:
                logger.error(f"Failed to export archive {archive.username}: {e}")
    
    def export_markdown(self, archive: Archive, output_path: Path) -> None:
        """Export archive to markdown format."""
        exporter = MarkdownExporter()
        exporter.export_tweets(archive.tweets, output_path)
    
    def export_conversations_oai(self, output_path: Path, system_message: str) -> None:
        """Export conversations in OpenAI format."""
        # Create parent directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        exporter = OpenAIExporter()
        all_threads = []
        
        for archive in self.archives:
            threads = archive.get_conversation_threads()
            all_threads.extend(threads)
        
        all_threads.sort(key=lambda t: t.created_at)
        
        try:
            exporter.export_conversations(all_threads, output_path, system_message)
            logger.info(f"Exported conversations to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export conversations: {e}")
            raise

    def export_conversations_chatml(self, output_path: Path, system_message: str = None) -> None:
        """Export conversations in ChatML format."""
        exporter = ChatMLExporter(system_message=system_message)
        for archive in self.archives:
            threads = archive.get_conversation_threads()
            for thread in threads:
                exporter.export_thread(thread, output_path)