from pathlib import Path
import pandas as pd
from typing import List
from .url_analysis.analyzer import URLAnalyzer as BaseURLAnalyzer

class URLAnalyzer(BaseURLAnalyzer):
    """Wrapper for URL analysis functionality."""
    
    def __init__(self):
        """Initialize without archive_dir as it's not needed for single archive analysis."""
        super().__init__(archive_dir=None)
    
    def analyze_archives(self, archives: List['Archive']) -> pd.DataFrame:
        """Analyze URLs across multiple archives."""
        dfs = []
        for archive in archives:
            df = self.analyze_archive(archive.file_path)
            if not df.empty:
                dfs.append(df)
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame() 