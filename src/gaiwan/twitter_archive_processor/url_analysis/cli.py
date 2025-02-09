from pathlib import Path
import argparse
import logging
from datetime import datetime, timezone
import pandas as pd
from tqdm import tqdm
from .analyzer import URLAnalyzer

logger = logging.getLogger(__name__)

class URLAnalysisReporter:
    """Handles reporting and statistics for URL analysis results."""
    
    def __init__(self, df: pd.DataFrame, analyzer: URLAnalyzer):
        self.df = df
        self.analyzer = analyzer
        
    def print_overall_stats(self):
        """Print overall statistics about the analysis."""
        print("\nOverall Statistics:")
        print(f"Archives analyzed: {self.df['username'].nunique()}")
        print(f"Total URLs found: {len(self.df)}")
        print(f"Unique URLs: {self.df['url'].nunique()}")
        
    def print_fetch_stats(self):
        """Print metadata fetch statistics."""
        print("\nMetadata Fetch Statistics:")
        fetch_stats = self.df['fetch_status'].value_counts()
        print(fetch_stats)
        
        if 'failed' in fetch_stats:
            print("\nTop failure reasons:")
            print(self.df[self.df['fetch_status'] == 'failed']['fetch_error'].value_counts().head())
            
    def print_domain_analysis(self):
        """Print analysis of different URL types and domains."""
        # Create masks for different categories
        unresolved_mask = (~self.df['is_resolved']) & (self.df['raw_domain'].isin(self.analyzer.domain_normalizer.shortener_domains))
        twitter_internal_mask = self.df['url'].str.contains(r'https?://(?:(?:www\.|m\.)?twitter\.com|x\.com)/\w+/status/', na=False)
        
        # Separate dataframes
        unresolved_df = self.df[unresolved_mask]
        twitter_internal_df = self.df[twitter_internal_mask & ~unresolved_mask]
        active_df = self.df[~unresolved_mask & ~twitter_internal_mask]
        
        self._print_unresolved_stats(unresolved_df)
        self._print_twitter_stats(twitter_internal_df)
        self._print_external_domains(active_df)
        self._print_protocol_stats()
        
    def _print_unresolved_stats(self, df: pd.DataFrame):
        print("\nUnresolved Shortened URLs:")
        if not df.empty:
            print(f"Total unresolved shortened URLs: {len(df)}")
            print("\nUnresolved domains breakdown:")
            print(df['raw_domain'].value_counts())
            print("\nNote: These are shortened URLs that could not be resolved. Check url_resolution.log for details.")
        else:
            print("No unresolved shortened URLs found")
            
    def _print_twitter_stats(self, df: pd.DataFrame):
        print("\nTwitter Internal Link Statistics:")
        if not df.empty:
            print(f"Total internal Twitter links: {len(df)}")
            print(f"Unique Twitter conversations referenced: {df['url'].nunique()}")
        else:
            print("No internal Twitter links found")
            
    def _print_external_domains(self, df: pd.DataFrame):
        print("\nTop 20 External Domains:")
        domain_counts = df['domain'].value_counts()
        if not domain_counts.empty:
            print(domain_counts.head(20))
        else:
            print("No external domains found")
            
    def _print_protocol_stats(self):
        print("\nProtocols used:")
        print(self.df['protocol'].value_counts())

def setup_logging(debug: bool):
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    if not debug:
        debug_handler = logging.FileHandler('url_resolution.log')
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(debug_handler)

def main():
    """Command-line interface for URL analysis."""
    parser = argparse.ArgumentParser(description="Analyze URLs in Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument('--output', type=Path, help="Save DataFrame to CSV/Parquet file")
    parser.add_argument('--force', action='store_true', help="Force reanalysis of all archives")
    args = parser.parse_args()
    
    setup_logging(args.debug)
    
    output_file = args.output or Path('urls.parquet')
    analyzer = URLAnalyzer(args.archive_dir)
    
    df = process_archives(analyzer, output_file, args.force)
    if df is not None:
        reporter = URLAnalysisReporter(df, analyzer)
        reporter.print_overall_stats()
        reporter.print_fetch_stats()
        reporter.print_domain_analysis()
        save_results(df, output_file)

async def process_archives(analyzer: URLAnalyzer, output_file: Path, force: bool = False):
    """Process all archives and save results."""
    existing_df = None if force else load_existing_data(output_file)
    
    new_df = await analyzer._analyze_archives_async()
    if existing_df is not None:
        new_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    if not new_df.empty:
        save_results(new_df, output_file)
    return new_df

def load_existing_data(output_file: Path) -> pd.DataFrame:
    """Load existing analysis data if available."""
    if output_file.exists():
        try:
            df = pd.read_parquet(output_file)
            logger.info(f"Loaded existing data with {len(df)} URLs")
            return df
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
    return None

def save_results(df: pd.DataFrame, output_file: Path):
    """Save analysis results to file."""
    if output_file.exists():
        backup_path = output_file.with_name(
            f"urls_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        output_file.rename(backup_path)
        logger.info(f"Created backup at {backup_path}")
    
    df.to_parquet(output_file)
    logger.info(f"Saved data to {output_file}")

if __name__ == '__main__':
    main()