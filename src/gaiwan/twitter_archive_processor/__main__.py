from pathlib import Path
import argparse
import logging
from .processor import ArchiveProcessor

def main():
    parser = argparse.ArgumentParser(description="Process Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_dir', type=Path, help="Directory for output files")
    parser.add_argument('--format', default=['markdown'], nargs='+',
                       choices=['markdown', 'oai'],
                       help="Output formats (markdown, oai)")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument('--system-message', 
                       default="You have been uploaded to the internet",
                       help="System message for OpenAI format")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize and run processor
    processor = ArchiveProcessor(args.archive_dir)
    processor.load_archives()
    
    # Create output directories
    output_dir = args.output_dir
    output_dir.mkdir(exist_ok=True)
    
    # Export in requested formats
    for format_type in args.format:
        if format_type == 'markdown':
            processor.export_all('markdown', output_dir)
        elif format_type == 'oai':
            oai_path = output_dir / 'conversations_oai.jsonl'
            processor.export_conversations_oai(oai_path, args.system_message)

if __name__ == '__main__':
    main() 