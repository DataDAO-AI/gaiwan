from pathlib import Path
import argparse
import logging
from .processor import ArchiveProcessor

def main():
    parser = argparse.ArgumentParser(description="Process Twitter archives")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_dir', type=Path, help="Directory for output files")
    parser.add_argument('--format', default='markdown', choices=['markdown'],
                       help="Output format (currently only markdown supported)")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize and run processor
    processor = ArchiveProcessor(args.archive_dir)
    processor.load_archives()
    processor.export_all(args.format, args.output_dir)

if __name__ == '__main__':
    main() 