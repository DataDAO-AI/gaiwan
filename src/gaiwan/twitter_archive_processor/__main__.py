from pathlib import Path
import argparse
import logging
from .core.processor import ArchiveProcessor

def main():
    parser = argparse.ArgumentParser(description='Process Twitter archives')
    parser.add_argument('archive_dir', type=Path, help='Directory containing Twitter archives')
    parser.add_argument('output_dir', type=Path, help='Output directory')
    parser.add_argument('--format', nargs='+', choices=['markdown', 'oai', 'chatml'], 
                       default=['markdown'], help='Output format(s)')
    parser.add_argument('--system-message', type=str, help='System message for AI formats')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    
    processor = ArchiveProcessor(args.archive_dir)
    processor.load_archives()
    
    for format_type in args.format:
        if format_type == 'oai':
            processor.export_conversations_oai(
                args.output_dir / 'oai' / 'conversations.jsonl',
                system_message=args.system_message
            )
        else:
            processor.export_all(format_type, args.output_dir, args.system_message)

if __name__ == '__main__':
    main() 