"""Generate JSON Schema from Twitter archive analysis."""

import json
import logging
import argparse
from pathlib import Path
from genson import SchemaBuilder

logger = logging.getLogger(__name__)

def generate_schema(archive_dir: Path, output_file: Path):
    """Generate JSON schema from archive files."""
    builder = SchemaBuilder()
    
    # Process each archive to build schema
    archives = list(archive_dir.glob("*_archive.json"))
    logger.info(f"Generating schema from {len(archives)} archives:")
    
    for archive_path in archives:
        try:
            logger.info(f"  Processing {archive_path.name}")
            with open(archive_path) as f:
                data = json.load(f)
                builder.add_object(data)
                
        except Exception as e:
            logger.error(f"Error processing {archive_path}: {str(e)}")
            continue
    
    # Write schema
    schema = builder.to_schema()
    logger.info(f"Writing schema to {output_file}")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(schema, f, indent=2)

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate Twitter archive schema")
    parser.add_argument('archive_dir', type=Path, help="Directory containing archives")
    parser.add_argument('output_file', type=Path, help="Output schema file")
    parser.add_argument('--force-download', action='store_true', 
                       help="Force fresh download of archives")
    args = parser.parse_args()
    
    if args.force_download:
        from gaiwan.community_archiver import download_archives, get_all_accounts
        accounts = get_all_accounts()
        download_archives(accounts, args.archive_dir)
    
    generate_schema(args.archive_dir, args.output_file)

if __name__ == '__main__':
    main()