"""Generate JSON schema from example files."""

import json
import logging
from pathlib import Path
from typing import List
from genson import SchemaBuilder
import pyarrow.parquet as pq
import jsonschema

logger = logging.getLogger(__name__)

CANONICAL_SCHEMA = {
    "type": "object",
    "properties": {
        "tweets": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "text": {"type": "string"},
                    "author_username": {"type": "string"},
                    "retweet_count": {"type": "integer"},
                    "in_reply_to_status_id": {"type": ["string", "null"]},
                    "in_reply_to_username": {"type": ["string", "null"]},
                    "quoted_tweet_id": {"type": ["string", "null"]},
                    "likers": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["id", "created_at", "text", "author_username"]
            }
        },
        "profiles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "username": {"type": "string"},
                    "bio": {"type": "string"},
                    "website": {"type": "string"},
                    "location": {"type": "string"},
                    "avatar_url": {"type": "string"},
                    "header_url": {"type": ["string", "null"]}
                },
                "required": ["username", "bio", "website", "location", "avatar_url"]
            }
        }
    },
    "required": ["tweets", "profiles"]
}

def validate_schema(data: dict) -> bool:
    """Validate data against canonical schema."""
    try:
        jsonschema.validate(data, CANONICAL_SCHEMA)
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Schema validation failed: {e}")
        return False

def generate_schema(files: List[Path], output_file: Path):
    """Generate schema from Parquet or JSON files."""
    logger.info(f"Generating schema from {len(files)} files:")
    
    builder = SchemaBuilder()
    for file in files:
        logger.info(f"  Processing {file}")
        
        # Load data based on file type
        if file.suffix == '.json':
            with open(file) as f:
                data = json.load(f)
        elif file.suffix == '.parquet':
            table = pq.read_table(file)
            data = table.to_pylist()
        else:
            logger.warning(f"Skipping unknown file type: {file}")
            continue
            
        builder.add_object(data)
    
    # Write schema
    logger.info(f"Writing schema to {output_file}")
    with open(output_file, 'w') as f:
        json.dump(builder.to_schema(), f, indent=2)

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