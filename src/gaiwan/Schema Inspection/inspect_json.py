"""Inspect JSON structure of Twitter archive files."""
import json
import argparse
from pathlib import Path

def inspect_first_items(json_file: Path):
    """Show structure of first items of each type."""
    with open(json_file) as f:
        data = json.load(f)
    
    # Regular tweet
    if data.get('tweets'):
        print("\nREGULAR TWEET STRUCTURE:")
        print(json.dumps(data['tweets'][0], indent=2))
    
    # Community tweet
    if data.get('community-tweet'):
        print("\nCOMMUNITY TWEET STRUCTURE:")
        print(json.dumps(data['community-tweet'][0], indent=2))
    
    # Note tweet
    if data.get('note-tweet'):
        print("\nNOTE TWEET STRUCTURE:")
        print(json.dumps(data['note-tweet'][0], indent=2))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('json_file', type=Path)
    args = parser.parse_args()
    inspect_first_items(args.json_file)

if __name__ == '__main__':
    main() 