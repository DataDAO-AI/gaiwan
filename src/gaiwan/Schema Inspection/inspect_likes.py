"""Inspect like data in Twitter archive files."""
import json
import argparse
from pathlib import Path

def inspect_likes(json_file: Path):
    """Show structure of liked tweets."""
    with open(json_file) as f:
        data = json.load(f)
    
    print(f"\nInspecting likes in {json_file.name}:")
    if not isinstance(data, dict) or 'like' not in data:
        print("No likes found")
        return
        
    likes = data['like']
    if not likes:
        print("Empty likes array")
        return
        
    print(f"\nFound {len(likes)} likes")
    print("\nFirst like structure:")
    print(json.dumps(likes[0], indent=2))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('json_file', type=Path)
    args = parser.parse_args()
    inspect_likes(args.json_file)

if __name__ == '__main__':
    main() 