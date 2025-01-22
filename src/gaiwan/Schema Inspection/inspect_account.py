"""Inspect account data in Twitter archive files."""
import json
import argparse
from pathlib import Path

def inspect_all_accounts(archive_dir: Path):
    """Show account data from all archive files."""
    for json_file in archive_dir.glob('*.json'):
        try:
            with open(json_file) as f:
                data = json.load(f)
            
            print(f"\n{json_file.name}:")
            print(f"Data type: {type(data)}")
            if isinstance(data, list):
                print(f"List length: {len(data)}")
                if data:
                    print("First item type:", type(data[0]))
                    if isinstance(data[0], dict):
                        print("First item keys:", list(data[0].keys()))
                        if 'account' in data[0]:
                            print("Account data:")
                            print(json.dumps(data[0]['account'], indent=2))
                        else:
                            print("No account key in first item")
                    else:
                        print("First item is not a dict")
            else:
                print("Data is not a list")
                if isinstance(data, dict):
                    print("Root keys:", list(data.keys()))
                    if 'account' in data:
                        print("Account data:")
                        print(json.dumps(data['account'], indent=2))
                    else:
                        print("No account key at root")
        except Exception as e:
            print(f"Error reading file: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('archive_dir', type=Path)
    args = parser.parse_args()
    inspect_all_accounts(args.archive_dir)

if __name__ == '__main__':
    main() 