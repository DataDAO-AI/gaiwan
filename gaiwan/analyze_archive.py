"""Analyze Twitter archive.json structure and generate schema documentation."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Set, Union
from collections import defaultdict
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ArchiveAnalyzer:
    """Analyzes Twitter archive structure and collects sample values."""

    def __init__(self):
        self.type_samples = defaultdict(lambda: defaultdict(set))
        self.path_counts = defaultdict(int)
        self.max_samples = 5  # Number of sample values to keep per field
        self.tweets_with_urls = []  # Store tweets that contain URLs

    def _collect_field_info(self, data: Any, path: str = "") -> None:
        """Recursively collect type and sample information for all fields."""
        if isinstance(data, dict):
            # Check for tweets with URLs in different types of content
            if 'tweets' in data:
                for tweet_container in data['tweets']:
                    tweet = tweet_container.get('tweet', {})
                    if ('entities' in tweet and 
                        'urls' in tweet['entities'] and 
                        tweet['entities']['urls']):
                        self.tweets_with_urls.append({
                            'type': 'tweet',
                            'full_text': tweet.get('full_text', ''),
                            'entities': tweet['entities']
                        })
            elif 'community-tweet' in data:
                for tweet_container in data['community-tweet']:
                    tweet = tweet_container.get('tweet', {})
                    if ('entities' in tweet and 
                        'urls' in tweet['entities'] and 
                        tweet['entities']['urls']):
                        self.tweets_with_urls.append({
                            'type': 'community_tweet',
                            'full_text': tweet.get('full_text', ''),
                            'entities': tweet['entities']
                        })
            elif 'like' in data:
                like = data['like']
                if 'expandedUrl' in like:
                    self.tweets_with_urls.append({
                        'type': 'like',
                        'full_text': like.get('fullText', ''),
                        'expanded_url': like['expandedUrl']
                    })
            elif 'noteTweet' in data:
                note = data['noteTweet']
                if 'core' in note and 'urls' in note['core'] and note['core']['urls']:
                    self.tweets_with_urls.append({
                        'type': 'note',
                        'text': note['core'].get('text', ''),
                        'urls': note['core']['urls']
                    })
            
            for key, value in data.items():
                new_path = f"{path}.{key}" if path else key
                self.path_counts[new_path] += 1
                self._collect_field_info(value, new_path)
        
        elif isinstance(data, list):
            if data:
                self.path_counts[path] += 1
                for item in data[:3]:
                    self._collect_field_info(item, path)
        
        else:
            data_type = type(data).__name__
            if data is not None:
                samples = self.type_samples[path][data_type]
                if len(samples) < self.max_samples:
                    samples.add(str(data))

    def analyze_file(self, filepath: Path) -> Dict[str, Any]:
        """Analyze a Twitter archive file and return its schema."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Analyzing {filepath}")
            self._collect_field_info(data)
            
            # Log URL statistics
            if self.tweets_with_urls:
                logger.info(f"\nFound {len(self.tweets_with_urls)} items containing URLs:")
                url_types = {}
                for item in self.tweets_with_urls:
                    url_types[item['type']] = url_types.get(item['type'], 0) + 1
                
                for type_name, count in url_types.items():
                    logger.info(f"- {type_name}: {count}")
                
                # Show example of each type
                logger.info("\nExample of each type:")
                shown_types = set()
                for item in self.tweets_with_urls:
                    if item['type'] not in shown_types:
                        logger.info(f"\n{item['type'].upper()} example:")
                        logger.info(json.dumps(item, indent=2))
                        shown_types.add(item['type'])
            else:
                logger.info("\nNo items with URLs found")
            
            schema = self._format_schema()
            
            # Save URL examples
            if self.tweets_with_urls:
                url_examples_path = filepath.parent / f"{filepath.stem}_url_examples.json"
                with open(url_examples_path, 'w', encoding='utf-8') as f:
                    json.dump(self.tweets_with_urls, f, indent=2, ensure_ascii=False)
                logger.info(f"\nURL examples saved to {url_examples_path}")
            
            return schema
            
        except Exception as e:
            logger.error(f"Error analyzing {filepath}: {e}")
            return {}

    def _format_schema(self) -> Dict[str, Any]:
        """Format collected information into a readable schema."""
        schema = {}
        for path, count in sorted(self.path_counts.items()):
            path_info = {
                "count": count,
                "types": {}
            }
            for data_type, samples in self.type_samples[path].items():
                path_info["types"][data_type] = {
                    "samples": sorted(samples)
                }
            schema[path] = path_info
        return schema

def main():
    """Analyze Twitter archive structure."""
    parser = argparse.ArgumentParser(description="Analyze Twitter archive structure")
    parser.add_argument('archive_path', type=Path, help="Path to archive.json file")
    args = parser.parse_args()

    analyzer = ArchiveAnalyzer()
    schema = analyzer.analyze_file(args.archive_path)
    
    logger.info("\nTop-level keys found:")
    for key in sorted(k.split('.')[0] for k in schema.keys()):
        logger.info(f"- {key}")

if __name__ == "__main__":
    main() 