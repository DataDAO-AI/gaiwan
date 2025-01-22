from pathlib import Path
import json
from twitter_archive_processor.tweet import Tweet

class Archive:
    def __init__(self, archive_path: Path):
        self.archive_path = archive_path
        self.tweets = []

    def load(self):
        with open(self.archive_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            self.tweets = [self._create_tweet(tweet_data) for tweet_data in data.get('tweets', [])]

    def _create_tweet(self, tweet_data):
        return Tweet(
            id=tweet_data.get('id'),
            text=tweet_data.get('full_text'),
            created_at=tweet_data.get('created_at'),
            media=tweet_data.get('media', []),
            parent_id=tweet_data.get('in_reply_to_status_id')
        )