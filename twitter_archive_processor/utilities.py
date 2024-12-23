""" Shared Utilities to clean text, process media files, and set up logging """

import json
import logging
import os
import re
from typing import Optional
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

from gaiwan.models import CanonicalTweet, TweetMetadata
from twitter_archive_processor.coretypes import Tweet
from twitter_archive_processor.extraction import clean_json_string, extract_tweet

logger = logging.getLogger(__name__)

def clean_text(text: str, entities: Optional[dict[str, any]] = None) -> str:  
    """ clean tweet text """
    if entities and 'urls' in entities:
        for url_obj in entities['urls']:
            short_url = url_obj.get('url', '')
            full_url = url_obj.get('expanded_url', '')
            if short_url and full_url:
                text = text.replace(short_url, full_url)

    text = re.sub(r'https://t.co/\w+', '', text) #t.co link removal
    text = re.sub(r'@\w+', '', text) #mentions removal
    text = re.sub(r'#\w+', '', text) #hashtags removal  May want to include this on occasion
    text = re.sub(r'\n+', ' ', text) #Replace multiple line breaks with a single space
    text = re.sub(r'\s+', ' ', text) #Replace multiple spaces with a single space
    text = text.replace(r'\\', '\\')  # First handle double backslashes
    text = text.replace(r"\'", "'")   # Then handle escaped single quotes
    text = text.replace(r'\"', '"')   # Then handle escaped double quotes
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>') #Replace HTML entities
    text = text.replace('"', '"').replace('"', '"').replace('’', "'").replace('—', '-') #Uniform quotes and dashes
    text = text.lower() #Lowercase
    text = text.strip() #Remove leading and trailing spaces

    return text

def load_json_file(file_path: str) -> any:
    """ load json data with allowances for various wrappings in data file """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            raw_string = file.read()
            cleaned_string = clean_json_string(raw_string)
            data = json.loads(cleaned_string)

            return data
    except json.JSONDecodeError:
        #might be wrapped differently
        with open(file_path, 'r', encoding='utf=8') as f:
            raw_content = f.read()
        #extract from JS variable?
        match = re.search(r'window\.__THAR_CONFIG\s*=\s*{{.*}}', raw_content, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        else:
            logger.error("Failed to parse JSON from %s", file_path)

            return None
    except Exception as e:
        logger.warning("Error processing file '%s', %s", file_path, e)
        return None

def convert_to_canonical_tweet(tweet: Tweet) -> CanonicalTweet:
    """Convert Tweet to CanonicalTweet format"""
    return CanonicalTweet(
        id=tweet.id,
        text=tweet.text,
        author_id=tweet.metadata.get('user', {}).get('screen_name'),
        created_at=tweet.timestamp,
        reply_to_tweet_id=tweet.parent_id,
        metadata=TweetMetadata(
            mentioned_users=set(tweet.metadata.get('mentioned_users', [])),
            hashtags=set(tweet.metadata.get('hashtags', [])),
            urls=set(tweet.metadata.get('urls', [])),
            quoted_tweet_id=tweet.metadata.get('quoted_tweet_id'),
            is_retweet=tweet.metadata.get('is_retweet', False),
            retweet_of_id=tweet.metadata.get('retweet_of_id')
        ),
        liked_by=set()  # If you have this data, add it here
    )

def process_single_file(
        file_path: str, extractor: callable, media_dir: str,
        content_source: str) -> list[CanonicalTweet]:
    """ process a single json file worth of tweet data """

    #Load the file
    data = load_json_file(file_path)
    if data is None:
        return []
    #Extract the tweets
    tweets = extractor(data, media_dir, content_source)
    return [convert_to_canonical_tweet(t) for t in tweets]

def process_data_for_type(archive_path: str, data_type: str,
                          media_dir: str, content_source: str) -> list[CanonicalTweet]:
    """ process data of a given data type """
    media_folder = os.path.join(archive_path, 'data', 'tweets_media')
    contents =[]
    files_info = type_info.get('files', [])

    file_path = [os.path.join(archive_path, file_info['fileName']) for file_info in files_info]
    with ProcessPoolExecutor() as executor:
        for file_name in data_files:
            file_path = os.path.join(archive_path, 'data', file_name)
            future = executor.submit(
                process_single_file, file_path, extract_tweet,
                media_dir, content_source)
            futures.append(future)
        #Collect the results
    results = []
    for future in as_completed(futures):
        results.extend(future.result())
    return results
