""" Functions to extract data from files and parse content """

import datetime
import os
import re
import logging
from typing import Any, Dict, Tuple, Optional, List

from .coretypes import MediaFile, Message, Tweet, Content
from .common import clean_json_string, load_json_file
from .transformation import format_message, \
    trim_conversation_to_last_assistant

logger = logging.getLogger(__name__)

def extract_manifest(archive_path: str) -> Dict[str, Any]:
    """ load manifest data """
    manifest_path = os.path.join(archive_path, 'data', 'manifest.js')
    data = load_json_file(manifest_path)
    if data is None or 'dataTypes' not in data:  # If lacking key
        logger.error("Failed to extract manifest from %s", archive_path)
        return {}
    return data

def get_media_files(tweet_id: str, media_data: Dict[str, Any], media_dir: str) -> list[str]:
    """ obtain list of media files """
    # list of media files
    try:
        all_files = os.listdir(media_dir)  # Edit media_folder to actual file path
    except Exception as e:
        logger.error("Error reading media folder '%s': %s", media_dir, e)
        return []
    # Filter those with non-zero file size and tweet_id
    result = []
    for fname in all_files:
        if fname.startswith(f"{tweet_id}-"):
            fpath = os.path.join(media_dir, fname)
            if os.path.getsize(fpath) > 0:
                result.append(fname)
    return result

def get_media_type(filename: str) -> str:
    """ determine media type by extension """
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif']:
        return 'image'
    elif ext in ['.mp4', '.mov', '.avi']:
        return 'video'
    else:
        return 'unknown'

def extract_tweet(item: dict[str, Any], content_source: str, media_dir:str) -> Optional[Tweet]:
    """ extract tweet data for a single tweet """
    tweet_id = item.get('id') or item.get('tweetId') #check file type to verify which one is used
    if not tweet_id:
        logger.warning("Tweet missing ID: %s", item)
        return None

    #Check actual file type to verify which one is used
    text = item.get['text'] or item.get('full_text') or item.get('fullText', '')
    #Check keys used in data
    timestamp = datetime.datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S %z %Y')
    parent_id = item.get('in_reply_to_status_id', None)

    #Append associated media files
    media_file_names = get_media_files(tweet_id, item, media_dir)
    media_file_objects = []
    for fname in media_file_names:
        media_type = get_media_type(fname)
        media_path = os.path.join(media_dir, fname)
        metadata = {
            'parent_tweet': item
            #Add more metadata as needed
        }
        media_file_objects.append(MediaFile(
            id=f"{tweet_id}_{os.path.splitext(fname)[0]}",
            content_type=media_type,
            path=media_path,
            metadata=metadata
        ))
    return Tweet(
        id=id,
        text=text,
        metadata=item,
        timestamp=timestamp,
        parent_id=parent_id,
        media=media_file_objects,
        content_source=content_source
    )

def extract_convo_thread(
        file_data: list[Dict[str, Any]],
        media_dir: str,
        content_source: str) -> list[Tweet]:
    """ extract conversation thread from tweet data """
    results = []
    for entry in file_data:
        tweet_data = entry.get('tweet')
        if tweet_data:
            tweet = extract_tweet(tweet_data, content_source, media_dir)
            if tweet:
                results.append(tweet)
    return results

def extract_likes(
        file_data: list[Dict[str, Any]],
        media_dir: str,
        content_source: str) -> list[Tweet]:
    """ extract 'Like' data """
    results = []
    for entry in file_data:
        tweet_data = entry.get('like')
        if tweet_data:
            tweet = extract_tweet(tweet_data, content_source, media_dir)
            if tweet:
                results.append(tweet)
    return results

def extract_archive_data(archive_path: str) -> Dict[str, list[Tweet]]:
    """ extract archive data """
    manifest_data = extract_manifest(archive_path)
    if not manifest_data:
        logger.error("No manifest data found.")
        return {}

    data_types = manifest_data.get('dataTypes', {})
    if not data_types:
        logger.warning("No dataTypes found in manifest.")
        return {}

    #Map known data types to their extractor functions
    extractors = {
        'tweet': extract_convo_thread,
        'like': extract_likes
    }

    results = {}
    for data_type, type_info in data_types.items():
        if data_type in extractors:
            results[data_type] = process_data_for_type(
                archive_path, data_type, type_info, media_dir, type_info['contentSource']
            )
        else:
            logger.warning("Unknown data type: %s", data_type)

    return results

def get_conversation_data(contents: List[Content]) -> List[Message]:
    """Extract conversation data from content."""
    messages = []
    for content in contents:
        if isinstance(content, Tweet):
            messages.append(Message(role='user', content=content.text))
            for media in content.media:
                messages.append(Message(role='assistant', content=media.path))
    return messages

def get_conversation_texts(contents: list[Content]) -> list[Tuple[str, str]]:
    """ obrain converation text content bodies """
    result = []
    for content in contents:
        if isinstance(content, Message):
            result.append((content.role, content.content))
    return result

def extract_note_tweets(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and clean Note Tweets from JSON data.
    
    Args:
        data: The JSON data of a user as a Python dictionary.
        
    Returns:
        A list of dictionaries, each containing cleaned Note Tweet data.
    """
    note_tweets = []

    for note_entry in data.get("note-tweet", []):
        note_tweet = note_entry.get("noteTweet", {})
        core = note_tweet.get("core", {})

        # Extract fields
        note_id = note_tweet.get("noteTweetId", "")
        created_at = note_tweet.get("createdAt", "")
        text = core.get("text", "")

        # Metadata: URLs, mentions, hashtags
        urls = [url.get("expandedUrl", "") for url in core.get("urls", [])]
        mentions = [mention.get("screenName", "") for mention in core.get("mentions", [])]
        hashtags = core.get("hashtags", [])

        # Clean the text
        cleaned_text = clean_text(text)

        # Append cleaned data
        note_tweets.append({
            "noteTweetId": note_id,
            "createdAt": created_at,
            "text": cleaned_text,
            "urls": urls,
            "mentions": mentions,
            "hashtags": hashtags
        })

    return note_tweets
