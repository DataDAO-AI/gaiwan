#Functions to extract data from files and parse content
from coretypes import MediaFile, Tweet
import datetime
import os
import json
import re
import logging
from typing import Any, Dict, Tuple, list, Optional

from twitter_archive_processor.utilities import load_json_file, process_data_for_type

logger = logging.getLogger(__name__)

def clean_json_string(json_string: str) -> str:
    #Clean the leading pattern:
    cleaned = re.sub(r'^window\.[^=]+=]+=\s*', '' , json_string.strip()) 
    #Clean the trailing pattern:
    cleaned = cleaned.rstrip(';')

    return cleaned


def extract_manifest(archive_path: str) -> Dict[str, Any]:
    manifest_path = os.path.join(archive_path, 'data', 'manifest.js')
    data = load_json_file(manifest_path)
    if data is None or 'dataTypes' not in data:  # If lacking key
        logger.error(f"Failed to extract manifest from {archive_path}")
        return {}
    return data

def get_media_files(tweet_id: str, media_data: Dict[str, Any], media_dir: str) -> list[str]:
    # list of media files
    try:
        all_files = os.listdir(media_dir)  # Edit media_folder to actual file path
    except Exception as e:
        logger.error(f"Error reading media folder {media_dir}: {e}")
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
    #determine media type by extension.
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif']:
        return 'image'
    elif ext in ['.mp4', '.mov', '.avi']:
        return 'video'
    else:
        return 'unknown'
    


def extract_tweet(item: dict[str, Any], content_source: str, media_dir:str) -> Optional[Tweet]:  #extract tweet data for a single tweet
    tweet_id = item.get('id') or item.get('tweetId') #check file type to verify which one is used
    if not tweet_id:
        logger.warning(f"Tweet missing ID: {item}")
        return None
    text = item.get['text'] or item.get('full_text') or item.get('fullText', '')  #Check actual file type to verify which one is used
    timestamp = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S %z %Y')  #Check keys used in data
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
            id=f"{tweet_id}_{os.path.splittext(fname)[0]}",
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
        media_files=media_file_objects,
        content_source=content_source
    )

def extract_ConvoThread(file_data: list[Dict[str, Any]], media_dir: str, content_source: str) -> list[Tweet]:
    results = []
    for entry in file_data:
        tweet_data = entry.get('tweet')
        if tweet_data:
            tweet = extract_tweet(tweet_data, content_source, media_dir)
            if tweet:
                results.append(tweet)
    return results

def extract_likes(file_data: list[Dict[str, Any]], media_dir: str, content_source: str) -> list[Tweet]:
    results = []
    for entry in file_data:
        tweet_data = entry.get('like')
        if tweet_data:
            tweet = extract_tweet(tweet_data, content_source, media_dir)
            if tweet:
                results.append(tweet)
    return results    

def extract_archive_data(archive_path: str) -> Dict[str, list[Tweet]]:
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
        'tweet': extract_ConvoThread,
        'like': extract_likes
    }

    results = {}
    for data_type, type_info in data_types.items():
        if data_type in extractors:
            results[data_type] = process_data_for_type(archive_path, data_type, type_info, media_dir, type_info['contentSource'])
        else:
            logger.warning(f"Unknown data type: {data_type}")
    
    return results

def get_conversation_data(contents: list[Content]) -> list[Message]:
    conversation_data = []
    current_role = None
    buffer = []

    for text, role in get_conversation_texts(contents):
        cleaned = clean_text(text, contents[0].metadata.get('entities'))
        if not cleaned:
            continue
        if role != current_role:
            if buffer and current_role:
                conversation_data.append(format_message(buffer, current_role)) 
                buffer = []
            current_role = role
        buffer.append(cleaned)
    
    if buffer and current_role: 
        conversation_data.append(format_message(buffer, current_role))
    
    conversation_data = trim_conversation_to_last_assistant(conversation_data)
    return conversation_data

def get_conversation_texts(contents: list[Content]) -> list[Tuple[str, str]]:
    result = []
    for content in contents:
        if isinstance(content, Message):
            result.append((content.role, content.content))
    return result
