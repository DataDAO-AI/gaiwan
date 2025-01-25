""" Functions to save ConvoThreads, tweets, and conversations in various formats """

import datetime
import json
import logging
import os
import re
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List
from datetime import datetime, timezone

# Change these absolute imports to relative ones
from .extraction import get_conversation_data
from .transformation import format_conversation
from .utilities import clean_text
from .coretypes import ConvoThread, MediaFile, Tweet, Content
from .conversation import ConversationThread
from .models import CanonicalTweet

logger = logging.getLogger(__name__)

class Exporter(ABC):
    """Base class for archive exporters."""
    
    @abstractmethod
    def export_tweets(self, tweets: List[Tweet], output_path: Path) -> None:
        """Export tweets to the specified format."""
        pass
    
    @abstractmethod
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        """Export a conversation thread to the specified format."""
        pass

class MarkdownExporter(Exporter):
    """Export tweets to Markdown format."""
    
    def export_tweets(self, tweets: List[Tweet], output_path: Path) -> None:
        with open(output_path, 'w', encoding='utf-8') as f:
            for tweet in sorted(tweets, key=lambda t: t.created_at or datetime.min.replace(tzinfo=timezone.utc)):
                self._write_tweet(f, tweet)
    
    def export_thread(self, thread: ConversationThread, output_path: Path) -> None:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"# Thread started on {thread.created_at:%Y-%m-%d %H:%M:%S}\n\n")
            for tweet in thread.all_tweets:
                self._write_tweet(f, tweet)
    
    def _write_tweet(self, f, tweet: Tweet) -> None:
        """Write a single tweet to the markdown file."""
        if tweet.created_at:
            f.write(f"## {tweet.created_at:%Y-%m-%d %H:%M:%S}\n\n")
        f.write(f"{tweet.text}\n\n")
        for media in tweet.media:
            f.write(f"![{media.get('type', 'media')}]({media.get('media_url', '')})\n\n")

def save_convo_threads_as_markdown(
        conversation_threads: list[CanonicalTweet], output_folder: str, images_folder: str):
    """ save conversation threads as markdown text """
    if not conversation_threads.contents:
        return

    first_tweet = conversation_threads.contents[0]
    dt = datetime.datetime.strptime(first_tweet.metadata['created_at'], '%a %b %d %H:%M:%S %z %Y')

    frontmatter = f"---\nDate: {dt}\n---\n\n"

    #Build the ConvoThread text
    convo_thread_text = []
    for tweet in conversation_threads.contents:
        text = clean_text(tweet.text, tweet.metadata.get('entities'))
        convo_thread_text.append(text)
        convo_thread_text.extend(process_media_files(tweet.media, images_folder))

    full_text = frontmatter + '\n'.join(convo_thread_text)

    #Create a Filename
    first_words = re.sub(r'[^\w\s-]', '', cleaned).split()[:5]
    filename = "_".join(first_words) if first_words else 'ConvoThread'
    filename = f"{filename}.md"

    output_path = os.path.join(output_folder, filename)

    #link back to first tweet on twitter
    tweet_url = \
        f"https://twitter.com/{first_tweet.metadata['user']['screen_name']}/status/{first_tweet.id}"
    tweet_link = f"[View on Twitter]({tweet_url})"

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_text)
        f.write('\n\n')
        f.write(tweet_link)

def save_conversations_to_jsonl(
        conversation_threads: list[CanonicalTweet],
        conversations: list[list[Content]],
        output_folder: str):
    """ save conversation threads in jsonl file """

    for convo_thread, conversation in zip(conversation_threads, conversations):
        conversation_data = get_conversation_data(convo_thread.contents)
        formatted = format_conversation(conversation_data, system_message="Conversation")
        filename = f"{convo_thread.id}.jsonl"
        output_path = os.path.join(output_folder, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            for message in formatted["messages"]:
                f.write(json.dumps(message) + '\n')

def process_media_files(media_files: list[MediaFile], images_folder: str) -> list[str]:
    """ For each media file, copy to images folder and return markdown """
    links = []
    for mf in media_files:
        if os.path.isfile(mf.path):
            ext = os.path.splitext(mf.path)[1]
            new_path = os.path.join(images_folder, f"{mf.id}{ext}")
            shutil.copyfile(mf.path, new_path)
            links.append(f"![{mf.id}]({new_path})")
        else:
            logger.warning("Missing media file: '%s'", mf.path)
    return links

def save_tweets_by_date(tweets: list[CanonicalTweet], output_folder: str, images_folder: str):
    """ save tweets grouped by date """
    tweet_ids = {c.id for t in tweets for c in t.contents}

    #identify standalone tweets
    standalone_tweets = [
        c for c in all_content.values()
        if c.id not in tweet_ids
        and c.content_source == ['tweet']
        and not c.parent_id
        and not c.text.startswith('RT @')
    ]

    #Group tweets by date
    tweets_by_date = {}
    for tweet in standalone_tweets:
        try:
            date_str = tweet.timestamp.strftime('%Y-%m-%d')
            date_key = datetime.date()
        except ValueError:
            logger.warning("Invalid date for tweet: %s", tweet)
            continue
        if date_key not in tweets_by_date:
            tweets_by_date[date_key] = []
        tweets_by_date[date_key].append(tweet)

    #Save tweets to markdown files
    for date_key, day_tweets in tweets_by_date.items():
        day_tweets.sort(key=lambda t: t.timestamp)

        contents = []
        for tweet in day_tweets:
            text = clean_text(tweet.text, tweet.metadata.get('entities'))
            media_links = process_media_files(tweet.media, images_folder)
            dt = datetime.datetime.strftime(tweet.timestamp, '%H:%M')
            time_str = dt.strftime('%H:%M')
            block = f"### {time_str}\n\n{text}\n\n{''.join(media_links)}"
            if media_links:
                block += '\n\n' + '\n\n'.join(media_links)
            contents.append(block)

    full_text = '\n\n'.join(contents)
    filename = f"{date_key}.md"
    output_path = os.path.join(output_folder, filename)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_text)
