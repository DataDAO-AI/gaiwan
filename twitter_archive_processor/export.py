""" Functions to save ConvoThreads, tweets, and conversations in various formats """

import datetime
import json
import logging
import os
import re
import shutil
from twitter_archive_processor.extraction import get_conversation_data
from twitter_archive_processor.transformation import format_conversation
from twitter_archive_processor.utilities import clean_text

from twitter_archive_processor.coretypes import ConvoThread, MediaFile, Tweet

logger = logging.getLogger(__name__)

def save_convo_threads_as_markdown(
        conversation_threads: list[ConvoThread], output_folder: str, images_folder: str):
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
        conversation_threads: list[ConvoThread],
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

def save_tweets_by_date(tweets: list[Tweet], output_folder: str, images_folder: str):
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
