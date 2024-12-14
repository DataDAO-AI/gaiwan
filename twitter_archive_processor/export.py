#Functions to save threads, tweets, and conversations in various formats



def save_threads_as_markdown(threads: List[Thread], output_folder: str, images_folder: str):
    if not thread.contents:
        return
    
    first_tweet = thread.contents[0]
    try:
        dt = datetime.strptime(first_tweet.metadata['created_at'], '%a %b %d %H:%M:%S %z %Y')

    frontmatter = f"---\nDate: {date_str}\n---\n\n"

    #Build the thread text
    thread_text = [] 
    for tweet in thread.contents:
        text = clean_text(tweet.text, tweet.metadata.get('entities'))
        thread_text.append(text)
        thread_text.extend(process_media_files(tweet.media, images_folder))
    
    full_text = frontmatter + '\n'.join(thread_text)

    #Create a Filename
    first_words = re.sub(r'[^\w\s-]', '', cleaned).split()[:5]
    filename = "_".join(first_words) if first_words else 'thread'
    filename = f"{filename}.md"

    output_path = os.path.join(output_folder, filename)

    #link back to first tweet on twitter
    tweet_url = f"https://twitter.com/{first_tweet.metadata['user']['screen_name']}/status/{first_tweet.id}"
    tweet_link = f"[View on Twitter]({tweet_url})"

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_text)
        f.write('\n\n')
        f.write(tweet_link)

def save_conversations_to_jsonl(threads: List[Thread], conversations: List[List[Content]], output_folder: str):
    for thread, conversation in zip(threads, conversations):
        conversation_data = get_conversation_data(thread.contents)
        formatted = format_conversation(conversation_data, system_message="Conversation")
        filename = f"{thread.id}.jsonl"
        output_path = os.path.join(output_folder, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            for message in formatted["messages"]:
                f.write(json.dumps(message) + '\n')

def process_media_files(media_files: List[MediaFile], images_folder: str) -> List[str]:
    #For each media file, copy to images folder and return markdown
    links = []
    for mf in media_files:
        if os.path.isfile(mf.path):
            ext = os.path.splitext(mf.path)[1]
            new_path = os.path.join(images_folder, f"{mf.id}{ext}")
            shutil.copyfile(mf.path, new_path)
            links.append(f"![{mf.id}]({new_path})")
        else:
            logger.warning(f"Missing media file: {mf.path}")
    return links    

def save_tweets_by_date(tweets: List[Tweet], output_folder: str, images_folder: str):
    thread_ids = {c.id for t in threads for c in t.contents}

    #identify standalone tweets
    standalone_tweets = [
        c for c in all_content.values() 
        if c.id not in thread_ids
        and c.content_source == 'tweet']
        and not c.parent_id
        and not c.text.startswith('RT @')
    ]

    #Group tweets by date
    tweets_by_date = {}
    for tweet in standalone_tweets:
        try:
            date_str = tweet.timestamp.strftime('%Y-%m-%d')
            date_key = dt.date()
        except ValueError:
            logger.warning(f"Invalid date for tweet: {tweet}")
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
            dt = datetime.strftime(tweet.timestamp, '%H:%M')
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