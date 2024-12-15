""" Functions to combine, clean, and structure data """

from typing import Dict, Literal, Tuple

from twitter_archive_processor.coretypes import ConvoThread, Message, Tweet

def extract_threads_and_conversations(
        all_content: Dict[str, Tweet]
        ) -> Tuple[list[ConvoThread], list[Message]]:
    """ Extract ConvoThreads and conversation Messages from tweets """
    cts = [] # conversation threads list
    conversations = []
    #Group tweets by parent ID
    parent_map = {}
    for tweet in all_content.values():
        parent_id = tweet.parent_id
        if parent_id:
            if parent_id not in parent_map:
                parent_map[parent_id] = []
            parent_map[parent_id].append(tweet)

    #Extract ConvoThreads
    for parent_id, tweets in parent_map.items():
        parent_tweet = all_content.get(parent_id)
        if parent_tweet:
            cts.append(
                ConvoThread(
                    id=parent_id,
                    tweets=[parent_tweet] + tweets,
                    metadata=parent_tweet.metadata
                )
            )

    #Extract conversations
    for parent_id, tweets in parent_map.items():
        conversation = []
        for tweet in tweets:
            conversation.append(Message(role='user', content=tweet.text))
            for media in tweet.media:
                conversation.append(Message(role='agent', content=media.path))
        conversations.append(conversation)

    return cts, conversations

def combine_all_content(data: Dict[str, list[Tweet]]) -> list[Tweet]:
    """ combine tweet and like data """
    combined = {}
    #add tweets
    for tweet in data.get('tweets', []):
        if tweet.id:
            combined[tweet.id] = tweet

    #add likes
    for like in data.get('likes', []):
        if like.id:
            combined[like.id] = like

    return combined

def trim_conversation_to_last_assistant(conversation_data: list[Message]) -> list[Message]:
    """ Remove all user messages after the last assistant message """
    for i in range(len(conversation_data)-1, -1, -1):
        if conversation_data[i].role == 'assistant':
            return conversation_data[:i+1]
    return conversation_data

def format_conversation(conversation_data: list[Message], system_message: str) -> Dict[str, any]:
    """ format conversation data """
    formatted = {"messages": [{"role": "system", "content": system_message}]}
    formatted["messages"].extend({"role": m.role, "content": m.content} for m in conversation_data)
    return formatted

def format_message(content_pieces: list[str], role: Literal['assistant', 'user']) -> Message:
    """ format message jata, including joining content pieces """
    joined = "\n\n".join(content_pieces)
    return Message(role=role, content=joined)
