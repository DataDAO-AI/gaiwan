#Functions to combine, clean, and structure data
from typing import Dict, List, Tuple
# Define Data Classes
from dataclasses import dataclass

@dataclass
class MediaFile:  #Data Class for Media Files
    id: str
    content_type: str
    path: str
    metadata:Dict[str, Any]

@dataclass
class Tweet: #Data Class for Individual Tweets
    id: str
    text: str
    media: List[MediaFile]
    metadata: Dict[str, Any]
    timestamp: datetime
    content_source: str

@dataclass
class Thread: #Data Class for Tweet Threads
    id: str
    tweets: List[Tweet]
    metadata: Dict[str, Any]

@dataclass 
class Message:  #Data Class for turns in a conversation for ChatML
    role: Literal["user", "agent"]   #Other roles can be added as needed
    content: str

def extract_threads_and_conversations(all_content: Dict[str, Tweet]) -> Tuple[List[Thread], List[List[Content]]]:
    threads = []
    conversations = []
    #Group tweets by parent ID
    parent_map = {}
    for tweet in all_content.values():
        parent_id = tweet.parent_id
        if parent_id:
            if parent_id not in parent_map:
                parent_map[parent_id] = []
            parent_map[parent_id].append(tweet)
    
    #Extract threads
    for parent_id, tweets in parent_map.items():
        parent_tweet = all_content.get(parent_id)
        if parent_tweet:
            thread = Thread(
                id=parent_id,
                tweets=[parent_tweet] + tweets,
                metadata=parent_tweet.metadata
            )
            threads.append(thread)
    
    #Extract conversations
    for parent_id, tweets in parent_map.items():
        conversation = []
        for tweet in tweets:
            conversation.append(Message(role='user', content=tweet.text))
            for media in tweet.media:
                conversation.append(Message(role='agent', content=media.path))
        conversations.append(conversation)
    
    return threads, conversations

def combine_all_content(data: Dict[str, List[Tweet]]) -> List[Tweet]:
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

def trim_conversation_to_last_assistant(conversation_data: List[Message]) -> List[Message]:
    #Remove all user messages after the last assistant message
    for i in range(len(conversation_data)-1, -1, -1):
        if conversation_data[i].role == 'assistant':
            return conversation_data[:i+1]
    return conversation_data

def format_conversation(conversation_data: List[Message], system_message: str) -> Dict[str, Any]:
    formatted = {"messages": [{"role": "system", "content": system_message}]}
    formatted["messages"].extend({"role": m.role, "content": m.content} for m in conversation_data)
    return formatted

def format_message(content_pieces: List[str], role: Literal['assistant', 'user']) -> Message:
    joined = "\n\n".join
    return Message(role=role, content=joined)
