""" A collection of types used by several modules within the project """
from dataclasses import dataclass
import datetime
from typing import Dict, Any, List, Optional, Union, Literal

@dataclass
class MediaFile:
    """ Data Class for Media Files """
    id: str
    content_type: str
    path: str
    metadata: Dict[str, Any]


@dataclass
class Tweet:
    """ Data Class for Individual Tweets """
    id: str
    parent_id: str
    text: str
    media: List[MediaFile]
    metadata: Dict[str, Any]
    timestamp: datetime.datetime
    content_source: str


@dataclass
class ConvoThread:
    """ Data Class for Tweet ConvoThreads """
    id: str
    tweets: List[Tweet]
    metadata: Dict[str, Any]

@dataclass
class Message:
    """ Data Class for turns in a conversation for ChatML """
    role: Literal["user", "agent"]   #Other roles can be added as needed
    content: str

# Type alias for content that can be processed
Content = Union[Tweet, Message, MediaFile]

__all__ = ['MediaFile', 'Tweet', 'ConvoThread', 'Message', 'Content']
