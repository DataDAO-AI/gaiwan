""" A collection of types used by several modules within the project """
from dataclasses import dataclass
import datetime
from typing import Literal

@dataclass
class MediaFile:
    """ Data Class for Media Files """
    id: str
    content_type: str
    path: str
    metadata: dict[str, any]


@dataclass
class Tweet:
    """ Data Class for Individual Tweets """
    id: str
    parent_id: str
    text: str
    media: list[MediaFile]
    metadata: dict[str, any]
    timestamp: datetime
    content_source: str


@dataclass
class ConvoThread:
    """ Data Class for Tweet ConvoThreads """
    id: str
    tweets: list[Tweet]
    metadata: dict[str, any]

@dataclass
class Message:
    """ Data Class for turns in a conversation for ChatML """
    role: Literal["user", "agent"]   #Other roles can be added as needed
    content: str
