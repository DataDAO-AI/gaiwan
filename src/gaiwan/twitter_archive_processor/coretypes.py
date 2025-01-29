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
class Message:
    """ Data Class for turns in a conversation for ChatML """
    role: Literal["user", "agent"]
    content: str

# Update Content type to use BaseTweet
from .tweets.base import BaseTweet
Content = Union[BaseTweet, Message, MediaFile]

__all__ = ['MediaFile', 'Message', 'Content']
