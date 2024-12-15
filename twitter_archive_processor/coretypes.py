from dataclasses import dataclass
import datetime

@dataclass
class MediaFile:  #Data Class for Media Files
    id: str
    content_type: str
    path: str
    metadata: dict[str, any]


@dataclass
class Tweet: #Data Class for Individual Tweets
    id: str
    text: str
    media: list[MediaFile]
    metadata: dict[str, any]
    timestamp: datetime
    content_source: str


@dataclass
class ConvoThread: #Data Class for Tweet ConvoThreads
    id: str
    tweets: list[Tweet]
    metadata: dict[str, any]