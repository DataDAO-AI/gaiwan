"""JSON Schema definition for Twitter archives."""

TWITTER_ARCHIVE_SCHEMA = {
    "type": "object",
    "properties": {
        # Account Information
        "account": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "account": {
                        "type": "object",
                        "properties": {
                            "accountId": {"type": "string"},  # Numeric ID as string
                            "username": {"type": "string"},   # @ handle, lowercase
                            "accountDisplayName": {"type": "string"},
                            "createdAt": {"type": "string"},  # ISO format
                            "createdVia": {"type": "string"}  # e.g., "web"
                        }
                    }
                }
            }
        },

        # Regular Tweets
        "tweets": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "tweet": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "id_str": {"type": "string"},  # Same as id
                            "created_at": {"type": "string"},  # Twitter format
                            "full_text": {"type": "string"},
                            "entities": {
                                "type": "object",
                                "properties": {
                                    "urls": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "url": {"type": "string"},         # t.co short URL
                                                "expanded_url": {"type": "string"}, # Full URL
                                                "display_url": {"type": "string"},  # Shown in tweet
                                                "indices": {
                                                    "type": "array",
                                                    "items": {"type": "string"},    # [start, end]
                                                    "minItems": 2,
                                                    "maxItems": 2
                                                }
                                            }
                                        }
                                    },
                                    "media": {  # Optional media attachments
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "type": {"type": "string"},        # e.g., "photo"
                                                "url": {"type": "string"},         # t.co URL
                                                "media_url": {"type": "string"},   # Direct media URL
                                                "media_url_https": {"type": "string"},
                                                "expanded_url": {"type": "string"},
                                                "display_url": {"type": "string"},
                                                "indices": {
                                                    "type": "array",
                                                    "items": {"type": "string"},
                                                    "minItems": 2,
                                                    "maxItems": 2
                                                },
                                                "sizes": {
                                                    "type": "object",
                                                    "properties": {
                                                        "medium": {"type": "object"},
                                                        "small": {"type": "object"},
                                                        "thumb": {"type": "object"},
                                                        "large": {"type": "object"}
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "user_mentions": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "string"},
                                                "id_str": {"type": "string"},
                                                "indices": {
                                                    "type": "array",
                                                    "items": {"type": "string"},
                                                    "minItems": 2,
                                                    "maxItems": 2
                                                },
                                                "name": {"type": "string"},
                                                "screen_name": {"type": "string"}
                                            }
                                        }
                                    },
                                    "hashtags": {"type": "array"},
                                    "symbols": {"type": "array"}
                                }
                            },
                            "in_reply_to_status_id": {"type": ["string", "null"]},
                            "in_reply_to_user_id": {"type": ["string", "null"]},
                            "in_reply_to_screen_name": {"type": ["string", "null"]},
                            "favorite_count": {"type": "string"},  # Number as string
                            "retweet_count": {"type": "string"},   # Number as string
                            "favorited": {"type": "boolean"},
                            "retweeted": {"type": "boolean"},
                            "lang": {"type": "string"},            # e.g., "en"
                            "truncated": {"type": "boolean"}
                        }
                    }
                }
            }
        },

        # Community Tweets (same structure as regular tweets plus community IDs)
        "community-tweet": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "tweet": {
                        "type": "object",
                        "properties": {
                            # Same as regular tweet properties plus:
                            "community_id": {"type": "string"},
                            "community_id_str": {"type": "string"}
                        }
                    }
                }
            }
        },

        # Likes
        "like": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "like": {
                        "type": "object",
                        "properties": {
                            "tweetId": {"type": "string"},
                            "fullText": {"type": "string"},
                            "expandedUrl": {"type": "string"}  # Full URL to liked tweet
                        }
                    }
                }
            }
        },

        # Note Tweets (longer form content)
        "note-tweet": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "noteTweet": {
                        "type": "object",
                        "properties": {
                            "noteTweetId": {"type": "string"},
                            "core": {
                                "type": "object",
                                "properties": {
                                    "text": {"type": "string"},
                                    "mentions": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "screenName": {"type": "string"},
                                                "fromIndex": {"type": "string"},  # Character position
                                                "toIndex": {"type": "string"}     # Character position
                                            }
                                        }
                                    },
                                    "urls": {"type": "array"},
                                    "hashtags": {"type": "array"},
                                    "cashtags": {"type": "array"}
                                }
                            },
                            "createdAt": {"type": "string"},  # ISO format
                            "updatedAt": {"type": "string"}   # ISO format
                        }
                    }
                }
            }
        }
    }
}

# Important notes:
# 1. All IDs are strings, even though they contain numeric values
# 2. Timestamps come in two formats:
#    - Twitter format: "Fri Sep 27 16:17:03 +0000 2024"
#    - ISO format: "2008-10-21T12:01:00.000Z"
# 3. Numeric values (like counts) are often stored as strings
# 4. Reply information is optional and only present in reply tweets
# 5. Media attachments are optional
# 6. URLs in tweets are always shortened with t.co
# 7. Character positions (indices) are stored as strings 