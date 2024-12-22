"""Generate JSON Schema from Twitter archive analysis."""

import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Set, List
from gaiwan.analyze_archive import SchemaValidator

logger = logging.getLogger(__name__)

# Define reusable schema components
MEDIA_SIZE = {
    "type": "object",
    "properties": {
        "h": {"type": "string"},
        "w": {"type": "string"},
        "resize": {"type": "string"}
    },
    "required": ["h", "w", "resize"]
}

URL_ENTITY = {
    "type": "object",
    "properties": {
        "displayUrl": {"type": "string"},
        "expandedUrl": {"type": "string"},
        "shortUrl": {"type": "string"},
        "url": {"type": "string"},
        "indices": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["url", "indices"]
}

MEDIA_ENTITY = {
    "type": "object",
    "properties": {
        "display_url": {"type": "string"},
        "expanded_url": {"type": "string"},
        "id": {"type": "string"},
        "id_str": {"type": "string"},
        "indices": {
            "type": "array",
            "items": {"type": "string"}
        },
        "media_url": {"type": "string"},
        "media_url_https": {"type": "string"},
        "sizes": {
            "type": "object",
            "properties": {
                "large": MEDIA_SIZE,
                "medium": MEDIA_SIZE,
                "small": MEDIA_SIZE,
                "thumb": MEDIA_SIZE
            },
            "required": ["large", "medium", "small", "thumb"]
        },
        "type": {"type": "string"},
        "url": {"type": "string"}
    },
    "required": ["id", "id_str", "media_url", "type", "url", "indices"]
}

TWEET_MEDIA_ENTITY = {
    "type": "object",
    "properties": {
        **MEDIA_ENTITY["properties"],  # Include base media properties
        "source_status_id": {"type": "string"},
        "source_status_id_str": {"type": "string"},
        "source_user_id": {"type": "string"},
        "source_user_id_str": {"type": "string"},
        "video_info": {
            "type": "object",
            "properties": {
                "aspect_ratio": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "variants": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "bitrate": {"type": "string"},
                            "content_type": {"type": "string"},
                            "url": {"type": "string"}
                        },
                        "required": ["content_type", "url"]
                    }
                }
            }
        }
    }
}

TWEET_REQUIRED_FIELDS = {
    "id", "id_str", "created_at", "full_text"
}

# Add more required field sets
REQUIRED_FIELDS = {
    "tweets.tweet": {"id", "id_str", "created_at", "full_text"},
    "community-tweet.tweet": {"id", "id_str", "created_at", "full_text"},
    "profile.profile.description": {"bio"},
    "note-tweet.noteTweet": {"noteTweetId", "core"},
    "note-tweet.noteTweet.core": {"text"},
    "entities.user_mentions": {"id", "id_str", "name", "screen_name"},
    "entities.media": {"id", "id_str", "media_url", "type"}
}

# Add item types for empty arrays
ARRAY_ITEM_TYPES = {
    "hashtags": {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "indices": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["text", "indices"]
    },
    "symbols": {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "indices": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["text", "indices"]
    },
    "cashtags": {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "indices": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["text", "indices"]
    },
    "styletags": {
        "type": "object",
        "properties": {
            "fromIndex": {"type": "string"},
            "toIndex": {"type": "string"},
            "styleTypes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "value": {"type": "string"},
                        "originalName": {"type": "string"},
                        "annotations": {"type": "object"}
                    },
                    "required": ["name", "value"]
                }
            }
        },
        "required": ["fromIndex", "toIndex", "styleTypes"]
    }
}

def generate_schema_for_array(field_path: str, field_types: Dict[str, Set[str]]) -> Dict[str, Any]:
    """Generate schema for array fields based on their path and sample value."""
    # Arrays that should contain strings
    string_arrays = {
        "indices", "display_text_range", "editTweetIds", "aspect_ratio"
    }
    
    # Get the last part of the path
    array_type = field_path.split('.')[-1]
    
    if any(part in field_path for part in string_arrays):
        return {
            "type": "array",
            "items": {"type": "string"}
        }
    elif "media" in field_path:
        return {
            "type": "array",
            "items": TWEET_MEDIA_ENTITY if "tweet" in field_path else MEDIA_ENTITY
        }
    elif "urls" in field_path:
        return {
            "type": "array",
            "items": URL_ENTITY
        }
    elif array_type in ARRAY_ITEM_TYPES:
        return {
            "type": "array",
            "items": ARRAY_ITEM_TYPES[array_type]
        }
    elif "user_mentions" in field_path:
        return {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "id_str": {"type": "string"},
                    "indices": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "name": {"type": "string"},
                    "screen_name": {"type": "string"}
                },
                "required": ["id", "id_str", "name", "screen_name"]
            }
        }
    elif any(part in field_path for part in ["tweets", "like", "note-tweet", "community-tweet"]):
        return {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {}
            }
        }
    
    # Default to object array with empty properties
    return {
        "type": "array",
        "items": {"type": "object"}
    }

def should_be_boolean_or_string(field_path: str) -> bool:
    """Determine if a field should accept both boolean and string values."""
    boolean_string_fields = {
        "favorited", "retweeted", "truncated", "isEditEligible",
        "keepPrivate", "uploadLikes"
    }
    return any(field in field_path for field in boolean_string_fields)

def build_object_properties(field_path: str, field_types: Dict[str, Set[str]]) -> Dict[str, Any]:
    """Build properties for an object based on its known fields."""
    properties = {}
    prefix = field_path + "."
    
    # Find all fields that belong to this object
    for path, types in field_types.items():
        if path.startswith(prefix):
            relative_path = path[len(prefix):]
            if "." not in relative_path:  # Direct property
                if 'bool' in types:
                    if should_be_boolean_or_string(path):
                        properties[relative_path] = {"type": ["boolean", "string"]}
                    else:
                        properties[relative_path] = {"type": "boolean"}
                elif 'list' in types:
                    properties[relative_path] = generate_schema_for_array(path, field_types)
                elif 'dict' in types:
                    properties[relative_path] = {
                        "type": "object",
                        "properties": build_object_properties(path, field_types)
                    }
                else:
                    properties[relative_path] = {"type": "string"}
    
    return properties

def generate_schema(field_types: Dict[str, Set[str]]) -> Dict[str, Any]:
    """Generate schema from field types."""
    properties = {}
    
    for field in sorted({path.split('.')[0] for path in field_types.keys()}):
        field_type = field_types.get(field, set())
        
        if field == "upload-options":
            properties[field] = {
                "type": "object",
                "properties": {
                    "keepPrivate": {
                        "type": ["boolean", "string"],
                        "description": "Whether to keep the archive private"
                    },
                    "uploadLikes": {
                        "type": ["boolean", "string"],
                        "description": "Whether to include liked tweets"
                    },
                    "startDate": {
                        "type": "string",
                        "format": "date-time",
                        "description": "Start date for archive range"
                    },
                    "endDate": {
                        "type": "string",
                        "format": "date-time",
                        "description": "End date for archive range"
                    }
                }
            }
            continue
        
        if field in ["tweets", "community-tweet"]:
            tweet_props = build_tweet_properties(field_types, field)
            properties[field] = {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "tweet": {
                            "type": "object",
                            "properties": tweet_props,
                            "required": ["id", "id_str", "created_at", "full_text", "entities"]
                        }
                    },
                    "required": ["tweet"]
                }
            }
            continue
        
        if field == "account":
            properties[field] = {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "account": build_account_properties()
                    }
                }
            }
            continue
        
        # Handle other fields normally...
        schema = {
            "type": "array" if 'list' in field_type else "object",
            "items" if 'list' in field_type else "properties": {
                "type": "object",
                "properties": build_object_properties(field, field_types)
            }
        }
        properties[field] = schema
    
    return properties

def build_entities_object() -> Dict[str, Any]:
    """Build the standard entities object structure."""
    return {
        "type": "object",
        "properties": {
            "hashtags": {
                "type": "array",
                "items": ARRAY_ITEM_TYPES["hashtags"]
            },
            "symbols": {
                "type": "array",
                "items": ARRAY_ITEM_TYPES["symbols"]
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
                            "items": {"type": "string"}
                        },
                        "name": {"type": "string"},
                        "screen_name": {"type": "string"}
                    },
                    "required": ["id", "id_str", "name", "screen_name"]
                }
            },
            "urls": {
                "type": "array",
                "items": URL_ENTITY
            },
            "media": {
                "type": "array",
                "items": TWEET_MEDIA_ENTITY
            }
        }
    }

def clean_properties(schema: Dict[str, Any], path: str = "") -> Dict[str, Any]:
    """Clean up schema properties and add required fields."""
    if not isinstance(schema, dict):
        return schema
    
    for key, value in list(schema.items()):
        if isinstance(value, dict):
            current_path = f"{path}.{key}" if path else key
            
            # Add required fields and format hints
            if key == "tweet":
                value["required"] = ["id", "id_str", "created_at", "full_text", "entities"]
            elif key == "account":
                value["required"] = ["accountId", "username"]
            elif key == "description" and "profile" in path:
                value["required"] = ["bio"]
            elif key == "noteTweet":
                value["required"] = ["noteTweetId", "core", "createdAt"]
            elif key == "core" and "noteTweet" in path:
                value["required"] = ["text"]
            elif key in ["created_at", "createdAt", "updatedAt", "startDate", "endDate"]:
                value["format"] = "date-time"
            
            # Add entities object where needed
            if key == "entities" and "tweet" in path:
                schema[key] = build_entities_object()
                continue
            
            # Clean up properties
            schema[key] = clean_properties(value, current_path)
            
            # Remove null required fields
            if "required" in value and value["required"] is None:
                del value["required"]
    
    return schema

def generate_schema_header() -> str:
    """Generate a header comment for the schema file."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Twitter Archive Schema",
        "description": "Schema for Twitter archive JSON files",
        "type": "object",
        "required": ["profile", "tweets"],
        "$comment": (
            "This schema is auto-generated. To regenerate:\n"
            "python -m gaiwan.schema_generator path/to/archive --output schema.json\n"
            f"Generated on: {datetime.now().isoformat()}"
        )
    }

def analyze_schema_differences(base_schema: Dict[str, Any], archive_file: Path, validator: SchemaValidator) -> Dict[str, Any]:
    """Analyze differences between base schema and a new archive."""
    logger.info(f"Analyzing {archive_file.name}...")
    
    # Analyze new file
    validator.validate_file(archive_file)
    field_types = validator.field_types
    
    # Generate schema for this file
    new_properties = generate_schema(field_types)
    new_schema = clean_properties({"properties": new_properties})["properties"]
    
    differences = {
        "new_fields": set(),
        "different_types": {},
        "new_required": set(),
    }
    
    def compare_schemas(base: Dict[str, Any], new: Dict[str, Any], path: str = ""):
        """Compare two schema structures recursively."""
        all_keys = set(base.keys()) | set(new.keys())
        
        for key in all_keys:
            current_path = f"{path}.{key}" if path else key
            
            # Check for new fields
            if key not in base:
                differences["new_fields"].add(current_path)
                continue
            
            if key not in new:
                continue
            
            # Compare types
            base_type = base[key].get("type")
            new_type = new[key].get("type")
            if base_type != new_type:
                differences["different_types"][current_path] = (base_type, new_type)
            
            # Compare required fields
            base_required = set(base[key].get("required", []))
            new_required = set(new[key].get("required", []))
            if new_required - base_required:
                differences["new_required"].add((current_path, tuple(new_required - base_required)))
            
            # Recurse into nested structures
            if "properties" in base[key] and "properties" in new[key]:
                compare_schemas(base[key]["properties"], new[key]["properties"], current_path)
            if "items" in base[key] and "items" in new[key]:
                if "properties" in base[key]["items"] and "properties" in new[key]["items"]:
                    compare_schemas(
                        base[key]["items"]["properties"],
                        new[key]["items"]["properties"],
                        current_path
                    )
    
    compare_schemas(base_schema["properties"], new_schema)
    return differences

def analyze_field_values(archive_file: Path, field_path: str) -> Set[Any]:
    """Analyze actual values of a specific field across an archive."""
    with open(archive_file) as f:
        data = json.load(f)
    
    values = set()
    parts = field_path.split('.')
    
    def extract_values(obj: Dict[str, Any], path: List[str]):
        if not path:
            values.add(obj)
            return
        if isinstance(obj, list):
            for item in obj:
                extract_values(item, path)
        elif isinstance(obj, dict):
            if path[0] in obj:
                extract_values(obj[path[0]], path[1:])
    
    extract_values(data, parts)
    return values

def build_tweet_properties(field_types: Dict[str, Set[str]], path: str) -> Dict[str, Any]:
    """Build common tweet properties that appear in both tweets and community-tweets."""
    props = {
        "id": {
            "type": "string",
            "description": "Numeric tweet ID (as string to preserve precision)"
        },
        "id_str": {
            "type": "string",
            "description": "String representation of tweet ID (preferred over id)"
        },
        "created_at": {
            "type": "string",
            "format": "date-time",
            "description": "UTC timestamp when tweet was created"
        },
        "full_text": {
            "type": "string",
            "description": "Complete tweet text content"
        },
        "entities": {
            **build_entities_object(),
            "description": "Structured entities like mentions, hashtags, and URLs"
        },
        "extended_entities": {
            **build_entities_object(),
            "description": "Extended media information, present when tweet contains media"
        },
        "possibly_sensitive": {
            "type": "boolean",
            "description": "Content warning flag for sensitive media"
        },
        "favorited": {
            "type": ["boolean", "string"],
            "description": "Whether the tweet is favorited by the user"
        },
        "retweeted": {
            "type": ["boolean", "string"],
            "description": "Whether the tweet is retweeted by the user"
        },
        "retweet_count": {
            "type": "string",
            "description": "Number of retweets (stored as string)"
        },
        "favorite_count": {
            "type": "string",
            "description": "Number of favorites (stored as string)"
        },
        "truncated": {
            "type": ["boolean", "string"],
            "description": "Whether the tweet text was truncated"
        },
        "source": {
            "type": "string",
            "description": "Client application used to post the tweet"
        },
        "lang": {
            "type": "string",
            "description": "Language code of tweet content"
        },
        "in_reply_to_status_id": {
            "type": "string",
            "description": "ID of tweet being replied to (if any)"
        },
        "in_reply_to_status_id_str": {
            "type": "string",
            "description": "String ID of tweet being replied to (if any)"
        },
        "in_reply_to_user_id": {
            "type": "string",
            "description": "ID of user being replied to (if any)"
        },
        "in_reply_to_user_id_str": {
            "type": "string",
            "description": "String ID of user being replied to (if any)"
        },
        "in_reply_to_screen_name": {
            "type": "string",
            "description": "Username of user being replied to (if any)"
        },
        "display_text_range": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Start and end indices of displayable tweet text"
        }
    }
    
    # Add community-specific fields if needed
    if "community" in path:
        props.update({
            "community_id": {
                "type": "string",
                "description": "Numeric ID of the community (as string)"
            },
            "community_id_str": {
                "type": "string",
                "description": "String ID of the community"
            }
        })
    
    return props

def build_account_properties() -> Dict[str, Any]:
    """Build account properties including optional fields."""
    return {
        "type": "object",
        "properties": {
            "accountId": {"type": "string"},
            "username": {"type": "string"},
            "accountDisplayName": {"type": "string"},
            "createdAt": {"type": "string", "format": "date-time"},
            "createdVia": {"type": "string"},
            "email": {
                "type": "string",
                "description": "User email, present in some account exports"
            }
        },
        "required": ["accountId", "username"]
    }

def main():
    parser = argparse.ArgumentParser(
        description="Generate JSON Schema from Twitter archive analysis",
        epilog="Example: %(prog)s ../archives --output schema.json"
    )
    parser.add_argument('archive_dir', type=Path, help="Directory containing archive files")
    parser.add_argument('--output', type=Path, default=Path('generated_schema.json'),
                       help="Output path for generated schema")
    args = parser.parse_args()

    # Key archives to analyze
    key_archives = [
        "moonboi__archive.json",      # Has extended_entities and possibly_sensitive
        "eigenrobot_archive.json",    # Has account.email
        "alightcone_archive.json",    # Has extended_entities
        "plus3happiness_archive.json", # Has note-tweet timestamps (to verify)
        "satori_jojo_archive.json"    # Has both tweet and community-tweet fields
    ]

    # Create validator and analyze files
    validator = SchemaValidator()
    
    # First analyze base archive
    base_file = args.archive_dir / "visakanv_archive.json"
    if not base_file.exists():
        logger.error(f"Base archive not found at {base_file}")
        return

    logger.info("Analyzing archives...")
    
    # Analyze base archive
    validator.validate_file(base_file)
    
    # Analyze field values
    logger.info("\nAnalyzing possibly_sensitive values:")
    for archive in key_archives:
        archive_path = args.archive_dir / archive
        if archive_path.exists():
            values = analyze_field_values(archive_path, "tweets.tweet.possibly_sensitive")
            if values:
                logger.info(f"{archive}: {values}")
            
            values = analyze_field_values(archive_path, "community-tweet.tweet.possibly_sensitive")
            if values:
                logger.info(f"{archive} (community): {values}")
    
    # Generate schema
    field_types = validator.field_types
    properties = generate_schema(field_types)
    properties = clean_properties({"properties": properties})["properties"]
    
    schema = {
        **generate_schema_header(),
        "properties": properties
    }
    
    # Write schema to file
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nSchema generated and saved to {args.output}")

if __name__ == "__main__":
    main()