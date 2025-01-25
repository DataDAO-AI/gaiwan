"""Common utilities shared across modules."""

import re
import json
import logging

logger = logging.getLogger(__name__)

def clean_json_string(json_string: str) -> str:
    """Remove leading and trailing patterns."""
    # Clean the leading pattern:
    cleaned = re.sub(r'^window\.[^=]+=]+=\s*', '', json_string.strip())
    # Clean the trailing pattern:
    return cleaned.rstrip(';')

def load_json_file(file_path: str) -> any:
    """Load json data with allowances for various wrappings in data file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            raw_string = file.read()
            cleaned_string = clean_json_string(raw_string)
            data = json.loads(cleaned_string)
            return data
    except json.JSONDecodeError:
        # might be wrapped differently
        with open(file_path, 'r', encoding='utf=8') as f:
            raw_content = f.read()
        # extract from JS variable?
        match = re.search(r'window\.__THAR_CONFIG\s*=\s*{{.*}}', raw_content, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        else:
            logger.error("Failed to parse JSON from %s", file_path)
            return None
    except Exception as e:
        logger.warning("Error processing file '%s', %s", file_path, e)
        return None 