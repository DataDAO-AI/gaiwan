from pathlib import Path
import json
import logging
from typing import List
from ..conversation import ConversationThread

logger = logging.getLogger(__name__)

class OpenAIExporter:
    """Exports conversations in OpenAI format."""
    
    def export_conversations(
        self,
        threads: List[ConversationThread],
        output_path: Path,
        system_message: str
    ) -> None:
        """Export conversation threads as OpenAI JSONL format."""
        try:
            with open(output_path, 'w') as f:
                for thread in threads:
                    conversation = {
                        'messages': [
                            {'role': 'system', 'content': system_message},
                            *[{'role': 'user', 'content': tweet.clean_text()} 
                              for tweet in thread.all_tweets]
                        ]
                    }
                    f.write(json.dumps(conversation) + '\n')
            
            logger.info(f"Exported {len(threads)} conversations to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export conversations: {e}")
            raise 