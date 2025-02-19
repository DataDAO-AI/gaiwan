import pytest
from pathlib import Path
import json
from ...export.oai import OpenAIExporter

def test_oai_export_thread(sample_thread, tmp_path):
    exporter = OpenAIExporter(system_message="Test system message")
    output_path = tmp_path / "test_thread.jsonl"
    
    exporter.export_thread(sample_thread, output_path)
    
    with open(output_path) as f:
        data = json.loads(f.readline())
    
    messages = data["messages"]
    assert len(messages) == 3  # system + 2 tweets
    assert messages[0]["role"] == "system"
    assert all(msg["role"] == "user" for msg in messages[1:]) 