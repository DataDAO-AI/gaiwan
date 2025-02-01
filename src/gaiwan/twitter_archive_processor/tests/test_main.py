import pytest
from pathlib import Path
from unittest.mock import patch
from ..processor import ArchiveProcessor
from ..__main__ import main

@pytest.fixture
def mock_processor(mocker):
    mock = mocker.patch('gaiwan.twitter_archive_processor.__main__.ArchiveProcessor')
    mock_instance = mock.return_value
    mock_instance.create_output_dirs.return_value = {
        'markdown': Path('output/markdown'),
        'oai': Path('output/oai'),
        'chatml': Path('output/chatml')
    }
    return mock_instance

def test_main_execution_markdown(mock_processor, tmp_path):
    archive_dir = tmp_path / "archives"
    output_dir = tmp_path / "output"
    archive_dir.mkdir()
    output_dir.mkdir()

    with patch('sys.argv', ['script', str(archive_dir), str(output_dir), '--format', 'markdown']):
        main()
        mock_processor.load_archives.assert_called_once()
        mock_processor.export_all.assert_called_once_with('markdown', output_dir, None)

def test_main_execution_oai(mock_processor, tmp_path):
    archive_dir = tmp_path / "archives"
    output_dir = tmp_path / "output"
    archive_dir.mkdir()
    output_dir.mkdir()

    with patch('sys.argv', ['script', str(archive_dir), str(output_dir), '--format', 'oai']):
        main()
        mock_processor.load_archives.assert_called_once()
        mock_processor.export_conversations_oai.assert_called_once()

def test_main_with_debug(mock_processor, tmp_path):
    archive_dir = tmp_path / "archives"
    output_dir = tmp_path / "output"
    archive_dir.mkdir()
    output_dir.mkdir()

    with patch('sys.argv', ['script', str(archive_dir), str(output_dir), '--debug']):
        main()
        mock_processor.load_archives.assert_called_once()
        mock_processor.export_all.assert_called_once()

def test_main_execution_all_formats(mock_processor, tmp_path):
    archive_dir = tmp_path / "archives"
    output_dir = tmp_path / "output"
    archive_dir.mkdir()
    output_dir.mkdir()
    system_message = "Test system message"

    with patch('sys.argv', [
        'script', 
        str(archive_dir), 
        str(output_dir), 
        '--format', 'markdown', 'oai', 'chatml',
        '--system-message', system_message
    ]):
        main()
        mock_processor.load_archives.assert_called_once()
        assert mock_processor.export_all.call_count == 2  # markdown and chatml
        assert mock_processor.export_conversations_oai.call_count == 1  # oai 