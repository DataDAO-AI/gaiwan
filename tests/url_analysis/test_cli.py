import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import Mock, patch
import logging
from datetime import datetime, timezone

from gaiwan.twitter_archive_processor.url_analysis.cli import (
    URLAnalysisReporter, setup_logging, process_archives,
    load_existing_data, save_results
)
from .test_utils import async_mock_coro

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'username': ['user1', 'user2'],
        'url': ['https://example.com', 'https://test.com'],
        'domain': ['example.com', 'test.com'],
        'raw_domain': ['example.com', 't.co'],
        'protocol': ['https', 'https'],
        'fetch_status': ['success', 'failed'],
        'fetch_error': [None, 'timeout'],
        'page_title': ['Example', None],
        'is_resolved': [True, False],
        'tweet_created_at': [
            datetime.now(timezone.utc),
            datetime.now(timezone.utc)
        ]
    })

@pytest.fixture
def mock_analyzer():
    analyzer = Mock()
    analyzer.domain_normalizer.shortener_domains = {'t.co', 'bit.ly'}
    analyzer.archive_dir = Mock()
    analyzer.archive_dir.glob.return_value = [
        Path("user1_archive.json"),
        Path("user2_archive.json")
    ]
    return analyzer

def test_reporter_overall_stats(sample_df, mock_analyzer, capsys):
    reporter = URLAnalysisReporter(sample_df, mock_analyzer)
    reporter.print_overall_stats()
    
    captured = capsys.readouterr()
    assert "Total URLs found: 2" in captured.out
    assert "Unique URLs: 2" in captured.out

def test_reporter_fetch_stats(sample_df, mock_analyzer, capsys):
    reporter = URLAnalysisReporter(sample_df, mock_analyzer)
    reporter.print_fetch_stats()
    
    captured = capsys.readouterr()
    assert "success    1" in captured.out
    assert "failed     1" in captured.out

def test_reporter_domain_analysis(sample_df, mock_analyzer, capsys):
    reporter = URLAnalysisReporter(sample_df, mock_analyzer)
    reporter.print_domain_analysis()
    
    captured = capsys.readouterr()
    assert "example.com    1" in captured.out
    assert "t.co" in captured.out

def test_setup_logging():
    # Reset logger to default state
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.WARNING)

    # Test debug mode
    setup_logging(True)
    assert logging.getLogger().level == logging.DEBUG
    
    # Reset and test normal mode
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.WARNING)
    setup_logging(False)
    assert logging.getLogger().level == logging.INFO

@pytest.mark.asyncio
async def test_process_archives(tmp_path, mock_analyzer):
    output_file = tmp_path / "urls.parquet"
    
    # Create an async mock for _analyze_archives_async
    async def mock_analyze():
        return pd.DataFrame({
            'username': ['test_user'],
            'url': ['https://example.com'],
            'tweet_created_at': [datetime.now(timezone.utc)]
        })
    
    mock_analyzer._analyze_archives_async = mock_analyze
    df = await process_archives(mock_analyzer, output_file, force=False)
    assert not df.empty
    assert 'username' in df.columns

def test_load_existing_data(tmp_path):
    output_file = tmp_path / "urls.parquet"
    
    # Test with non-existent file
    assert load_existing_data(output_file) is None
    
    # Test with valid file
    df = pd.DataFrame({
        'test': [1, 2, 3],
        'tweet_created_at': [
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc)
        ]
    })
    df.to_parquet(output_file)
    loaded_df = load_existing_data(output_file)
    assert not loaded_df.empty
    assert all(loaded_df['test'] == [1, 2, 3])

def test_save_results(tmp_path):
    output_file = tmp_path / "urls.parquet"
    df = pd.DataFrame({
        'test': [1, 2, 3],
        'tweet_created_at': [
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc)
        ]
    })
    
    # Test initial save
    save_results(df, output_file)
    assert output_file.exists()
    
    # Test backup creation
    df_new = pd.DataFrame({
        'test': [4, 5, 6],
        'tweet_created_at': [
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc)
        ]
    })
    save_results(df_new, output_file)
    
    # Check that backup was created
    backup_files = list(tmp_path.glob("urls_*.parquet"))
    assert len(backup_files) == 1 