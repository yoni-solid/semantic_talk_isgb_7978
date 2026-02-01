"""
Tests for database module
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from database import SnowflakeManager
import pandas as pd


class TestSnowflakeManager:
    def test_initialization(self):
        with patch.dict('os.environ', {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USER': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
            'SNOWFLAKE_ROLE': 'test_role'
        }):
            manager = SnowflakeManager()
            assert manager.account == 'test_account'
            assert manager.user == 'test_user'
            assert manager.schema == 'SUPPLY_CHAIN'

    @patch('snowflake.connector.connect')
    def test_connect(self, mock_connect):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch.dict('os.environ', {
            'SNOWFLAKE_ACCOUNT': 'test',
            'SNOWFLAKE_USER': 'test',
            'SNOWFLAKE_PASSWORD': 'test',
            'SNOWFLAKE_WAREHOUSE': 'test',
            'SNOWFLAKE_ROLE': 'test'
        }):
            manager = SnowflakeManager()
            manager.connect()
            assert manager.conn is not None
            assert manager.cursor is not None

    @patch('snowflake.connector.connect')
    def test_create_schema(self, mock_connect):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch.dict('os.environ', {
            'SNOWFLAKE_ACCOUNT': 'test',
            'SNOWFLAKE_USER': 'test',
            'SNOWFLAKE_PASSWORD': 'test',
            'SNOWFLAKE_WAREHOUSE': 'test',
            'SNOWFLAKE_ROLE': 'test'
        }):
            manager = SnowflakeManager()
            manager.conn = mock_conn
            manager.cursor = mock_cursor
            manager.create_schema()
            assert mock_cursor.execute.call_count >= 2

    def test_load_dataframe_empty(self):
        with patch.dict('os.environ', {
            'SNOWFLAKE_ACCOUNT': 'test',
            'SNOWFLAKE_USER': 'test',
            'SNOWFLAKE_PASSWORD': 'test',
            'SNOWFLAKE_WAREHOUSE': 'test',
            'SNOWFLAKE_ROLE': 'test'
        }):
            manager = SnowflakeManager()
            manager.conn = Mock()
            manager.cursor = Mock()
            
            empty_df = pd.DataFrame()
            # Should not raise error, just log warning
            manager.load_dataframe(empty_df, "TEST_TABLE")
