"""
Integration tests for the full pipeline
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, AsyncMock
from scraper import WebScraper
from database import SnowflakeManager


@pytest.mark.asyncio
async def test_scraper_to_dataframes_flow():
    """Test that scraper can create dataframes"""
    scraper = WebScraper()
    
    # Add some test data
    from models import Product, ProductCategory
    
    scraper.product_categories["test"] = ProductCategory(
        cat_cd="CAT_0001",
        cat_nm="test"
    )
    
    scraper.products.append(Product(
        inv_id="INV_001",
        inv_nm="Test Product",
        unit_prc=10.0,
        cat_cd="CAT_0001",
        desc_txt="Test"
    ))
    
    dataframes = scraper.to_dataframes()
    assert "CAT_REF" in dataframes
    assert "INV_MAST" in dataframes
    assert len(dataframes["CAT_REF"]) == 1
    assert len(dataframes["INV_MAST"]) == 1


def test_database_table_creation_order():
    """Test that tables are created in correct order"""
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
        
        # Track execution order
        execution_order = []
        original_execute = manager.cursor.execute
        
        def track_execute(sql):
            execution_order.append(sql)
            return original_execute(sql)
        
        manager.cursor.execute = track_execute
        
        manager.create_all_tables()
        
        # Verify dimension tables created before fact tables
        dim_tables = [sql for sql in execution_order if "CAT_REF" in sql or "AUTH_REF" in sql]
        fact_tables = [sql for sql in execution_order if "INV_MAST" in sql or "BK_CATALOG" in sql]
        
        assert len(dim_tables) > 0
        assert len(fact_tables) > 0
        # Dimension tables should come before fact tables
        assert execution_order.index(dim_tables[0]) < execution_order.index(fact_tables[0])
