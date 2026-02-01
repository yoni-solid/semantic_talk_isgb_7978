"""
Live Snowflake database tests (requires actual Snowflake credentials in .env)
These tests will actually connect to Snowflake and perform operations.
"""
import pytest
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from database import SnowflakeManager

load_dotenv()

# Skip all tests if Snowflake credentials are not available
SNOWFLAKE_AVAILABLE = all([
    os.getenv('SNOWFLAKE_ACCOUNT'),
    os.getenv('SNOWFLAKE_USER'),
    os.getenv('SNOWFLAKE_PASSWORD'),
    os.getenv('SNOWFLAKE_WAREHOUSE'),
    os.getenv('SNOWFLAKE_ROLE')
])

pytestmark = pytest.mark.skipif(
    not SNOWFLAKE_AVAILABLE,
    reason="Snowflake credentials not available in .env file"
)


class TestSnowflakeLive:
    """Live Snowflake connection and operation tests"""
    
    @pytest.fixture(scope="class")
    def db_manager(self):
        """Create a database manager instance"""
        manager = SnowflakeManager()
        yield manager
        # Cleanup
        if manager.conn:
            manager.disconnect()
    
    def test_live_connection(self, db_manager):
        """Test actual connection to Snowflake"""
        db_manager.connect()
        assert db_manager.conn is not None
        assert db_manager.cursor is not None
    
    def test_create_schema(self, db_manager):
        """Test creating the SUPPLY_CHAIN schema"""
        db_manager.connect()
        db_manager.create_schema()
        
        # Verify schema exists
        db_manager.cursor.execute("SHOW SCHEMAS LIKE 'SUPPLY_CHAIN'")
        result = db_manager.cursor.fetchone()
        assert result is not None
    
    def test_create_dimension_tables(self, db_manager):
        """Test creating dimension tables"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_dimension_tables()
        
        # Verify tables exist
        tables = ["CAT_REF", "BK_CAT_REF", "AUTH_REF", "DIR_REF", "PERF_REF", "AWARD_REF"]
        for table in tables:
            db_manager.cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA SUPPLY_CHAIN")
            result = db_manager.cursor.fetchone()
            assert result is not None, f"Table {table} was not created"
    
    def test_create_fact_tables(self, db_manager):
        """Test creating fact tables"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_fact_tables()
        
        # Verify tables exist
        tables = ["INV_MAST", "BK_CATALOG", "MEDIA_MAST"]
        for table in tables:
            db_manager.cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA SUPPLY_CHAIN")
            result = db_manager.cursor.fetchone()
            assert result is not None, f"Table {table} was not created"
    
    def test_create_bridge_tables(self, db_manager):
        """Test creating bridge tables"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_bridge_tables()
        
        # Verify tables exist
        tables = ["BK_CAT_XREF", "MEDIA_PERF_XREF", "MEDIA_AWD_XREF"]
        for table in tables:
            db_manager.cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA SUPPLY_CHAIN")
            result = db_manager.cursor.fetchone()
            assert result is not None, f"Table {table} was not created"
    
    def test_create_detail_tables(self, db_manager):
        """Test creating detail tables"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_detail_tables()
        
        # Verify tables exist
        tables = ["INV_VAR", "INV_REV", "INV_SIM"]
        for table in tables:
            db_manager.cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA SUPPLY_CHAIN")
            result = db_manager.cursor.fetchone()
            assert result is not None, f"Table {table} was not created"
    
    def test_load_dimension_data(self, db_manager):
        """Test loading data into dimension tables"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_all_tables()
        
        # Create test data
        test_cat_ref = pd.DataFrame({
            'CAT_CD': ['CAT_0001', 'CAT_0002'],
            'CAT_NM': ['apparel', 'consumables']
        })
        
        db_manager.load_dataframe(test_cat_ref, "CAT_REF")
        
        # Verify data was loaded
        db_manager.cursor.execute("SELECT COUNT(*) FROM SUPPLY_CHAIN.CAT_REF")
        count = db_manager.cursor.fetchone()[0]
        assert count >= 2
    
    def test_load_fact_data(self, db_manager):
        """Test loading data into fact tables"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_all_tables()
        
        # First load a category (required for FK reference)
        test_cat_ref = pd.DataFrame({
            'CAT_CD': ['CAT_TEST'],
            'CAT_NM': ['test_category']
        })
        db_manager.load_dataframe(test_cat_ref, "CAT_REF")
        
        # Create test product data
        test_inv_mast = pd.DataFrame({
            'INV_ID': ['INV_TEST_001'],
            'INV_NM': ['Test Product'],
            'UNIT_PRC': [10.99],
            'CAT_CD': ['CAT_TEST'],
            'DESC_TXT': ['Test description'],
            'LNK_ID': ['LINK_TEST'],
            'SCRP_DT': [datetime.now()]
        })
        
        db_manager.load_dataframe(test_inv_mast, "INV_MAST")
        
        # Verify data was loaded
        db_manager.cursor.execute("SELECT COUNT(*) FROM SUPPLY_CHAIN.INV_MAST")
        count = db_manager.cursor.fetchone()[0]
        assert count >= 1
    
    def test_create_analytical_views(self, db_manager):
        """Test creating analytical views"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_all_tables()
        db_manager.create_analytical_views()
        
        # Verify views exist
        views = ["VW_INV_ANALYTICS", "VW_BK_ANALYTICS", "VW_MEDIA_ANALYTICS"]
        for view in views:
            db_manager.cursor.execute(f"SHOW VIEWS LIKE '{view}' IN SCHEMA SUPPLY_CHAIN")
            result = db_manager.cursor.fetchone()
            assert result is not None, f"View {view} was not created"
    
    def test_verify_data(self, db_manager):
        """Test data verification"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_all_tables()
        
        # Load some test data
        test_cat_ref = pd.DataFrame({
            'CAT_CD': ['CAT_VERIFY'],
            'CAT_NM': ['verify_category']
        })
        db_manager.load_dataframe(test_cat_ref, "CAT_REF")
        
        # Run verification
        results = db_manager.verify_data()
        
        assert isinstance(results, dict)
        assert "CAT_REF" in results
        assert results["CAT_REF"] >= 1
    
    def test_full_pipeline(self, db_manager):
        """Test full pipeline: create tables, load data, create views"""
        db_manager.connect()
        db_manager.create_schema()
        db_manager.create_all_tables()
        
        # Create comprehensive test data
        test_dataframes = {
            "CAT_REF": pd.DataFrame({
                'CAT_CD': ['CAT_FULL_001', 'CAT_FULL_002'],
                'CAT_NM': ['Full Test Category 1', 'Full Test Category 2']
            }),
            "AUTH_REF": pd.DataFrame({
                'AUTH_CD': ['AUTH_FULL_001'],
                'AUTH_NM': ['Full Test Author']
            }),
            "INV_MAST": pd.DataFrame({
                'INV_ID': ['INV_FULL_001'],
                'INV_NM': ['Full Test Product'],
                'UNIT_PRC': [25.99],
                'CAT_CD': ['CAT_FULL_001'],
                'DESC_TXT': ['Full pipeline test product'],
                'LNK_ID': ['LINK_FULL_001'],
                'SCRP_DT': [datetime.now()]
            }),
            "BK_CATALOG": pd.DataFrame({
                'BK_ID': ['BK_FULL_001'],
                'BK_TTL': ['Full Test Book'],
                'UNIT_PRC': [15.50],
                'AUTH_CD': ['AUTH_FULL_001'],
                'AVAIL_STS': ['In stock'],
                'RTG_VAL': [4],
                'DESC_TXT': ['Full pipeline test book'],
                'LNK_ID': ['LINK_FULL_002'],
                'SCRP_DT': [datetime.now()]
            })
        }
        
        # Load all data
        db_manager.load_all_data(test_dataframes)
        
        # Create views
        db_manager.create_analytical_views()
        
        # Verify everything
        verification = db_manager.verify_data()
        assert verification["CAT_REF"] >= 2
        assert verification["AUTH_REF"] >= 1
        assert verification["INV_MAST"] >= 1
        assert verification["BK_CATALOG"] >= 1
        
        # Test that views can be queried
        db_manager.cursor.execute("SELECT COUNT(*) FROM SUPPLY_CHAIN.VW_INV_ANALYTICS")
        view_count = db_manager.cursor.fetchone()[0]
        assert view_count >= 0  # View should exist and be queryable
