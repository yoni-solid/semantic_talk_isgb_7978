"""
Main orchestration script for web scraping to Snowflake pipeline.
"""
import asyncio
import logging
import sys
from pathlib import Path
from scraper import WebScraper
from database import SnowflakeManager

# Configure logging
def setup_logging():
    """Setup logging to both file and console"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / "pipeline.log"
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

logger = logging.getLogger(__name__)


async def main():
    """Main orchestration function"""
    try:
        logger.info("=" * 60)
        logger.info("SUPPLY CHAIN DATA PIPELINE - START")
        logger.info("=" * 60)
        
        # Step 1: Scrape data
        logger.info("\n[STEP 1] SCRAPING DATA")
        logger.info("-" * 60)
        scraper = WebScraper()
        await scraper.run_all()
        
        # Step 2: Normalize and process data
        logger.info("\n[STEP 2] PROCESSING AND NORMALIZING DATA")
        logger.info("-" * 60)
        dataframes = scraper.to_dataframes()
        logger.info(f"Created {len(dataframes)} DataFrames")
        
        # Step 3: Export to CSV
        logger.info("\n[STEP 3] EXPORTING TO CSV")
        logger.info("-" * 60)
        csv_paths = scraper.export_to_csv()
        logger.info(f"Exported {len(csv_paths)} CSV files")
        
        # Step 4: Connect to Snowflake
        logger.info("\n[STEP 4] CONNECTING TO SNOWFLAKE")
        logger.info("-" * 60)
        db_manager = SnowflakeManager()
        db_manager.connect()
        
        # Step 5: Create schema and tables
        logger.info("\n[STEP 5] CREATING SCHEMA AND TABLES")
        logger.info("-" * 60)
        db_manager.create_schema()
        db_manager.create_all_tables()
        
        # Step 6: Load data
        logger.info("\n[STEP 6] LOADING DATA INTO SNOWFLAKE")
        logger.info("-" * 60)
        db_manager.load_all_data(dataframes)
        
        # Step 7: Create analytical views
        logger.info("\n[STEP 7] CREATING ANALYTICAL VIEWS")
        logger.info("-" * 60)
        db_manager.create_analytical_views()
        
        # Step 8: Verify data
        logger.info("\n[STEP 8] VERIFYING DATA")
        logger.info("-" * 60)
        verification = db_manager.verify_data()
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info("Summary:")
        for table, count in verification.items():
            logger.info(f"  {table}: {count} rows")
        
        db_manager.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        return False


if __name__ == "__main__":
    setup_logging()
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
