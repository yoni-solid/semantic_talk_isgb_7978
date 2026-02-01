"""
Script to create Snowflake tasks for all successful business queries.
Each task will run hourly indefinitely.
"""
import logging
import sys
from database import SnowflakeManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/task_creation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main function to create all tasks"""
    try:
        logger.info("=" * 60)
        logger.info("SNOWFLAKE TASK CREATION - START")
        logger.info("=" * 60)
        
        # Connect to Snowflake
        logger.info("\n[STEP 1] CONNECTING TO SNOWFLAKE")
        logger.info("-" * 60)
        db_manager = SnowflakeManager()
        db_manager.connect()
        
        # Create all tasks
        logger.info("\n[STEP 2] CREATING TASKS FROM BUSINESS QUERIES")
        logger.info("-" * 60)
        results = db_manager.create_all_tasks("business_queries.json")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("TASK CREATION COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total tasks created: {results['total_created']}")
        logger.info(f"Total tasks failed: {results['total_failed']}")
        
        if results['failed']:
            logger.warning("\nFailed tasks:")
            for failed in results['failed']:
                logger.warning(f"  Query ID {failed['id']}: {failed['reason']}")
        
        # List all tasks to verify
        logger.info("\n[STEP 3] VERIFYING TASKS")
        logger.info("-" * 60)
        all_tasks = db_manager.list_tasks()
        logger.info(f"Total tasks in schema: {len(all_tasks)}")
        
        # Disconnect
        db_manager.disconnect()
        logger.info("\nDisconnected from Snowflake")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise


if __name__ == "__main__":
    main()
