import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from config.config import Config
from src.utils.db_handler import DatabaseHandler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_database():
    try:
        logger.info("Setting up PostgreSQL database...")

        # Create database handler
        db_handler = DatabaseHandler(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )

        # Connect to database
        logger.info(f"Connecting to database: {Config.POSTGRES_DB}")
        db_handler.connect()
        logger.info(" Connected successfully")

        # Execute SQL file to create tables
        logger.info("Creating tables...")
        sql_file_path = Path(__file__).parent.parent / 'sql' / 'create_table.sql'
        db_handler.execute_sql_file(str(sql_file_path))
        logger.info(" Tables created successfully")

        # Close connection
        db_handler.close()

        logger.info("")
        logger.info("=" * 60)
        logger.info(" DATABASE SETUP COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Database: {Config.POSTGRES_DB}")
        logger.info(f"Host: {Config.POSTGRES_HOST}")
        logger.info(f"Table: {Config.TABLE_GOLD}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f" Error setting up database: {e}")
        raise


if __name__ == "__main__":
    setup_database()