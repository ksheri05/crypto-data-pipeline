#!/usr/bin/env python3
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent))

from config.config import Config
from src.ingestion.api_client import CoinGeckoAPIClient
from src.transformation.bronze_to_silver import BronzeToSilverTransformer
from src.transformation.silver_to_gold import SilverToGoldTransformer
from src.utils.s3_handler import S3Handler
from src.utils.db_handler import DatabaseHandler


def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)


def main():
    """Main pipeline execution"""
    logger = setup_logging()

    try:
        logger.info("=" * 80)
        logger.info("CRYPTO DATA PIPELINE - STARTING")
        logger.info("=" * 80)
        logger.info(f"Execution started at: {datetime.now()}")
        logger.info("")

        # ============================================================
        # STEP 1: FETCH DATA FROM API (Bronze Layer)
        # ============================================================
        logger.info("=" * 80)
        logger.info("STEP 1: FETCHING DATA FROM API (BRONZE LAYER)")
        logger.info("=" * 80)

        # Initialize API client
        api_client = CoinGeckoAPIClient(Config.COINGECKO_API_URL)

        # Fetch raw data
        bronze_data = api_client.fetch_crypto_data()
        logger.info(f"✓ Fetched {len(bronze_data)} cryptocurrency records")
        logger.info("")

        # ============================================================
        # STEP 2: UPLOAD BRONZE DATA TO S3
        # ============================================================
        logger.info("=" * 80)
        logger.info("STEP 2: UPLOADING BRONZE DATA TO S3")
        logger.info("=" * 80)

        # Initialize S3 handler
        s3_handler = S3Handler(
            bucket_name=Config.S3_BUCKET_NAME,
            region=Config.AWS_REGION
        )

        # Upload bronze data to S3
        bronze_s3_key = s3_handler.upload_json(
            data=bronze_data,
            prefix=Config.S3_BRONZE_PREFIX,
            filename=f"crypto_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        logger.info(f"✓ Bronze data saved to: s3://{Config.S3_BUCKET_NAME}/{bronze_s3_key}")
        logger.info("")

        # ============================================================
        # STEP 3: TRANSFORM TO SILVER LAYER (Clean Data)
        # ============================================================
        logger.info("=" * 80)
        logger.info("STEP 3: TRANSFORMING TO SILVER LAYER (CLEANING DATA)")
        logger.info("=" * 80)

        # Transform bronze to silver
        silver_transformer = BronzeToSilverTransformer()
        silver_df = silver_transformer.transform(bronze_data)
        logger.info(f"✓ Silver transformation completed: {len(silver_df)} records")

        # Upload silver data to S3
        silver_s3_key = s3_handler.upload_json(
            data=silver_df.to_dict(orient='records'),
            prefix=Config.S3_SILVER_PREFIX,
            filename=f"crypto_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        logger.info(f"✓ Silver data saved to: s3://{Config.S3_BUCKET_NAME}/{silver_s3_key}")
        logger.info("")

        # ============================================================
        # STEP 4: TRANSFORM TO GOLD LAYER (Aggregate Data)
        # ============================================================
        logger.info("=" * 80)
        logger.info("STEP 4: TRANSFORMING TO GOLD LAYER (AGGREGATING DATA)")
        logger.info("=" * 80)

        # Transform silver to gold
        gold_transformer = SilverToGoldTransformer()
        gold_df = gold_transformer.transform(silver_df)
        logger.info(f"✓ Gold transformation completed: {len(gold_df)} records")

        # Upload gold data to S3
        gold_s3_key = s3_handler.upload_json(
            data=gold_df.to_dict(orient='records'),
            prefix=Config.S3_GOLD_PREFIX,
            filename=f"crypto_aggregated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        logger.info(f"✓ Gold data saved to: s3://{Config.S3_BUCKET_NAME}/{gold_s3_key}")
        logger.info("")

        # ============================================================
        # STEP 5: LOAD GOLD DATA TO POSTGRESQL
        # ============================================================
        logger.info("=" * 80)
        logger.info("STEP 5: LOADING GOLD DATA TO POSTGRESQL")
        logger.info("=" * 80)

        # Initialize database handler
        db_handler = DatabaseHandler(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )

        # Connect to database
        db_handler.connect()
        logger.info("✓ Connected to PostgreSQL database")

        # Create table if not exists
        db_handler.execute_sql_file('sql/create_table.sql')
        logger.info("✓ Table created/verified")

        # Load gold data to database
        rows_inserted = db_handler.load_gold_data(gold_df, Config.TABLE_GOLD)
        logger.info(f"✓ Loaded {rows_inserted} records to PostgreSQL")

        # Close database connection
        db_handler.close()
        logger.info("")

        # ============================================================
        # PIPELINE COMPLETED SUCCESSFULLY
        # ============================================================
        logger.info("=" * 80)
        logger.info(" PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info("")
        logger.info(" DATA LAKE (S3):")
        logger.info(f"   Bronze: s3://{Config.S3_BUCKET_NAME}/{bronze_s3_key}")
        logger.info(f"   Silver: s3://{Config.S3_BUCKET_NAME}/{silver_s3_key}")
        logger.info(f"   Gold:   s3://{Config.S3_BUCKET_NAME}/{gold_s3_key}")
        logger.info("")
        logger.info("  DATA WAREHOUSE (PostgreSQL):")
        logger.info(f"   Database: {Config.POSTGRES_DB}")
        logger.info(f"   Table: {Config.TABLE_GOLD}")
        logger.info(f"   Records: {rows_inserted}")
        logger.info("")
        logger.info(f"✓ Execution completed at: {datetime.now()}")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ PIPELINE FAILED!")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())