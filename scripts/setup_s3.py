import boto3
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from config.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_s3_bucket():
    try:
        logger.info("Setting up AWS S3...")

        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            region_name=Config.AWS_REGION,
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
        )

        bucket_name = Config.S3_BUCKET_NAME

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"✓ Bucket '{bucket_name}' already exists")
        except:
            # Create bucket
            logger.info(f"Creating bucket: {bucket_name}")
            if Config.AWS_REGION == 'us-east-1':
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': Config.AWS_REGION}
                )
            logger.info(f" Successfully created bucket: {bucket_name}")

        # Create folder structure (Bronze, Silver, Gold)
        folders = [
            Config.S3_BRONZE_PREFIX,
            Config.S3_SILVER_PREFIX,
            Config.S3_GOLD_PREFIX
        ]

        logger.info("Creating folder structure...")
        for folder in folders:
            if not folder.endswith('/'):
                folder += '/'

            s3_client.put_object(Bucket=bucket_name, Key=folder, Body=b'')
            logger.info(f" Created folder: s3://{bucket_name}/{folder}")

        logger.info("")
        logger.info("=" * 60)
        logger.info(" S3 SETUP COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Region: {Config.AWS_REGION}")
        logger.info("Folder structure:")
        logger.info(f"  - {Config.S3_BRONZE_PREFIX}")
        logger.info(f"  - {Config.S3_SILVER_PREFIX}")
        logger.info(f"  - {Config.S3_GOLD_PREFIX}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f" Error setting up S3: {e}")
        raise


if __name__ == "__main__":
    setup_s3_bucket()