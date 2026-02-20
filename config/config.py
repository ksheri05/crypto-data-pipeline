import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:

    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

    # S3 Folder Paths (Medallion Architecture)
    S3_BRONZE_PREFIX = 'bronze/raw_data/'
    S3_SILVER_PREFIX = 'silver/cleaned_data/'
    S3_GOLD_PREFIX = 'gold/aggregated_data/'

    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'crypto_warehouse')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

    # API Configuration
    COINGECKO_API_URL = os.getenv('COINGECKO_API_URL')

    # Database Table Name
    TABLE_GOLD = 'gold_crypto_aggregated'