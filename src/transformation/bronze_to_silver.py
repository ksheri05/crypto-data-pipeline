import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BronzeToSilverTransformer:

    def transform(self, bronze_data):
        try:
            logger.info("Starting bronze to silver transformation...")

            # Convert to DataFrame
            df = pd.DataFrame(bronze_data)

            # Select only the columns we need
            columns_to_keep = [
                'id', 'symbol', 'name', 'current_price',
                'market_cap', 'market_cap_rank', 'total_volume',
                'price_change_percentage_24h', 'last_updated'
            ]

            df_silver = df[columns_to_keep].copy()

            # Clean data
            df_silver['last_updated'] = pd.to_datetime(df_silver['last_updated'])
            df_silver['ingestion_timestamp'] = datetime.now()

            # Handle missing values
            df_silver['price_change_percentage_24h'] = df_silver['price_change_percentage_24h'].fillna(0)

            # Remove any rows with null prices or market cap
            df_silver = df_silver[df_silver['current_price'] > 0]
            df_silver = df_silver[df_silver['market_cap'] > 0]

            logger.info(f"Silver transformation completed: {len(df_silver)} records")
            return df_silver

        except Exception as e:
            logger.error(f"Error in bronze to silver transformation: {e}")
            raise