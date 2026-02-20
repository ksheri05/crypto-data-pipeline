import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class SilverToGoldTransformer:

    def transform(self, silver_df):
        try:
            logger.info("Starting silver to gold transformation...")

            # Group by cryptocurrency and aggregate
            gold_df = silver_df.groupby(['id', 'name', 'symbol']).agg({
                'current_price': 'mean',
                'market_cap': 'mean',
                'total_volume': 'mean',
                'market_cap_rank': 'min',
                'price_change_percentage_24h': 'mean'
            }).reset_index()

            # Rename columns
            gold_df.columns = [
                'crypto_id', 'crypto_name', 'crypto_symbol',
                'avg_price', 'avg_market_cap', 'avg_volume',
                'best_rank', 'avg_price_change_pct'
            ]

            # Add metadata
            gold_df['date_aggregated'] = datetime.now().date()

            # Round numeric columns
            gold_df['avg_price'] = gold_df['avg_price'].round(2)
            gold_df['avg_price_change_pct'] = gold_df['avg_price_change_pct'].round(2)

            logger.info(f"Gold transformation completed: {len(gold_df)} records")
            return gold_df

        except Exception as e:
            logger.error(f"Error in silver to gold transformation: {e}")
            raise