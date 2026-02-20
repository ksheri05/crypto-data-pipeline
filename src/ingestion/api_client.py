import requests
import logging

logger = logging.getLogger(__name__)


class CoinGeckoAPIClient:

    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_crypto_data(self):
        try:
            logger.info("Fetching data from CoinGecko API...")

            response = requests.get(self.api_url, timeout=30)
            response.raise_for_status()  # Raise error if request fails

            data = response.json()

            logger.info(f"Successfully fetched {len(data)} cryptocurrency records")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}")
            raise