
def test_imports():
    try:
        from config.config import Config
        from src.ingestion.api_client import CoinGeckoAPIClient
        from src.transformation.bronze_to_silver import BronzeToSilverTransformer
        from src.transformation.silver_to_gold import SilverToGoldTransformer
        from src.utils.s3_handler import S3Handler
        from src.utils.db_handler import DatabaseHandler
        print(" All imports successful!")
        assert True
    except ImportError as e:
        print(f" Import failed: {e}")
        assert False


def test_config():
    from config.config import Config

    # Check if config has required attributes
    assert hasattr(Config, 'S3_BUCKET_NAME')
    assert hasattr(Config, 'POSTGRES_DB')
    assert hasattr(Config, 'COINGECKO_API_URL')
    print(" Config test passed!")


if __name__ == "__main__":
    test_imports()
    test_config()
    print("\n All tests passed!")