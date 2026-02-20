-- Create table for Gold layer (curated data)

CREATE TABLE IF NOT EXISTS gold_crypto_aggregated (
    crypto_id VARCHAR(50),
    crypto_name VARCHAR(100),
    crypto_symbol VARCHAR(20),
    avg_price DECIMAL(20, 2),
    avg_market_cap BIGINT,
    avg_volume BIGINT,
    best_rank INT,
    avg_price_change_pct DECIMAL(10, 2),
    date_aggregated DATE,
    PRIMARY KEY (crypto_id, date_aggregated)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_date_aggregated
ON gold_crypto_aggregated(date_aggregated);

CREATE INDEX IF NOT EXISTS idx_best_rank
ON gold_crypto_aggregated(best_rank);