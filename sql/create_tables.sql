CREATE TABLE IF NOT EXISTS raw_assets (
    id SERIAL PRIMARY KEY,
    asset_symbol VARCHAR(20),
    price_usd NUMERIC(20, 8),
    timestamp_utc TIMESTAMP,
    source VARCHAR(50),
    etl_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_asset_time 
ON raw_assets (asset_symbol, timestamp_utc);