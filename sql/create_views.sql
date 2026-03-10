CREATE OR REPLACE VIEW vw_asset_performance AS
WITH latest_prices AS (
    SELECT DISTINCT ON (asset_symbol)
        asset_symbol,
        price_usd AS current_price,
        timestamp_utc AS last_updated,
        source
    FROM raw_assets
    ORDER BY asset_symbol, timestamp_utc DESC
),
stats AS (
    SELECT 
        asset_symbol,
        AVG(price_usd) AS avg_price_history,
        COUNT(*) AS total_readings
    FROM raw_assets
    GROUP BY asset_symbol
)
SELECT 
    l.asset_symbol,
    l.current_price,
    s.avg_price_history,
    ROUND(((l.current_price - s.avg_price_history) / s.avg_price_history * 100), 2) AS pct_diff_from_avg,
    l.last_updated,
    l.source,
    s.total_readings
FROM latest_prices l
JOIN stats s ON l.asset_symbol = s.asset_symbol;