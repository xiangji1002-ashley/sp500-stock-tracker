-- Data Quality Checks

-- 1. Null checks on fact table
SELECT
    COUNT(CASE WHEN ticker IS NULL THEN 1 END) = 0       AS ticker_ok,
    COUNT(CASE WHEN close_price IS NULL THEN 1 END) = 0  AS close_ok,
    COUNT(CASE WHEN trade_date IS NULL THEN 1 END) = 0   AS date_ok
FROM stock_data.stg.fact_daily_stock_price;

-- 2. Row counts across all tables
SELECT 'raw.staging' AS tbl, COUNT(*) AS cnt FROM stock_data.raw.staging_daily_stock_price
UNION ALL SELECT 'stg.fact', COUNT(*) FROM stock_data.stg.fact_daily_stock_price
UNION ALL SELECT 'marts.cumulative', COUNT(*) FROM stock_data.marts.cumulative_stock_price
UNION ALL SELECT 'marts.dim_daily', COUNT(*) FROM stock_data.marts.dim_daily_stock_price
UNION ALL SELECT 'marts.dim_ticker', COUNT(*) FROM stock_data.marts.dim_ticker_details
UNION ALL SELECT 'marts.current_day', COUNT(*) FROM stock_data.marts.current_day_stock_price;
