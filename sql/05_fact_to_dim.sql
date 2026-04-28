-- fact -> dim_daily_stock_price (full rebuild)

DELETE FROM stock_data.marts.dim_daily_stock_price WHERE 1=1;

INSERT INTO stock_data.marts.dim_daily_stock_price
SELECT
    ticker,
    ARRAY_AGG(close_price ORDER BY trade_date) AS price_history,
    ELEMENT_AT(ARRAY_AGG(close_price ORDER BY trade_date DESC), 1) AS latest_price,
    MIN(close_price) AS historic_low,
    MAX(close_price) AS historic_high,
    MAX(trade_date) AS last_updated
FROM stock_data.stg.fact_daily_stock_price
GROUP BY ticker;
