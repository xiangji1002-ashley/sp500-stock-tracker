-- fact -> cumulative (full rebuild)

DELETE FROM stock_data.marts.cumulative_stock_price WHERE 1=1;

INSERT INTO stock_data.marts.cumulative_stock_price
SELECT
    ticker,
    ARRAY_AGG(close_price ORDER BY trade_date) AS price_history,
    MAX(trade_date) AS last_updated
FROM stock_data.stg.fact_daily_stock_price
GROUP BY ticker;
