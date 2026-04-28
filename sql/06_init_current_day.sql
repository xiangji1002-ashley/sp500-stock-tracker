-- Initialize current_day_stock_price (run before market open)
-- Fills base rows from fact table for microbatch to update

DELETE FROM stock_data.marts.current_day_stock_price WHERE 1=1;

INSERT INTO stock_data.marts.current_day_stock_price
SELECT
    ticker,
    trade_date,
    close_price AS last_price,
    open_price,
    high_price,
    low_price,
    volume,
    CAST(NULL AS TIMESTAMP(6)) AS last_updated_ts
FROM (
    SELECT ticker, trade_date, open_price, high_price, low_price,
           close_price, volume,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
    FROM stock_data.stg.fact_daily_stock_price
)
WHERE rn = 1;
