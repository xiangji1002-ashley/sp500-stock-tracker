-- staging -> fact (idempotent: delete then insert)
-- Replace '2026-04-25' with your target date

DELETE FROM stock_data.stg.fact_daily_stock_price
WHERE trade_date = DATE('2026-04-25');

INSERT INTO stock_data.stg.fact_daily_stock_price
SELECT
    snapshot_date AS trade_date,
    ticker,
    open            AS open_price,
    high            AS high_price,
    low             AS low_price,
    close           AS close_price,
    CAST(volume AS BIGINT) AS volume
FROM stock_data.raw.staging_daily_stock_price
WHERE snapshot_date = DATE('2026-04-25');
