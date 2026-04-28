-- Historical Dashboard chart 2: Sector Performance Comparison.
-- In Superset: SQL Lab -> paste this -> RUN -> "Create chart" -> Bar Chart.
-- X Axis: sector, Metric: MAX(avg_return_pct).
SELECT
    d.sector,
    ROUND(AVG((latest.close_price - earliest.close_price) / earliest.close_price * 100), 2)
        AS avg_return_pct
FROM stock_data.marts.dim_ticker_details d
JOIN (
    SELECT ticker, close_price
    FROM stock_data.stg.fact_daily_stock_price
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_data.stg.fact_daily_stock_price)
) latest   ON d.ticker = latest.ticker
JOIN (
    SELECT ticker, close_price
    FROM stock_data.stg.fact_daily_stock_price
    WHERE trade_date = (SELECT MIN(trade_date) FROM stock_data.stg.fact_daily_stock_price)
) earliest ON d.ticker = earliest.ticker
GROUP BY d.sector
ORDER BY avg_return_pct DESC;
