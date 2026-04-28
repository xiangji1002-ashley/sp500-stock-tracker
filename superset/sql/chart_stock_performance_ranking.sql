-- Historical Dashboard chart 5: Stock Performance Ranking.
-- In Superset: SQL Lab -> paste this -> RUN -> "Create chart" -> Table.
-- Sort by total_return_pct DESC, Row Limit 50, enable color +/- (green/red).
SELECT
    d.ticker,
    d.company_name,
    d.sector,
    ROUND(latest.close_price, 2)                                                         AS latest_close,
    ROUND((latest.close_price - earliest.close_price) / earliest.close_price * 100, 2)
        AS total_return_pct
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
) earliest ON d.ticker = earliest.ticker;
