-- Virtual Dataset for the Real-time Monitor dashboard.
-- In Superset: SQL Lab -> paste this -> RUN -> SAVE -> "Save dataset"
-- Dataset name: current_day_enriched
-- Used by: Top 10 Gainers, Top 10 Losers, Market Heatmap
SELECT
    ticker,
    open_price,
    last_price,
    volume,
    ROUND((last_price - open_price) / open_price * 100, 2) AS pct_change,
    last_price * volume                                    AS dollar_volume
FROM stock_data.marts.current_day_stock_price
WHERE open_price > 0
  AND volume     > 0;
