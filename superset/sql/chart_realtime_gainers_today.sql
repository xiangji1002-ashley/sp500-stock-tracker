-- Real-time Dashboard chart 2: Gainers Today (Big Number).
-- In Superset: + Chart -> Dataset: current_day_stock_price -> Big Number.
-- Paste into METRIC -> CUSTOM SQL (SIMPLE doesn't support CASE).
COUNT(CASE WHEN last_price > open_price THEN 1 END)
