-- Real-time Dashboard chart 3: Today's Dollar Volume (Big Number).
-- In Superset: + Chart -> Dataset: current_day_stock_price -> Big Number.
-- Paste into METRIC -> CUSTOM SQL.
-- CUSTOMIZE -> NUMBER FORMAT: $.3s  (do NOT put $.3s in CURRENCY FORMAT — it errors).
SUM(last_price * volume)
