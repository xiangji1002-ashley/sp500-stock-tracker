-- Create tables in Starburst (run each separately)
-- NOTE: staging_daily_stock_price and dim_ticker_details are created by Glue Jobs (CTAS), NOT here

CREATE TABLE IF NOT EXISTS stock_data.stg.fact_daily_stock_price (
    trade_date DATE,
    ticker VARCHAR,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS stock_data.marts.cumulative_stock_price (
    ticker VARCHAR,
    price_history ARRAY(DOUBLE),
    last_updated DATE
);

CREATE TABLE IF NOT EXISTS stock_data.marts.dim_daily_stock_price (
    ticker VARCHAR,
    price_history ARRAY(DOUBLE),
    latest_price DOUBLE,
    historic_low DOUBLE,
    historic_high DOUBLE,
    last_updated DATE
);

CREATE TABLE IF NOT EXISTS stock_data.marts.current_day_stock_price (
    ticker          VARCHAR,
    trade_date      DATE,
    last_price      DOUBLE,
    open_price      DOUBLE,
    high_price      DOUBLE,
    low_price       DOUBLE,
    volume          BIGINT,
    last_updated_ts TIMESTAMP(6)
);
