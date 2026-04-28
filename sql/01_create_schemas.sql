-- Create schemas (Medallion Architecture)
-- Run each statement separately in Starburst Galaxy Query Editor

CREATE SCHEMA IF NOT EXISTS stock_data.raw;

CREATE SCHEMA IF NOT EXISTS stock_data.stg;

CREATE SCHEMA IF NOT EXISTS stock_data.marts;
