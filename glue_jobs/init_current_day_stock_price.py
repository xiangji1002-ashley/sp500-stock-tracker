import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark = SparkSession.builder.getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sql("""
    INSERT OVERWRITE glue_catalog.stock_data.current_day_stock_price
    SELECT
        s.ticker,
        s.close_price_last_day,
        s.close_price_avg_last_90_days,
        s.close_price_avg_last_365_days,
        CAST(NULL AS DOUBLE) AS current_price,
        CAST(NULL AS DOUBLE) AS m_price_change_last_day,
        CAST(NULL AS DOUBLE) AS m_price_change_last_day_pct,
        CAST(NULL AS DOUBLE) AS m_price_change_last_90_days,
        CAST(NULL AS DOUBLE) AS m_price_change_last_90_days_pct,
        CAST(NULL AS DOUBLE) AS m_price_change_last_365_days,
        CAST(NULL AS DOUBLE) AS m_price_change_last_365_days_pct,
        COALESCE(td.market_cap_description, 'Not Provided') AS market_cap_description,
        CAST(NULL AS TIMESTAMP) AS last_updated_datetime
    FROM glue_catalog.stock_data.dim_daily_stock_price s
    LEFT JOIN glue_catalog.stock_data.dim_ticker_details td ON s.ticker = td.ticker
""")

job.commit()