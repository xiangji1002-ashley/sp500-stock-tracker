"""
Glue ETL Job: Microbatch update current_day_stock_price with Alpaca real-time prices
"""
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp, round, when
from pyspark.sql.types import StringType, StructType, StructField, DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "runtime_minutes", "apca_api_key_id", "apca_api_secret_key"])

# ── 0. 配置 Iceberg Catalog ─────────────────────────────────────────────
S3_BUCKET = "YOUR_S3_BUCKET_NAME"
WAREHOUSE = f"s3://{S3_BUCKET}/iceberg/"

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

apca_api_key_id = args['apca_api_key_id']
apca_api_secret_key = args['apca_api_secret_key']
runtime_minutes = int(args['runtime_minutes'])

headers = {
    'APCA-API-KEY-ID': apca_api_key_id,
    'APCA-API-SECRET-KEY': apca_api_secret_key,
    'accept': 'application/json'
}

# ── 1. 获取标普500 ticker 列表 ─────────────────────────────────────────────
tickers_df = spark.sql("""
    SELECT ticker FROM glue_catalog.marts.current_day_stock_price
""")
tickers = [row.ticker for row in tickers_df.collect()]

# ── 2. 读取历史基准数据 ───────────────────────────────────────────────────
current_data_df = spark.sql("""
    SELECT ticker,
        close_price_last_day,
        close_price_avg_last_90_days,
        close_price_avg_last_365_days
    FROM glue_catalog.marts.current_day_stock_price
""")

# ── 3. 微批量循环 ─────────────────────────────────────────────────────────
end_time = datetime.now() + timedelta(minutes=runtime_minutes)

while datetime.now() < end_time:
    # 每批 500 只
    for i in range(0, len(tickers), 500):
        batch = tickers[i:i+500]
        symbol_string = '%2C'.join(batch)
        url = f'https://data.alpaca.markets/v2/stocks/bars/latest?symbols={symbol_string}&feed=iex'

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"API error: {response.text}")
            continue

        data = response.json()
        if "bars" not in data or not data["bars"]:
            continue

        # 构建 DataFrame
        rows = []
        for ticker, bar in data["bars"].items():
            c = float(bar["c"]) if isinstance(bar["c"], int) else bar["c"]
            rows.append(Row(
                ticker=ticker,
                current_price=c,
                last_updated_datetime=bar["t"]
            ))

        if not rows:
            continue

        price_df = spark.createDataFrame(rows)
        price_df = price_df.withColumn(
            "last_updated_datetime",
            to_timestamp(col("last_updated_datetime"), "yyyy-MM-dd'T'HH:mm:ssX")
        )

        # JOIN 计算 6 个指标
        update_df = price_df.join(current_data_df, "ticker") \
            .withColumn("m_price_change_last_day",
                round(col("current_price") - col("close_price_last_day"), 2)) \
            .withColumn("m_price_change_last_day_pct",
                when((col("close_price_last_day").isNull()) | (col("close_price_last_day") == 0), None)
                .otherwise(round(100 * (col("current_price") - col("close_price_last_day")) / col("close_price_last_day"), 2))) \
            .withColumn("m_price_change_last_90_days",
                round(col("current_price") - col("close_price_avg_last_90_days"), 2)) \
            .withColumn("m_price_change_last_90_days_pct",
                when((col("close_price_avg_last_90_days").isNull()) | (col("close_price_avg_last_90_days") == 0), None)
                .otherwise(round(100 * (col("current_price") - col("close_price_avg_last_90_days")) / col("close_price_avg_last_90_days"), 2))) \
            .withColumn("m_price_change_last_365_days",
                round(col("current_price") - col("close_price_avg_last_365_days"), 2)) \
            .withColumn("m_price_change_last_365_days_pct",
                when((col("close_price_avg_last_365_days").isNull()) | (col("close_price_avg_last_365_days") == 0), None)
                .otherwise(round(100 * (col("current_price") - col("close_price_avg_last_365_days")) / col("close_price_avg_last_365_days"), 2))) \
            .select(
                "ticker", "current_price", "last_updated_datetime",
                "m_price_change_last_day", "m_price_change_last_day_pct",
                "m_price_change_last_90_days", "m_price_change_last_90_days_pct",
                "m_price_change_last_365_days", "m_price_change_last_365_days_pct"
            )

        update_df.createOrReplaceTempView("stock_microbatch_update")

        spark.sql("""
            MERGE INTO glue_catalog.marts.current_day_stock_price AS target
            USING stock_microbatch_update AS source
                ON source.ticker = target.ticker
            WHEN MATCHED THEN
                UPDATE SET
                    current_price = source.current_price,
                    last_updated_datetime = source.last_updated_datetime,
                    m_price_change_last_day = source.m_price_change_last_day,
                    m_price_change_last_day_pct = source.m_price_change_last_day_pct,
                    m_price_change_last_90_days = source.m_price_change_last_90_days,
                    m_price_change_last_90_days_pct = source.m_price_change_last_90_days_pct,
                    m_price_change_last_365_days = source.m_price_change_last_365_days,
                    m_price_change_last_365_days_pct = source.m_price_change_last_365_days_pct
        """)

    print(f"批次完成: {datetime.now()}")

job.commit()