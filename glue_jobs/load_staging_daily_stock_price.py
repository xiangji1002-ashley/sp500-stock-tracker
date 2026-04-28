"""
Glue ETL Job: 读取 S3 当天 CSV → 写入 Iceberg raw.staging_daily_stock_price 表（增量）
支持两种模式：
  - 有 --ds 参数：读 daily/{ds}.csv（每日增量）
  - 无 --ds 参数：读 sp500_history.csv（历史全量，首次加载用）
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, LongType

# 解析参数
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 可选参数 --ds
ds = None
for i, arg in enumerate(sys.argv):
    if arg == '--ds' and i + 1 < len(sys.argv):
        ds = sys.argv[i + 1]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── 0. 配置 Iceberg Catalog ───────────────────────────────────────────────
S3_BUCKET = "YOUR_S3_BUCKET_NAME"
WAREHOUSE = f"s3://{S3_BUCKET}/iceberg/"

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# ── 1. 读取 S3 CSV ─────────────────────────────────────────────────────────
if ds:
    csv_path = f"s3://{S3_BUCKET}/raw/sp500_history/daily/{ds}.csv"
    print(f"Incremental mode: reading {csv_path}")
else:
    csv_path = f"s3://{S3_BUCKET}/raw/sp500_history/sp500_history.csv"
    print(f"Full load mode: reading {csv_path}")

df = spark.read.option("header", "true").csv(csv_path)

# ── 2. 类型转换 ────────────────────────────────────────────────────────────
df = df.select(
    col("ticker").cast("string"),
    to_date(col("snapshot_date"), "yyyy-MM-dd").alias("snapshot_date"),
    col("open").cast(DoubleType()),
    col("high").cast(DoubleType()),
    col("low").cast(DoubleType()),
    col("close").cast(DoubleType()),
    col("volume").cast(LongType())
)

# ── 3. 写入 Iceberg 表 ───────────────────────────────────────────────────
df.createOrReplaceTempView("staging_temp")

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.raw")

if ds:
    # 增量模式：INSERT INTO（追加当天数据）
    spark.sql("""
        INSERT INTO glue_catalog.raw.staging_daily_stock_price
        SELECT * FROM staging_temp
    """)
    print(f"Inserted {df.count()} rows for {ds}")
else:
    # 全量模式：CREATE OR REPLACE（首次加载）
    spark.sql(f"""
        CREATE OR REPLACE TABLE glue_catalog.raw.staging_daily_stock_price
        USING iceberg
        LOCATION '{WAREHOUSE}raw/staging_daily_stock_price'
        AS SELECT * FROM staging_temp
    """)
    print(f"Full load complete: {df.count()} rows")

job.commit()
