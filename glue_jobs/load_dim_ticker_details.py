"""
Glue ETL Job: 读取 S3 CSV → 写入 Iceberg dim_ticker_details 表
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

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
csv_path = f"s3://{S3_BUCKET}/raw/sp500_ticker_details/sp500_ticker_details.csv"
df = spark.read.option("header", "true").csv(csv_path)

# ── 2. 类型转换 ────────────────────────────────────────────────────────────
df = df.select(
    col("ticker").cast("string"),
    col("company_name").cast("string"),
    col("sector").cast("string"),
    col("industry").cast("string"),
    col("market_cap").cast(DoubleType()),
    col("market_cap_description").cast("string"),
    col("exchange").cast("string"),
    col("country").cast("string")
)

# ── 3. 用 Spark SQL 创建 Iceberg 表并写入 ─────────────────────────────────
df.createOrReplaceTempView("ticker_temp")

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.marts")

spark.sql(f"""
    CREATE OR REPLACE TABLE glue_catalog.marts.dim_ticker_details
    USING iceberg
    LOCATION '{WAREHOUSE}marts/dim_ticker_details'
    AS SELECT * FROM ticker_temp
""")

job.commit()
