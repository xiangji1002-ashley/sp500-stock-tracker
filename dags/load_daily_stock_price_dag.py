"""
离线 DAG:每天 8 AM UTC 运行
1. yfinance 下载前一交易日 S&P 500 数据 → 上传 S3
2. Glue Job:S3 CSV → raw.staging_daily_stock_price
3. Starburst SQL:raw.staging → stg.fact_daily_stock_price
4. Starburst SQL:stg.fact → marts.cumulative_stock_price(ARRAY_AGG)
5. Starburst SQL:stg.fact → marts.dim_daily_stock_price(聚合指标)
6. DQ 检查
"""
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime
import requests


# ── 配置 ──────────────────────────────────────────────────────────────────
STARBURST_HOST = "YOUR_STARBURST_HOST"
STARBURST_PORT = 443
STARBURST_USER = "YOUR_STARBURST_USER"
STARBURST_PASSWORD = "YOUR_STARBURST_PASSWORD"
CATALOG = "stock_data"

S3_BUCKET = "YOUR_S3_BUCKET_NAME"


def run_starburst_sql(query):
    """通过 Trino Python client 执行 Starburst SQL"""
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication
    conn = connect(
        host=STARBURST_HOST,
        port=STARBURST_PORT,
        user=STARBURST_USER,
        catalog=CATALOG,
        schema="stg",
        http_scheme="https",
        auth=BasicAuthentication(STARBURST_USER, STARBURST_PASSWORD),
    )
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


def download_daily_data(**context):
    """用 yfinance 下载前一交易日的 S&P 500 数据，上传到 S3"""
    import yfinance as yf
    import pandas as pd
    import boto3
    from io import StringIO

    ds = context["ds"]

    # 获取 S&P 500 成分股列表（加 User-Agent 避免 Wikipedia 403）
    resp = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        headers={"User-Agent": "Mozilla/5.0 (compatible; StockDE/1.0)"},
        timeout=30,
    )
    sp500_table = pd.read_html(resp.text)[0]
    tickers = sp500_table["Symbol"].str.replace(".", "-", regex=False).tolist()

    # 下载当天数据
    df = yf.download(tickers, start=ds, end=pd.Timestamp(ds) + pd.Timedelta(days=1), group_by="ticker")

    if df.empty:
        print(f"No data for {ds} (non-trading day), skipping")
        return

    # 处理 MultiIndex 列
    records = []
    for ticker in tickers:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                ticker_data = df[ticker].dropna()
            else:
                ticker_data = df.dropna()

            if ticker_data.empty:
                continue

            for date, row in ticker_data.iterrows():
                records.append({
                    "ticker": ticker.replace("-", "."),
                    "snapshot_date": date.strftime("%Y-%m-%d"),
                    "open": round(row["Open"], 4),
                    "high": round(row["High"], 4),
                    "low": round(row["Low"], 4),
                    "close": round(row["Close"], 4),
                    "volume": int(row["Volume"]),
                })
        except (KeyError, TypeError):
            continue

    if not records:
        print(f"No valid records for {ds}")
        return

    result_df = pd.DataFrame(records)

    # 上传到 S3（指定 us-east-2 区域）
    csv_buffer = StringIO()
    result_df.to_csv(csv_buffer, index=False)

    s3 = boto3.client("s3", region_name="us-east-2")
    s3_key = f"raw/sp500_history/daily/{ds}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Uploaded {len(records)} records to s3://{S3_BUCKET}/{s3_key}")


def load_staging_to_fact(**context):
    """raw.staging → stg.fact

    注意:DAG 在 UTC 08:00 跑,此时 ds 当天美股未开盘,
    yfinance 返回的是前一交易日(ds-1)的数据,所以这里按 ds-1 过滤。
    """
    ds = context["ds"]

    run_starburst_sql(f"""
        DELETE FROM {CATALOG}.stg.fact_daily_stock_price
        WHERE trade_date = DATE('{ds}') - INTERVAL '1' DAY
    """)

    run_starburst_sql(f"""
        INSERT INTO {CATALOG}.stg.fact_daily_stock_price
        SELECT
            snapshot_date   AS trade_date,
            ticker,
            open            AS open_price,
            high            AS high_price,
            low             AS low_price,
            close           AS close_price,
            CAST(volume AS BIGINT) AS volume
        FROM {CATALOG}.raw.staging_daily_stock_price
        WHERE snapshot_date = DATE('{ds}') - INTERVAL '1' DAY
    """)
    print(f"fact_daily_stock_price loaded for trade_date = {ds} - 1 day")


def load_cumulative(**context):
    """stg.fact → marts.cumulative(ARRAY_AGG)"""
    run_starburst_sql(f"""
        DELETE FROM {CATALOG}.marts.cumulative_stock_price WHERE 1=1
    """)

    run_starburst_sql(f"""
        INSERT INTO {CATALOG}.marts.cumulative_stock_price
        SELECT
            ticker,
            ARRAY_AGG(close_price ORDER BY trade_date) AS price_history,
            MAX(trade_date) AS last_updated
        FROM {CATALOG}.stg.fact_daily_stock_price
        GROUP BY ticker
    """)
    print("cumulative_stock_price rebuilt")


def load_dim(**context):
    """stg.fact → marts.dim_daily_stock_price"""
    run_starburst_sql(f"""
        DELETE FROM {CATALOG}.marts.dim_daily_stock_price WHERE 1=1
    """)

    run_starburst_sql(f"""
        INSERT INTO {CATALOG}.marts.dim_daily_stock_price
        SELECT
            ticker,
            ARRAY_AGG(close_price ORDER BY trade_date) AS price_history,
            ELEMENT_AT(ARRAY_AGG(close_price ORDER BY trade_date DESC), 1) AS latest_price,
            MIN(close_price) AS historic_low,
            MAX(close_price) AS historic_high,
            MAX(trade_date) AS last_updated
        FROM {CATALOG}.stg.fact_daily_stock_price
        GROUP BY ticker
    """)
    print("dim_daily_stock_price rebuilt")


def run_dq_checks(**context):
    """数据质量检查"""
    result = run_starburst_sql(f"""
        SELECT
            COUNT(CASE WHEN ticker IS NULL THEN 1 END) = 0   AS ticker_not_null,
            COUNT(CASE WHEN close_price IS NULL THEN 1 END) = 0 AS close_not_null,
            COUNT(CASE WHEN trade_date IS NULL THEN 1 END) = 0 AS date_not_null
        FROM {CATALOG}.stg.fact_daily_stock_price
    """)
    print(f"DQ Check - null checks: {result}")

    count = run_starburst_sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.stg.fact_daily_stock_price
    """)
    print(f"Total rows in fact table: {count}")

    dim_count = run_starburst_sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.marts.dim_daily_stock_price
    """)
    ticker_count = run_starburst_sql(f"""
        SELECT COUNT(DISTINCT ticker) FROM {CATALOG}.stg.fact_daily_stock_price
    """)
    print(f"dim rows: {dim_count}, distinct tickers: {ticker_count}")


@dag(
    "load_daily_stock_price_dag",
    description="Daily: download → staging → fact → cumulative → dim → DQ checks",
    default_args={
        "owner": "stock-project",
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
    },
    schedule="0 8 * * *",
    catchup=False,
    tags=["offline", "daily", "stock"],
)
def load_daily_stock_price_dag():

    download_data = PythonOperator(
        task_id="download_daily_data",
        python_callable=download_daily_data,
    )

    load_staging = GlueJobOperator(
        task_id="load_staging",
        job_name="load_staging_daily_stock_price",
        aws_conn_id="aws_default",
        region_name="us-east-2",
        wait_for_completion=True,
        script_args={"--ds": "{{ ds }}"},
    )

    staging_to_fact = PythonOperator(
        task_id="load_staging_to_fact",
        python_callable=load_staging_to_fact,
    )

    fact_to_cumulative = PythonOperator(
        task_id="load_cumulative",
        python_callable=load_cumulative,
    )

    fact_to_dim = PythonOperator(
        task_id="load_dim",
        python_callable=load_dim,
    )

    dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_dq_checks,
    )

    download_data >> load_staging >> staging_to_fact >> fact_to_cumulative >> fact_to_dim >> dq_checks


load_daily_stock_price_dag()
