"""
实时微批 DAG：美股开市期间每 5 分钟跑一次
1. yfinance 拉 S&P 500 最近 1 分钟 K 线
2. UPSERT 到 marts.current_day_stock_price
"""
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


STARBURST_HOST = "YOUR_STARBURST_HOST"
STARBURST_PORT = 443
STARBURST_USER = "YOUR_STARBURST_USER"
STARBURST_PASSWORD = "YOUR_STARBURST_PASSWORD"
CATALOG = "stock_data"
TARGET_TABLE = f"{CATALOG}.marts.current_day_stock_price"


def run_starburst_sql(query):
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication
    conn = connect(
        host=STARBURST_HOST,
        port=STARBURST_PORT,
        user=STARBURST_USER,
        catalog=CATALOG,
        schema="marts",
        http_scheme="https",
        auth=BasicAuthentication(STARBURST_USER, STARBURST_PASSWORD),
    )
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


def _get_sp500_tickers():
    import pandas as pd
    resp = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        headers={"User-Agent": "Mozilla/5.0 (compatible; StockDE/1.0)"},
        timeout=30,
    )
    sp500_table = pd.read_html(resp.text)[0]
    return sp500_table["Symbol"].str.replace(".", "-", regex=False).tolist()


def fetch_latest_prices(**context):
    import yfinance as yf
    import pandas as pd

    tickers = _get_sp500_tickers()
    df = yf.download(
        tickers,
        period="1d",
        interval="1m",
        group_by="ticker",
        progress=False,
        threads=True,
    )

    if df.empty:
        print("yfinance returned empty (非交易时段 / 节假日),跳过")
        return []

    records = []
    for t in tickers:
        try:
            sub = df[t].dropna() if isinstance(df.columns, pd.MultiIndex) else df.dropna()
            if sub.empty:
                continue
            last = sub.iloc[-1]
            ts = sub.index[-1]
            ts_str = (
                ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
                if ts.tzinfo
                else ts.strftime("%Y-%m-%d %H:%M:%S.%f")
            )
            records.append({
                "ticker": t.replace("-", "."),
                "trade_date": ts.strftime("%Y-%m-%d"),
                "last_price": round(float(last["Close"]), 4),
                "open_price": round(float(last["Open"]), 4),
                "high_price": round(float(last["High"]), 4),
                "low_price":  round(float(last["Low"]),  4),
                "volume":     int(last["Volume"]),
                "last_updated_ts": ts_str,
            })
        except (KeyError, TypeError):
            continue

    print(f"Fetched {len(records)} ticker snapshots")
    return records


def upsert_to_current_day(**context):
    records = context["ti"].xcom_pull(task_ids="fetch_latest_prices")
    if not records:
        print("No records, skip upsert")
        return

    rows_sql = ",\n".join(
        f"('{r['ticker']}', DATE '{r['trade_date']}', {r['last_price']}, {r['open_price']}, "
        f"{r['high_price']}, {r['low_price']}, {r['volume']}, "
        f"TIMESTAMP '{r['last_updated_ts']}')"
        for r in records
    )

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} t
        USING (
            VALUES {rows_sql}
        ) AS s(ticker, trade_date, last_price, open_price, high_price, low_price, volume, last_updated_ts)
        ON (t.ticker = s.ticker AND t.trade_date = s.trade_date)
        WHEN MATCHED THEN UPDATE SET
            last_price = s.last_price,
            open_price = s.open_price,
            high_price = s.high_price,
            low_price  = s.low_price,
            volume     = s.volume,
            last_updated_ts = s.last_updated_ts
        WHEN NOT MATCHED THEN INSERT
            (ticker, trade_date, last_price, open_price, high_price, low_price, volume, last_updated_ts)
            VALUES (s.ticker, s.trade_date, s.last_price, s.open_price, s.high_price, s.low_price, s.volume, s.last_updated_ts)
    """
    run_starburst_sql(merge_sql)
    print(f"UPSERT {len(records)} rows into {TARGET_TABLE}")


def sanity_check(**context):
    records = context["ti"].xcom_pull(task_ids="fetch_latest_prices")
    if not records:
        print("Upstream skipped, nothing to check")
        return

    result = run_starburst_sql(f"""
        SELECT COUNT(*) AS n,
               CAST(MAX(last_updated_ts) AS VARCHAR) AS latest_ts
        FROM {TARGET_TABLE}
        WHERE trade_date = DATE '{records[0]['trade_date']}'
    """)
    n, latest = result[0]
    print(f"Sanity: {n} rows for today, latest_ts={latest}")
    if n < 400:
        raise ValueError(f"Row count too low: {n}")


@dag(
    "update_current_day_stock_price_microbatch_dag",
    description="Microbatch: refresh current_day_stock_price every 5 min during US market hours",
    default_args={
        "owner": "stock-project",
        "start_date": datetime(2025, 1, 1),
        "retries": 0,
    },
    schedule="*/5 13-21 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "microbatch", "stock"],
)
def update_current_day_stock_price_microbatch_dag():

    fetch = PythonOperator(
        task_id="fetch_latest_prices",
        python_callable=fetch_latest_prices,
    )

    upsert = PythonOperator(
        task_id="upsert_to_current_day",
        python_callable=upsert_to_current_day,
    )

    check = PythonOperator(
        task_id="sanity_check",
        python_callable=sanity_check,
    )

    fetch >> upsert >> check


update_current_day_stock_price_microbatch_dag()
