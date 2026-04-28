"""
一次性脚本：下载标普500所有成分股近一年历史数据
运行方式：python jobs/local/download_sp500_history.py
"""

import yfinance as yf
import pandas as pd
import time
import os


# ── 1. 获取标普500成分股列表（从 Wikipedia）──────────────────────────────────
def get_sp500_tickers():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    tickers = table[0]['Symbol'].tolist()
    tickers = [t.replace('.', '-') for t in tickers]
    print(f"标普500成分股数量: {len(tickers)}")
    return tickers


# ── 2. 批量下载历史数据 ───────────────────────────────────────────────────────
def download_history(tickers):
    dfs = []
    failed = []

    for i, ticker in enumerate(tickers):
        try:
            df = yf.download(ticker, start="2025-01-01", end="2026-04-03", progress=False, auto_adjust=True)

            if df.empty:
                print(f"[{i+1}/{len(tickers)}] {ticker} 无数据，跳过")
                failed.append(ticker)
                continue

            # 处理 MultiIndex 列（新版 yfinance）
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df["ticker"] = ticker

            # 统一列名为小写
            df.columns = [c.lower() for c in df.columns]

            # 只保留需要的列
            df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]
            df = df.rename(columns={"date": "snapshot_date"})

            dfs.append(df)

            print(f"[{i+1}/{len(tickers)}] {ticker} ✓ {len(df)} 行")

        except Exception as e:
            print(f"[{i+1}/{len(tickers)}] {ticker} 失败: {e}")
            failed.append(ticker)

        if (i + 1) % 50 == 0:
            time.sleep(1)

    print(f"\n成功: {len(dfs)} 只，失败: {len(failed)} 只")
    if failed:
        print(f"失败列表: {failed}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── 3. 保存结果 ──────────────────────────────────────────────────────────────
def save(df, output_path="data/sp500_history.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\n已保存到 {output_path}")
    print(f"总行数: {len(df):,}")
    print(f"时间范围: {df['snapshot_date'].min()} ~ {df['snapshot_date'].max()}")
    print(f"股票数量: {df['ticker'].nunique()}")
    print(f"\n列名: {list(df.columns)}")
    print("\n数据预览:")
    print(df.head(10).to_string(index=False))


# ── 主流程 ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tickers = get_sp500_tickers()
    df = download_history(tickers)

    if not df.empty:
        save(df)
