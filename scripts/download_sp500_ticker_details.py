"""
一次性脚本：下载标普500成分股公司信息（维度数据）
运行方式：python jobs/local/download_sp500_ticker_details.py
"""

import yfinance as yf
import pandas as pd
import time
import os


# ── 1. 获取标普500成分股列表 ──────────────────────────────────────────────────
def get_sp500_tickers():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    tickers = table[0]['Symbol'].tolist()
    tickers = [t.replace('.', '-') for t in tickers]
    print(f"标普500成分股数量: {len(tickers)}")
    return tickers


# ── 2. 批量下载公司信息 ──────────────────────────────────────────────────────
def download_ticker_details(tickers):
    results = []
    failed = []

    for i, ticker in enumerate(tickers):
        try:
            info = yf.Ticker(ticker).info

            market_cap = info.get("marketCap", None)
            if market_cap and market_cap > 200000000000:
                cap_desc = "Mega Cap"
            elif market_cap and market_cap > 10000000000:
                cap_desc = "Large Cap"
            elif market_cap and market_cap > 2000000000:
                cap_desc = "Mid Cap"
            elif market_cap and market_cap > 250000000:
                cap_desc = "Small Cap"
            elif market_cap:
                cap_desc = "Micro Cap"
            else:
                cap_desc = "Not Provided"

            results.append({
                "ticker": ticker,
                "company_name": info.get("shortName", ""),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "market_cap": market_cap,
                "market_cap_description": cap_desc,
                "exchange": info.get("exchange", ""),
                "country": info.get("country", ""),
            })

            print(f"[{i+1}/{len(tickers)}] {ticker} ✓")

        except Exception as e:
            print(f"[{i+1}/{len(tickers)}] {ticker} 失败: {e}")
            failed.append(ticker)

        if (i + 1) % 50 == 0:
            time.sleep(2)

    print(f"\n成功: {len(results)} 只，失败: {len(failed)} 只")
    if failed:
        print(f"失败列表: {failed}")

    return pd.DataFrame(results)


# ── 3. 保存结果 ──────────────────────────────────────────────────────────────
def save(df, output_path="data/sp500_ticker_details.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\n已保存到 {output_path}")
    print(f"总行数: {len(df)}")
    print("\n数据预览:")
    print(df.head(10).to_string(index=False))


# ── 主流程 ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tickers = get_sp500_tickers()
    df = download_ticker_details(tickers)

    if not df.empty:
        save(df)
