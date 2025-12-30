import requests
import pandas as pd
import time
import os
from datetime import datetime

OUT_PATH = "data/processed/binance/eth_price_daily.csv"
os.makedirs("data/processed/binance", exist_ok=True)

BINANCE_URL = "https://api.binance.com/api/v3/klines"


def fetch_binance_kline(limit=1000, end_time=None):

    params = {
        "symbol": "ETHUSDT",
        "interval": "1d",
        "limit": limit,
    }

    if end_time:
        params["endTime"] = end_time

    r = requests.get(BINANCE_URL, params=params)
    data = r.json()

    if not isinstance(data, list):
        print("Error:", data)
        return pd.DataFrame()

    rows = []
    for k in data:
        ts = int(k[0]) // 1000
        price = float(k[4])  

        rows.append({"timestamp": ts, "price_usd": price})

    return pd.DataFrame(rows)


def fetch_full_history():
    all_df = []
    end_time = None

    print("🔎 Fetching full ETH price history from Binance...")

    for i in range(5): 
        print(f"   → Fetching chunk {i+1}/5")

        df = fetch_binance_kline(limit=1000, end_time=end_time)
        if df.empty:
            break

        all_df.append(df)

        oldest_ts = df["timestamp"].min() * 1000
        end_time = oldest_ts - 1

        time.sleep(0.5)

    full = pd.concat(all_df, ignore_index=True)
    full["day"] = pd.to_datetime(full["timestamp"], unit="s").dt.date
    full = full.drop_duplicates("day").sort_values("day")

    return full[["day", "price_usd"]]


def add_returns(df):
    df["daily_return"] = df["price_usd"].pct_change()
    df["rolling_vol_7d"] = df["daily_return"].rolling(7).std()
    return df


if __name__ == "__main__":
    df = fetch_full_history()

    print("Computing returns & volatility...")
    df = add_returns(df)

    df.to_csv(OUT_PATH, index=False)

    print(f"DONE → {OUT_PATH}")
    print(">>> Running fetch_eth_price_binance.py (USING V2 API)")  
