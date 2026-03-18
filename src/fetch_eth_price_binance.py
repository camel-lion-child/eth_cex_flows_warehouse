"""
Fetch ETH daily price data from Binance and build a historical dataset.
Computes daily returns and rolling volatility for analysis.
Saves results as a structured CSV.

Récupère les prix quotidiens ETH depuis Binance et construit un dataset historique.
Calcule les rendements journaliers et la volatilité glissante.
Sauvegarde les résultats dans un CSV structuré.
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

OUT_PATH = "data/processed/binance/eth_price_daily.csv"
os.makedirs("data/processed/binance", exist_ok=True)

BINANCE_URL = "https://api.binance.com/api/v3/klines" #binance API endpoint for historical price data (klines)


def fetch_binance_kline(limit=1000, end_time=None):

    params = {
        "symbol": "ETHUSDT", #Ethereum price in USDT
        "interval": "1d", #daily candles
        "limit": limit,
    }

    if end_time: #use end_time to paginate backward in time
        params["endTime"] = end_time

    r = requests.get(BINANCE_URL, params=params)
    data = r.json()

    if not isinstance(data, list): #validate API response
        print("Error:", data) #convert timestamp to seconds
        return pd.DataFrame() #closing price

    rows = []
    for k in data: #transform raw JSON to structured rows
        ts = int(k[0]) // 1000
        price = float(k[4])  

        rows.append({"timestamp": ts, "price_usd": price})

    return pd.DataFrame(rows)


def fetch_full_history(): #build a full historical dataset by fetching data in multiple API calls
    all_df = []
    end_time = None #handle API limits using pagination

    print("Fetching full ETH price history from Binance...")

    for i in range(5):  #loop to fetch multiple chunks of historical data
        print(f"  Fetching chunk {i+1}/5")

        df = fetch_binance_kline(limit=1000, end_time=end_time)
        if df.empty:
            break

        all_df.append(df)

        oldest_ts = df["timestamp"].min() * 1000 #move backward in time using the oldest timestamp
        end_time = oldest_ts - 1

        time.sleep(0.5) #avoid hitting API rate limits

    full = pd.concat(all_df, ignore_index=True) #combine all chunks into 1 dataset
    full["day"] = pd.to_datetime(full["timestamp"], unit="s").dt.date
    full = full.drop_duplicates("day").sort_values("day")

    return full[["day", "price_usd"]]


def add_returns(df): #compute financial indicators from price data
    df["daily_return"] = df["price_usd"].pct_change() #daily return, percentage change
    df["rolling_vol_7d"] = df["daily_return"].rolling(7).std() #7-day rolling volatility (standard deviation of returns)
    return df


if __name__ == "__main__":
    df = fetch_full_history() #fetch historical ETH price data

    print("Computing returns & volatility...")
    df = add_returns(df) #compute financial metrics

    df.to_csv(OUT_PATH, index=False) #save processed dataset

    print(f"DONE → {OUT_PATH}")
    print(">>> Running fetch_eth_price_binance.py (USING V2 API)")  
