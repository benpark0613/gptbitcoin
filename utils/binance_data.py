# binance_data.py

import os
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
from dotenv import load_dotenv
import time

load_dotenv()
API_KEY = os.getenv("BINANCE_ACCESS_KEY", "")
API_SECRET = os.getenv("BINANCE_SECRET_KEY", "")
client = Client(API_KEY, API_SECRET)

def fetch_binance_futures_klines(symbol, interval, months=15):
    end = datetime.utcnow()
    start = end - timedelta(days=30 * months)
    df_list = []
    while True:
        limit = 1500
        klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=int(start.timestamp() * 1000),
            endTime=int(end.timestamp() * 1000)
        )
        if not klines:
            break
        df_part = pd.DataFrame(klines, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","quote_vol","trades","taker_base",
            "taker_quote","ignore"
        ])
        df_part["open_time"] = pd.to_datetime(df_part["open_time"], unit='ms', utc=True)
        df_part["close_time"] = pd.to_datetime(df_part["close_time"], unit='ms', utc=True)
        df_list.append(df_part)
        last = df_part["close_time"].iloc[-1]
        if last >= end:
            break
        start = last + timedelta(milliseconds=1)
        time.sleep(0.2)
    if not df_list:
        return pd.DataFrame()
    df = pd.concat(df_list).drop_duplicates(subset=["open_time"]).reset_index(drop=True)
    df = df[["open_time","open","high","low","close","volume","close_time"]]
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df = df.sort_values("open_time").reset_index(drop=True)
    return df
