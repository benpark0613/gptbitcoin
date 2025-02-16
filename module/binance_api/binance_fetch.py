# module/binance_api/binance_fetch.py

import os
import time
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

load_dotenv()
api_key = os.getenv("BINANCE_ACCESS_KEY", "")
api_secret = os.getenv("BINANCE_SECRET_KEY", "")
client = Client(api_key, api_secret)

def fetch_binance_klines(symbol: str, interval: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """
    바이낸스에서 (symbol, interval) 캔들 데이터를 받아
    [open_time, open, high, low, close, volume] 형식의 DataFrame으로 반환.
    """
    limit = 1500
    all_data = []
    current_ts = start_ts

    while True:
        try:
            klines = client.get_klines(
                symbol=symbol,
                interval=interval,
                startTime=current_ts,
                endTime=end_ts,
                limit=limit
            )
        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Binance API error: {e}")
            break

        if not klines:
            break

        all_data.extend(klines)
        last_open_time = klines[-1][0]
        current_ts = last_open_time + 1
        if current_ts >= end_ts:
            break

        time.sleep(0.2)

    records = []
    for k in all_data:
        records.append([
            k[0], k[1], k[2], k[3], k[4], k[5]
        ])

    df = pd.DataFrame(records, columns=[
        "open_time", "open", "high", "low", "close", "volume"
    ])

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df
