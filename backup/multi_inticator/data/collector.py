import os
import pandas as pd
from datetime import datetime, timezone
from binance.client import Client
from dotenv import load_dotenv

def datetime_to_milliseconds(dt_str):
    """
    주어진 날짜 문자열("YYYY-MM-DD" 또는 "YYYY-MM-DD HH:MM:SS")을
    밀리초 단위의 타임스탬프로 변환합니다.
    """
    if len(dt_str.strip()) == 10:
        dt_str += " 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms, limit=1500):
    """
    바이낸스 선물 OHLCV 데이터를 pagination 방식으로 limit 개수씩 끊어 가져옵니다.
    """
    all_data = []
    current_start = start_ms

    while True:
        klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            startTime=current_start,
            endTime=end_ms,
            limit=limit
        )
        if not klines:
            break

        all_data.extend(klines)
        last_open_time = klines[-1][0]
        next_start = last_open_time + 1
        if next_start > end_ms:
            break
        current_start = next_start

        if len(klines) < limit:
            break

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    return df
