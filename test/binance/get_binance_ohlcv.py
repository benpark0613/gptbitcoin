# get_binance_ohlcv.py

import os
import shutil
import pandas as pd
from datetime import datetime, timezone
from binance.client import Client
from dotenv import load_dotenv

def datetime_to_milliseconds(dt_str):
    if len(dt_str.strip()) == 10:
        dt_str += " 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms, limit=1500):
    """
    바이낸스 선물 OHLCV 데이터를 한 번에 limit개씩 끊어 가져옵니다.
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

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    return df

def main(
        symbol="BTCUSDT",
        intervals=None,
        start_date="2024-01-01",
        end_date="2025-01-01",
        save_folder="test_result"
):
    """
    필요한 파라미터만 사용자가 지정해주면,
    BTCUSDT 선물에 대한 여러 타임프레임의 OHLCV 데이터를 가져와 CSV로 저장합니다.
    """
    if intervals is None:
        intervals = ["15m"]
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    client = Client(api_key, api_secret)

    start_ms = datetime_to_milliseconds(start_date)
    end_ms = datetime_to_milliseconds(end_date)

    if os.path.exists(save_folder):
        shutil.rmtree(save_folder)
    os.makedirs(save_folder, exist_ok=True)

    for interval in intervals:
        df = fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms)

        # open_time을 datetime으로 변환 후 인덱스로 설정
        df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time_dt", inplace=True)

        # 원하는 컬럼만 선택
        # index로 쓰고 있는 open_time_dt를 일반 컬럼으로 쓰기 위해 reset_index() 사용
        df.reset_index(inplace=True)
        columns_to_keep = ["open_time_dt", "open", "high", "low", "close", "volume"]

        # CSV 저장 (open_time_dt를 컬럼으로 포함, 인덱스는 저장 안 함)
        file_name = f"{symbol}_{interval}.csv"
        file_path = os.path.join(save_folder, file_name)
        df[columns_to_keep].to_csv(file_path, encoding="utf-8", index=False)

        print(f"[{symbol} - {interval}] : {len(df)}건 데이터 저장 완료 → {file_path}")

if __name__ == "__main__":
    main(
        symbol="BTCUSDT",
        intervals=["1d"],
        start_date="2023-08-15",
        end_date="2023-08-20",
        save_folder="test_result"
    )
