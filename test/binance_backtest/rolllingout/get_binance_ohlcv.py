import os
import shutil
import pandas as pd
from datetime import datetime, timezone
from binance.client import Client
from dotenv import load_dotenv

def datetime_to_milliseconds(dt_str):
    """
    날짜 문자열(YYYY-MM-DD 혹은 YYYY-MM-DD HH:MM:SS)을
    UTC 타임존 기준 밀리초 단위로 변환합니다.
    """
    if len(dt_str.strip()) == 10:
        dt_str += " 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms, limit=1500):
    """
    바이낸스 선물 OHLCV 데이터를, 한 번에 최대 limit개씩 끊어가며
    (start_ms ~ end_ms) 구간 전체를 모두 수집합니다.
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

    # 수집된 데이터를 DataFrame으로 변환
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
    1) 환경 변수 로드 및 바이낸스 Client 초기화
    2) 날짜 범위를 밀리초로 변환
    3) 기존 저장 폴더 정리
    4) 타임프레임별로 데이터 가져와 CSV로 저장
    """
    # (1) 환경 변수 로드 및 Binance Client 초기화
    if intervals is None:
        intervals = ["15m"]
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    client = Client(api_key, api_secret)

    # (2) 날짜 범위를 밀리초로 변환
    start_ms = datetime_to_milliseconds(start_date)
    end_ms = datetime_to_milliseconds(end_date)

    # (3) 기존 저장 폴더 정리
    if os.path.exists(save_folder):
        shutil.rmtree(save_folder)
    os.makedirs(save_folder, exist_ok=True)

    # (4) 타임프레임별로 데이터 가져와 CSV 저장
    for interval in intervals:
        df = fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms)

        # (4-1) open_time을 DateTime으로 변환
        df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms")

        # (4-2) open_time_dt를 인덱스로 설정
        df.set_index("open_time_dt", inplace=True)

        # (4-3) 불필요한 컬럼 제거
        drop_cols = [
            "open_time", "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ]
        df.drop(columns=drop_cols, inplace=True, errors="ignore")

        # (4-4) 컬럼 순서 정리 (OHLCV만 남김)
        df = df[["open", "high", "low", "close", "volume"]]

        # (4-5) CSV로 저장
        file_name = f"{symbol}_{interval}.csv"
        file_path = os.path.join(save_folder, file_name)
        df.to_csv(file_path, encoding="utf-8", index=True)

        print(f"[{symbol} - {interval}] : {len(df)}건 데이터 저장 완료 → {file_path}")

if __name__ == "__main__":
    # 테스트 실행 예시
    main(
        symbol="BTCUSDT",
        intervals=["1d", "1h", "30m", "15m"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        save_folder="test_result"
    )
