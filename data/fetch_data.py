# gptbitcoin/data/fetch_data.py
# 구글 스타일, 최소한의 한글 주석
# futures_kline 데이터를 받아 CSV로 저장하되, volume을 소수점 첫째 자리에서 반올림해 정수로 저장

import os
import csv
from datetime import datetime, timedelta
import pytz

from binance.client import Client
from config.config import (
    BINANCE_API_KEY,
    BINANCE_SECRET_KEY,
    WARMUP_BARS,
    START_DATE,
    END_DATE,
    DATA_DIR,
    ORIGIN_OHLCV_DIR,
)

MAX_LIMIT = 1500
UTC = pytz.utc
INTERVAL_TO_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
    "1M": 43200,
}

def _calculate_warmup_start(start_str: str, interval: str, warmup_bars: int) -> datetime:
    """
    시작 날짜에서 warmup_bars만큼 이전으로 돌아간 UTC datetime 반환.
    """
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    start_utc = UTC.localize(start_dt)
    minutes_per_bar = INTERVAL_TO_MINUTES[interval]
    offset_minutes = warmup_bars * minutes_per_bar
    return start_utc - timedelta(minutes=offset_minutes)

def _fetch_klines_chunk(
        client: Client,
        symbol: str,
        interval: str,
        start_str: str,
        end_str: str,
        limit: int = MAX_LIMIT
) -> list:
    """
    단일 호출로 [start_str ~ end_str] 구간 사이 최대 limit개 봉만 가져옴.
    """
    klines = client.futures_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_str,
        end_str=end_str,
        limit=limit
    )
    return klines

def _fetch_klines_full(
        client: Client,
        symbol: str,
        interval: str,
        start_utc: datetime,
        end_utc: datetime
) -> list:
    """
    start_utc ~ end_utc 구간 전체를 여러 번 호출해 가져옴.
    """
    all_klines = []
    current_start = start_utc

    while True:
        if current_start >= end_utc:
            break

        chunk = _fetch_klines_chunk(
            client,
            symbol,
            interval,
            current_start.strftime("%Y-%m-%d %H:%M:%S"),
            end_utc.strftime("%Y-%m-%d %H:%M:%S"),
            limit=MAX_LIMIT
        )
        if not chunk:
            break

        all_klines.extend(chunk)
        last_open_time_ms = chunk[-1][0]
        last_open_dt_utc = datetime.utcfromtimestamp(last_open_time_ms / 1000.0).replace(tzinfo=UTC)
        current_start = last_open_dt_utc + timedelta(seconds=1)

    return all_klines

def _convert_ms_to_utc_str(timestamp_ms: int) -> str:
    """
    UTC 기준 밀리초 타임스탬프 -> 'YYYY-MM-DD HH:MM:SS' UTC 문자열
    """
    dt_utc = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")

def _write_ohlcv_csv_utc(klines: list, symbol: str, interval: str) -> str:
    """
    UTC 기준 CSV로 저장하되, volume을 소수점 첫째 자리에서 반올림하여 정수로 만든다.
    data/origin_ohlcv/symbol_interval.csv 형태로 저장
    """
    origin_dir = os.path.join(DATA_DIR, ORIGIN_OHLCV_DIR)
    os.makedirs(origin_dir, exist_ok=True)

    filename = f"{symbol}_{interval}.csv"
    filepath = os.path.join(origin_dir, filename)

    header = ["datetime_utc", "open", "high", "low", "close", "volume"]
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for row in klines:
            ts_ms = int(row[0])
            o_price = row[1]
            h_price = row[2]
            l_price = row[3]
            c_price = row[4]
            vol_raw = row[5]

            # volume을 float 변환 후 소수점 첫째 자리에서 반올림 -> int
            vol_val = float(vol_raw)
            vol_val = round(vol_val)  # 정수로 반올림

            dt_utc_str = _convert_ms_to_utc_str(ts_ms)
            writer.writerow([dt_utc_str, o_price, h_price, l_price, c_price, vol_val])

    return filepath

def fetch_ohlcv_csv(
        symbol: str,
        interval: str,
        start_str: str = START_DATE,
        end_str: str = END_DATE,
        warmup_bars: int = WARMUP_BARS
) -> str:
    """
    바이낸스 선물 데이터를 (워밍업 포함) 전체 구간 받아 CSV로 저장.
    volume은 소수점 첫째 자리에서 반올림해 int로 저장.
    """
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

    warmup_start_utc = _calculate_warmup_start(start_str, interval, warmup_bars)
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    end_utc = UTC.localize(end_dt)

    klines_all = _fetch_klines_full(client, symbol, interval, warmup_start_utc, end_utc)
    csv_path = _write_ohlcv_csv_utc(klines_all, symbol, interval)
    return csv_path
