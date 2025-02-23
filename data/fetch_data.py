# gptbitcoin/data/fetch_data.py
# 구글 스타일, 최소한의 한글 주석
#
# 바이낸스 선물 데이터를 UTC 기준 시간으로 받아 CSV에 저장한다.
# 기존 _convert_utc_ms_to_kst_str 대신 UTC 타임스탬프를 그대로 기록하도록 수정했다.

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
UTC = pytz.utc  # (과거 코드와 동일하나, KST 변환은 제거)
# KST = pytz.timezone("Asia/Seoul")  # 더이상 사용 안 함

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
    시작 날짜에서 warmup_bars만큼 이전으로 돌아간 UTC datetime을 구한다.
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
    단일 호출로 [start_str ~ end_str] 구간 사이 최대 limit개 봉(바)만 가져온다.
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
    start_utc ~ end_utc 구간 전체 데이터를 여러 번에 걸쳐 가져온다.
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

        # 다음 호출 시작 시점(1초 뒤)
        current_start = last_open_dt_utc + timedelta(seconds=1)

    return all_klines


def _convert_ms_to_utc_str(timestamp_ms: int) -> str:
    """
    UTC 기준 밀리초 타임스탬프 -> UTC YYYY-MM-DD HH:MM:SS 로 변환
    """
    dt_utc = datetime.utcfromtimestamp(timestamp_ms / 1000.0)  # tzinfo 없음, UTC 시각
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")


def fetch_ohlcv_csv(
        symbol: str,
        interval: str,
        start_str: str = START_DATE,
        end_str: str = END_DATE,
        warmup_bars: int = WARMUP_BARS
) -> str:
    """
    바이낸스 선물 데이터를 (워밍업 포함) 전체 구간 받아 CSV로 저장.
    시간을 UTC 기준으로 기록한다.

    Args:
        symbol (str): 예) "BTCUSDT"
        interval (str): 예) "1h", "4h", "1d" 등
        start_str (str): 시작 날짜
        end_str (str): 종료 날짜
        warmup_bars (int): 워밍업 봉 수

    Returns:
        str: 저장된 CSV 파일 경로
    """
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

    warmup_start_utc = _calculate_warmup_start(start_str, interval, warmup_bars)
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    end_utc = UTC.localize(end_dt)

    klines_all = _fetch_klines_full(client, symbol, interval, warmup_start_utc, end_utc)
    csv_path = _write_ohlcv_csv_utc(klines_all, symbol, interval)
    return csv_path


def _write_ohlcv_csv_utc(klines: list, symbol: str, interval: str) -> str:
    """
    UTC 기준 CSV로 저장. data/origin_ohlcv/symbol_interval.csv
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
            vol = row[5]

            dt_utc_str = _convert_ms_to_utc_str(ts_ms)
            writer.writerow([dt_utc_str, o_price, h_price, l_price, c_price, vol])

    return filepath
