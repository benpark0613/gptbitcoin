# gptbitcoin/data/fetch_data.py
# 구글 스타일, 최소한의 한글 주석
# 이 모듈은 오직 바이낸스 API로부터 OHLCV 봉 데이터를 가져오는 기능만 담당한다.
# DB 삽입 로직은 update_data.py 로 분리함.

import math
import pytz
from datetime import datetime, timedelta
from typing import List
from binance.client import Client

from config.config import (
    BINANCE_API_KEY,
    BINANCE_SECRET_KEY,
    EXCHANGE_OPEN_DATE,
)

UTC = pytz.utc
MAX_LIMIT = 1500

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

def _fetch_klines_chunk(
    client: Client,
    symbol: str,
    interval: str,
    start_str: str,
    end_str: str,
    limit: int = MAX_LIMIT
) -> list:
    """
    바이낸스 선물 API에서 [start_str ~ end_str] 구간의 봉 데이터를
    최대 limit개만 받아온다.
    """
    return client.futures_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_str,
        end_str=end_str,
        limit=limit
    )


def _fetch_klines_full(
    client: Client,
    symbol: str,
    interval: str,
    start_utc: datetime,
    end_utc: datetime
) -> list:
    """
    [start_utc ~ end_utc] 구간 전체를 여러 번 _fetch_klines_chunk로 호출하여
    모두 합쳐 반환한다.
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
            # 더 이상 받아올 데이터가 없으면 중단
            break

        all_klines.extend(chunk)

        last_open_time_ms = chunk[-1][0]
        last_open_dt_utc = datetime.utcfromtimestamp(last_open_time_ms / 1000.0).replace(tzinfo=UTC)
        # 다음 chunk는 마지막 open_time + 1초 후부터
        current_start = last_open_dt_utc + timedelta(seconds=1)

    return all_klines


def fetch_ohlcv(
    symbol: str,
    interval: str,
    start_str: str,
    end_str: str
) -> list:
    """
    바이낸스 선물 API에서 [start_str ~ end_str] 구간의 봉 데이터를 전부 받아,
    klines 리스트로 반환한다. (DB 저장 X)
    EXCHANGE_OPEN_DATE 이전 요청은 의미 없으므로, start_str을 보정할 수 있음.

    Args:
        symbol (str): 예) "BTCUSDT"
        interval (str): 예) "1d", "5m" 등
        start_str (str): 예) "2019-01-01"
        end_str (str): 예) "2020-01-01"

    Returns:
        list: klines = [
            [open_time, open, high, low, close, volume, ...], ...
        ]
    """
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

    exchange_open_dt = datetime.strptime(EXCHANGE_OPEN_DATE, "%Y-%m-%d")
    user_start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    user_end_dt   = datetime.strptime(end_str,   "%Y-%m-%d")

    actual_start_dt = max(exchange_open_dt, user_start_dt)
    if user_end_dt <= exchange_open_dt:
        print("[WARN] 요청한 end_date가 거래소 오픈일과 같거나 이전이므로, 데이터 없음.")
        return []

    start_utc = UTC.localize(actual_start_dt)
    end_utc   = UTC.localize(user_end_dt)

    print(f"[INFO] fetch_ohlcv => symbol={symbol}, interval={interval}, "
          f"start={actual_start_dt}, end={user_end_dt}")

    klines = _fetch_klines_full(client, symbol, interval, start_utc, end_utc)
    return klines
