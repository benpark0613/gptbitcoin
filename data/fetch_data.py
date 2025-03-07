# gptbitcoin/data/fetch_data.py
"""
바이낸스 선물 API에서 OHLCV 데이터를 안전하게 수집하여 pandas DataFrame으로 반환하는 모듈.
NaN(결측치)이 하나라도 발견되면 예외를 발생시킨다.
(입력 파라미터인 start_time, end_time은 모두 UTC 기준 문자열로 가정한다.)
"""

import sys
import datetime
import pytz
import pandas as pd
from binance.client import Client

try:
    from config.config import (
        BINANCE_API_KEY,
        BINANCE_SECRET_KEY,
    )
except ImportError:
    print("config.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# 바이낸스 선물 API는 최대 1500봉(batch)만 요청 가능
BATCH_LIMIT = 1500

def get_ohlcv_from_binance(
        symbol: str,
        timeframe: str,
        start_time: str,  # "YYYY-MM-DD HH:MM:SS" (UTC 기준)
        end_time: str    # "YYYY-MM-DD HH:MM:SS" (UTC 기준)
) -> pd.DataFrame:
    """
    바이낸스 선물 시장에서 (symbol, timeframe, UTC의 start_time~end_time) 구간의 OHLCV 데이터를 수집한다.
    결측치가 하나라도 발견되면 예외를 발생시킨다.

    Args:
        symbol (str): 예) "BTCUSDT"
        timeframe (str): 예) "1d", "4h", "1h" 등
        start_time (str): "YYYY-MM-DD HH:MM:SS" (UTC 기준)
        end_time (str): "YYYY-MM-DD HH:MM:SS" (UTC 기준)

    Returns:
        pd.DataFrame:
            - 컬럼: ["open_time", "open", "high", "low", "close", "volume"]
            - open_time은 UTC 기준 밀리초(에포크 시간)

    Raises:
        ValueError: 데이터프레임에 결측치가 존재하거나, 수집 결과가 없으면 발생
    """

    client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_SECRET_KEY)

    # 입력받은 시간(UTC 문자열)을 datetime + pytz.utc 로 해석
    dt_format = "%Y-%m-%d %H:%M:%S"
    naive_start = datetime.datetime.strptime(start_time, dt_format)
    naive_end = datetime.datetime.strptime(end_time, dt_format)
    utc = pytz.utc
    start_utc = utc.localize(naive_start)
    end_utc = utc.localize(naive_end)

    start_ms = int(start_utc.timestamp() * 1000)
    end_ms = int(end_utc.timestamp() * 1000)

    # 결과 담을 리스트
    all_candles = []

    # 수집 시작 지점
    current_ms = start_ms

    while True:
        klines = client.futures_klines(
            symbol=symbol,
            interval=timeframe,
            startTime=current_ms,
            endTime=end_ms,
            limit=BATCH_LIMIT
        )
        if not klines:
            break

        all_candles.extend(klines)
        last_open_time = klines[-1][0]
        next_time = last_open_time + 1

        if next_time > end_ms:
            break

        current_ms = next_time
        if len(klines) < BATCH_LIMIT:
            break

    if not all_candles:
        raise ValueError("수집된 데이터가 없습니다. (빈 결과)")

    df = pd.DataFrame(
        all_candles,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
        ]
    )
    # 필요한 칼럼만 사용
    df = df[["open_time", "open", "high", "low", "close", "volume"]]

    # 숫자 변환
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # 결측치 검사
    if df.isnull().any().any():
        raise ValueError("OHLCV 데이터 내 결측치(NaN)가 발견되었습니다.")

    return df
