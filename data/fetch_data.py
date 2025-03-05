# gptbitcoin/data/fetch_data.py
"""
바이낸스 선물 API에서 OHLCV 데이터를 안전하게 수집하여 pandas DataFrame으로 반환하는 모듈.
NaN(결측치)이 하나라도 발견되면 예외를 발생시킨다.
"""

import sys
from datetime import datetime

import pandas as pd
from binance import Client

# config.py에서 API 키 로드
# 필요 시 PYTHONPATH를 조정하거나 아래 import 경로를 조정하세요.
try:
    from config.config import (
        BINANCE_API_KEY,
        BINANCE_SECRET_KEY,
    )
except ImportError:
    print("config.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# 바이낸스 선물 API는 1500봉(batch) 제한이 있음
BATCH_LIMIT = 1500


def get_ohlcv_from_binance(
        symbol: str,
        timeframe: str,
        start_time: str,
        end_time: str
) -> pd.DataFrame:
    """
    바이낸스 선물 시장에서 (symbol, timeframe, start_time~end_time) 구간의 OHLCV 데이터를 수집한다.
    결측치가 하나라도 발견되면 예외를 발생시킨다.

    Args:
        symbol (str): 예) "BTCUSDT"
        timeframe (str): 예) "1d", "4h", "1h" 등
        start_time (str): "YYYY-MM-DD HH:MM:SS" 형태의 시작 시점
        end_time (str): "YYYY-MM-DD HH:MM:SS" 형태의 종료 시점

    Returns:
        pd.DataFrame: OHLCV 칼럼(Timestamp, Open, High, Low, Close, Volume) 포함.
                      timestamp는 UTC 밀리초 기준
    Raises:
        ValueError: 데이터프레임에 결측치가 하나라도 있으면 발생
    """

    client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_SECRET_KEY)

    # 문자열 시간 -> UTC 밀리초
    start_ms = int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    end_ms = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    # 결과를 담을 리스트
    all_candles = []

    # 수집 시작 지점
    current_ms = start_ms

    while True:
        # 바이낸스 선물 K라인(futures_klines) 호출
        klines = client.futures_klines(
            symbol=symbol,
            interval=timeframe,
            startTime=current_ms,
            endTime=end_ms,
            limit=BATCH_LIMIT
        )

        if not klines:
            # 더 이상 가져올 데이터가 없으면 종료
            break

        all_candles.extend(klines)

        # 마지막 봉의 시간
        last_open_time = klines[-1][0]

        # 다음 호출 시점 갱신
        # 봉이 1개라도 있으면 마지막 봉의 open_time + (분해된 간격)에 해당하는 값
        # 하지만 안전하게 last_open_time + 1ms로 설정
        next_time = last_open_time + 1

        # 다음 호출 시점이 종료 시점을 넘어가면 종료
        if next_time > end_ms:
            break

        current_ms = next_time

        # 혹시나 데이터가 정확히 1500봉 미만이면(즉, 더 이상 가져올 데이터가 없을 수도 있음) 체크
        if len(klines) < BATCH_LIMIT:
            break

    # 빈 데이터 처리
    if not all_candles:
        raise ValueError("수집된 데이터가 없습니다. (빈 결과)")

    # 수집 결과 -> DataFrame 변환
    # 선물 K라인 구조:
    # [
    #   [
    #     0: open time (ms)
    #     1: open
    #     2: high
    #     3: low
    #     4: close
    #     5: volume
    #     6: close time (ms)
    #     ...
    #   ],
    #   ...
    # ]
    df = pd.DataFrame(all_candles, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
    ])

    # 필요한 칼럼만 사용
    df = df[["open_time", "open", "high", "low", "close", "volume"]]

    # 숫자형 변환
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # 결측치 검사
    if df.isnull().any().any():
        raise ValueError("OHLCV 데이터 내 결측치(NaN)가 발견되었습니다.")

    return df
