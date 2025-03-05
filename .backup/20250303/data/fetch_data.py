# gptbitcoin/data/fetch_data.py
# 구글 스타일, 최소한의 한글 주석
# 바이낸스 선물 API로 OHLCV를 가져오는 로직 + 예외적 볼륨 교정 로직을 분리.
# 1) fetch_ohlcv(...)로 klines 수집 → klines_to_dataframe(...)으로 DataFrame 변환
# 2) fix_ohlcv_exceptions(...)에서 특정 날짜/interval의 볼륨을 하드코딩 교정.

from datetime import datetime, timedelta
import pytz

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


def _fetch_klines_chunk(client: Client, symbol: str, interval: str,
                        start_str: str, end_str: str, limit: int = MAX_LIMIT) -> list:
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


def _fetch_klines_full(client: Client, symbol: str, interval: str,
                       start_utc: datetime, end_utc: datetime) -> list:
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


def klines_to_dataframe(klines: list) -> 'pd.DataFrame':
    """
    바이낸스 klines(list)를 Pandas DataFrame으로 변환.
    columns = ["datetime_utc","open","high","low","close","volume"].
    여기서는 단순 변환만 하고, 예외 교정은 별도 함수에서 처리한다.
    """

    df_list = []
    for row in klines:
        # row: [open_time, open, high, low, close, volume, ...]
        open_time_ms = int(row[0])
        dt_utc = datetime.utcfromtimestamp(open_time_ms / 1000.0).replace(tzinfo=UTC)
        dt_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

        o_val = float(row[1])
        h_val = float(row[2])
        l_val = float(row[3])
        c_val = float(row[4])
        vol_val = float(row[5])  # 이후 교정 필요 시 fix_ohlcv_exceptions에서 처리

        # volume 정수 반올림
        vol_val = round(vol_val)

        df_list.append([dt_str, o_val, h_val, l_val, c_val, vol_val])

    import pandas as pd
    df = pd.DataFrame(df_list, columns=["datetime_utc","open","high","low","close","volume"])
    return df


def fix_ohlcv_exceptions(df: 'pd.DataFrame', interval: str) -> 'pd.DataFrame':
    """
    특정 날짜/interval 조합에 대해
    하드코딩된 볼륨값 등 예외처리를 수행하는 함수.
    여러 케이스를 대비해 딕셔너리나 조건문으로 처리 가능.
    """

    # 예시: (날짜, interval) => 강제 볼륨
    # 필요한 만큼 추가 가능
    volume_fixes = {
        ("2023-08-16", "1d"): 280545,
        # ("2023-08-20", "4h"): 123456,  # 필요 시 이런 식으로 확장
    }

    # df['datetime_utc']가 "YYYY-MM-DD HH:MM:SS" 형태이므로
    # 날짜 부분만 추출해 interval과 함께 확인
    for idx in df.index:
        dt_str = df.at[idx, "datetime_utc"]  # "YYYY-MM-DD HH:MM:SS"
        date_only = dt_str.split(" ")[0]     # "YYYY-MM-DD"

        key = (date_only, interval)         # 튜플 키
        if key in volume_fixes:
            df.at[idx, "volume"] = volume_fixes[key]

    return df


def fetch_ohlcv(symbol: str, interval: str, start_str: str, end_str: str) -> list:
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

    exchange_open_dt = datetime.strptime(EXCHANGE_OPEN_DATE, "%Y-%m-%d %H:%M:%S")
    user_start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    user_end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")

    actual_start_dt = max(exchange_open_dt, user_start_dt)
    if user_end_dt <= exchange_open_dt:
        print("[WARN] 요청한 end_date가 거래소 오픈일과 같거나 이전이므로, 데이터 없음.")
        return []

    start_utc = UTC.localize(actual_start_dt)
    end_utc   = UTC.localize(user_end_dt)

    print(f"[INFO] fetch_ohlcv => symbol={symbol}, interval={interval}, "
          f"start={actual_start_dt}, end={user_end_dt}")

    # 1) klines 가져오기
    klines = _fetch_klines_full(client, symbol, interval, start_utc, end_utc)

    # 2) DataFrame 변환
    df_raw = klines_to_dataframe(klines)

    # 3) 예외 교정
    df_fixed = fix_ohlcv_exceptions(df_raw, interval)

    # 필요 시 df_fixed => list 형태로 되돌릴 수도 있음
    # 여기서는 list로 반환한다면 원래 klines와 동일 구조로 만들어야 함
    # [ [open_time, open, high, low, close, volume, ...], ... ]

    result_list = []
    for _, row in df_fixed.iterrows():
        dt_str = row["datetime_utc"]
        # dt_str -> timestamp(ms) 변환
        dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        dt_utc = dt_obj.replace(tzinfo=UTC)
        open_time_ms = int(dt_utc.timestamp() * 1000)

        # klines 구조: [open_time, open, high, low, close, volume, ...]
        result_list.append([
            open_time_ms,
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"]
        ])

    return result_list
