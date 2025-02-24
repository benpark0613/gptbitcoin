# gptbitcoin/data/update_data.py
# 구글 스타일, 최소한의 한글 주석
#
# 이 모듈은 다음 과정을 수행한다:
#  1) fetch_ohlcv(...)로 바이낸스 선물 OHLCV 데이터를 [start_str ~ end_str] 구간, 여러 interval에 대해 가져옴
#  2) 받아온 데이터를 ohlcv 테이블에 저장 (INSERT OR REPLACE)
#  3) preprocess_ohlcv_data(...)로 보조지표 전부 계산
#  4) ohlcv_indicators 테이블에 calc_all_indicators에서 생성된 컬럼들(예: ma_5, rsi_14, obv...)을 저장
#
# db_utils.py에서 테이블 생성/저장 로직, preprocess.py에서 지표 계산 로직을 관리한다.
# 여기서는 'update_data_db' 함수 하나로 전 과정을 처리한다.

import sys
import os
import math
import pytz
from datetime import datetime
from typing import List

import pandas as pd

# 바이낸스 klines(OHLCV) 다운로드
from data.fetch_data import fetch_ohlcv

# 전처리 + 보조지표 계산 (NaN 발견 시 예외, dropna 인자)
from data.preprocess import preprocess_ohlcv_data

# DB 유틸 (테이블 생성 + insert)
from utils.db_utils import (
    get_connection,
    create_ohlcv_table_if_not_exists,
    insert_ohlcv,
    create_indicators_table_if_not_exists,
    insert_indicators
)

UTC = pytz.utc

def klines_to_dataframe(klines: list) -> pd.DataFrame:
    """
    바이낸스 klines(list) -> Pandas DataFrame 변환
    columns = ["datetime_utc","open","high","low","close","volume"]
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
        vol_val = float(row[5])

        df_list.append([dt_str, o_val, h_val, l_val, c_val, vol_val])

    df = pd.DataFrame(df_list, columns=["datetime_utc","open","high","low","close","volume"])
    return df

def dataframe_to_rows(df: pd.DataFrame) -> list:
    """
    insert_ohlcv(conn, symbol, interval, rows)가 요구하는
    [(datetime_utc, open, high, low, close, volume), ...] 형태로 변환.
    """
    rows = []
    for _, row in df.iterrows():
        dt_str = row["datetime_utc"]
        o_val  = float(row["open"])
        h_val  = float(row["high"])
        l_val  = float(row["low"])
        c_val  = float(row["close"])
        vol_val= float(row["volume"])
        rows.append((dt_str, o_val, h_val, l_val, c_val, vol_val))
    return rows

def update_data_db(
    symbol: str,
    intervals: List[str],
    start_str: str,
    end_str: str,
    dropna_indicators: bool = False
) -> None:
    """
    1) fetch_ohlcv(...)로 [start_str ~ end_str] 구간, 각 interval의 klines 가져옴
    2) klines -> df_raw -> ohlcv 테이블에 저장
    3) preprocess_ohlcv_data(df_raw)로 보조지표 계산( calc_all_indicators )
    4) ohlcv_indicators 테이블에 삽입 (insert_indicators):
       - db_utils.py의 create_indicators_table_if_not_exists 내
         모든 지표 컬럼(MA, RSI, OBV 등)을 사전 정의해 둠

    Args:
      symbol (str): 예) "BTCUSDT"
      intervals (List[str]): ["1d","5m"] 등
      start_str (str): 예) "2019-01-01"
      end_str (str): 예) "2019-12-31"
      dropna_indicators (bool): 지표 계산 후 NaN이 생긴 행 제거할지 여부
    """
    print("[INFO] update_data_db start")
    print(f"  symbol={symbol}, intervals={intervals}, start={start_str}, end={end_str}")
    print(f"  dropna_indicators={dropna_indicators}")

    # 지표 테이블에 들어갈 컬럼(예: ma_5, ma_10, rsi_14, obv 등)
    # db_utils.py의 create_indicators_table_if_not_exists 함수가
    # 동일한 지표 컬럼들을 이미 정의해둠.
    # insert_indicators 호출 시 필요한 extra_cols
    all_indicator_cols = [
        # MA
        "ma_5","ma_10","ma_20","ma_50","ma_100","ma_200",
        # RSI
        "rsi_14","rsi_21","rsi_30",
        # OBV
        "obv","obv_sma_5","obv_sma_10","obv_sma_30","obv_sma_50","obv_sma_100",
        # Filter
        "filter_min_10","filter_max_10","filter_min_20","filter_max_20",
        # Support/Resistance
        "sr_min_10","sr_max_10","sr_min_20","sr_max_20",
        # Channel_Breakout
        "ch_min_14","ch_max_14","ch_min_20","ch_max_20"
    ]

    for interval in intervals:
        print(f"[INFO] Fetching interval='{interval}' from {start_str} to {end_str}")
        klines = fetch_ohlcv(symbol, interval, start_str, end_str)
        if not klines:
            print(f"[WARN] No klines returned for '{interval}'. Skip.")
            continue

        # klines -> df_raw
        df_raw = klines_to_dataframe(klines)
        if df_raw.empty:
            print(f"[WARN] df_raw empty for '{interval}'. Skip.")
            continue

        # 1) DB에 ohlcv 저장
        conn = get_connection()
        try:
            create_ohlcv_table_if_not_exists(conn)
            rows = dataframe_to_rows(df_raw)
            insert_ohlcv(conn, symbol, interval, rows)
        finally:
            conn.close()

        # 2) 지표 계산
        #    OHLC 중 NaN 있으면 예외 발생, dropna_indicators=True면 지표 NaN도 제거
        df_ind = preprocess_ohlcv_data(df_raw, dropna=dropna_indicators)
        if df_ind.empty:
            print(f"[WARN] After indicator calc, DF empty for '{interval}'. No indicator insertion.")
            continue

        # 3) DB(ohlcv_indicators)에 저장
        conn = get_connection()
        try:
            # 테이블 사전 생성(모든 지표 컬럼 포함)
            create_indicators_table_if_not_exists(conn)
            # insert_indicators => extra_cols=all_indicator_cols
            insert_indicators(conn, symbol, interval, df_ind, all_indicator_cols)
        finally:
            conn.close()

    print("[INFO] update_data_db complete.")


if __name__ == "__main__":
    """
    예시 실행:
      python update_data.py
    """
    SYMBOL = "BTCUSDT"
    INTERVALS = ["1d"]
    START_STR = "2019-01-01"
    END_STR   = "2024-12-31"
    DROPNA_INDICATORS = False

    print("[INFO] Running update_data_db with local vars:")
    print(f"  SYMBOL={SYMBOL}, INTERVALS={INTERVALS}, START={START_STR}, END={END_STR}")
    print(f"  dropna_indicators={DROPNA_INDICATORS}")

    update_data_db(
        symbol=SYMBOL,
        intervals=INTERVALS,
        start_str=START_STR,
        end_str=END_STR,
        dropna_indicators=DROPNA_INDICATORS
    )
