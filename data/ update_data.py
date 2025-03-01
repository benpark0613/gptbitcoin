# gptbitcoin/data/update_data.py
# 구글 스타일, 최소한의 한글 주석
#
# 이 모듈은 다음 과정을 수행한다:
#  1) fetch_ohlcv(...)로 바이낸스 선물 OHLCV 데이터를 [start_str ~ end_str] 구간, 여러 interval에 대해 가져옴
#  2) (요구사항) 해당 intervals의 [BOUNDARY_DATE 이후] 데이터를 전부 삭제(ohlcv, ohlcv_indicators)한 뒤
#  3) 받아온 [start_str ~ end_str] 데이터를 ohlcv 테이블에 (INSERT OR REPLACE)
#  4) preprocess_ohlcv_data(...)로 보조지표 계산
#  5) ohlcv_indicators 테이블에 calc_all_indicators에서 생성된 컬럼들(예: ma_5, rsi_14, obv...)을 저장
#
# db_utils.py에서 테이블 생성/저장 로직, preprocess.py에서 지표 계산 로직을 관리한다.
# 여기서는 'update_data_db' 함수 하나로 전 과정을 처리하되,
# 'intervals'마다 BOUNDARY_DATE 이후 데이터를 먼저 DELETE 처리한 후 새로 INSERT.

import os
from datetime import datetime
from typing import List

import pandas as pd
import pytz

from data.fetch_data import fetch_ohlcv
from data.preprocess import preprocess_ohlcv_data
from utils.db_utils import (
    get_connection,
    create_ohlcv_table_if_not_exists,
    insert_ohlcv,
    create_indicators_table_if_not_exists,
    insert_indicators
)

UTC = pytz.utc

BOUNDARY_DATE_STR = "2025-01-01"  # 예: 2025-01-01 이후는 삭제

def _delete_data_after_boundary(conn, symbol: str, interval: str) -> None:
    """
    ohlcv, ohlcv_indicators 테이블에서
    'interval'='interval' AND 'symbol'='symbol' 이며
    datetime_utc >= BOUNDARY_DATE_STR 인 행 삭제
    """
    boundary_dt = BOUNDARY_DATE_STR + " 00:00:00"

    # ohlcv
    sql_ohlcv = """
    DELETE FROM ohlcv
     WHERE symbol=? AND interval=?
       AND datetime_utc >= ?
    """
    conn.execute(sql_ohlcv, (symbol, interval, boundary_dt))

    # indicators
    sql_inds = """
    DELETE FROM ohlcv_indicators
     WHERE symbol=? AND interval=?
       AND datetime_utc >= ?
    """
    conn.execute(sql_inds, (symbol, interval, boundary_dt))
    conn.commit()

def klines_to_dataframe(klines: list) -> pd.DataFrame:
    """
    바이낸스 klines(list) -> Pandas DataFrame 변환
    columns = ["datetime_utc","open","high","low","close","volume"]
    """
    df_list = []
    for row in klines:
        open_time_ms = int(row[0])
        dt_utc = datetime.utcfromtimestamp(open_time_ms / 1000.0).replace(tzinfo=UTC)
        dt_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

        o_val = float(row[1])
        h_val = float(row[2])
        l_val = float(row[3])
        c_val = float(row[4])
        # volume을 소수점 첫째자리에서 반올림(=정수 반올림)
        vol_val = round(float(row[5]))

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
    1) intervals 별로, DB에 있는 BOUNDARY_DATE 이후 데이터(o,l,h,c,v,지표) 전부 DELETE
    2) [start_str ~ end_str] 구간 klines fetch → df_raw
    3) df_raw -> ohlcv 테이블 INSERT OR REPLACE
    4) preprocess_ohlcv_data(df_raw) 지표 계산
    5) ohlcv_indicators 테이블에 해당 지표 삽입
    """
    print("[INFO] update_data_db start")
    print(f"  symbol={symbol}, intervals={intervals}, start={start_str}, end={end_str}")
    print(f"  dropna_indicators={dropna_indicators}")
    print(f"  BOUNDARY_DATE_STR={BOUNDARY_DATE_STR} 이후 데이터는 삭제합니다.")

    # 지표 테이블에 들어갈 컬럼(예: ma_5, ma_10, rsi_14, obv 등)
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
        print(f"[INFO] Interval='{interval}': 1) Delete BOUNDARY_DATE 이후 data from DB.")
        conn = get_connection()
        try:
            _delete_data_after_boundary(conn, symbol, interval)
        finally:
            conn.close()

        print(f"[INFO] Interval='{interval}': 2) Fetch klines: {start_str}~{end_str}")
        klines = fetch_ohlcv(symbol, interval, start_str, end_str)
        if not klines:
            print(f"[WARN] No klines returned for '{interval}'. Skip.")
            continue

        df_raw = klines_to_dataframe(klines)
        if df_raw.empty:
            print(f"[WARN] df_raw empty for '{interval}'. Skip.")
            continue

        print(f"[INFO] Interval='{interval}': Insert to ohlcv table.")
        conn = get_connection()
        try:
            create_ohlcv_table_if_not_exists(conn)
            rows = dataframe_to_rows(df_raw)
            insert_ohlcv(conn, symbol, interval, rows)
        finally:
            conn.close()

        print(f"[INFO] Interval='{interval}': Calculate indicators.")
        df_ind = preprocess_ohlcv_data(df_raw, dropna=dropna_indicators)
        if df_ind.empty:
            print(f"[WARN] After indicator calc, DF empty for '{interval}'. Skip inserting indicators.")
            continue

        print(f"[INFO] Interval='{interval}': Insert to ohlcv_indicators.")
        conn = get_connection()
        try:
            create_indicators_table_if_not_exists(conn)
            insert_indicators(conn, symbol, interval, df_ind, all_indicator_cols)
        finally:
            conn.close()

    print("[INFO] update_data_db complete.")


def _delete_data_after_boundary(conn, symbol: str, interval: str) -> None:
    """
    BOUNDARY_DATE 이후의 데이터(ohlcv, ohlcv_indicators) 전부 삭제
    """
    boundary_dt_str = BOUNDARY_DATE_STR + " 00:00:00"

    sql_1 = """
    DELETE FROM ohlcv
     WHERE symbol=? AND interval=?
       AND datetime_utc >= ?
    """
    conn.execute(sql_1, (symbol, interval, boundary_dt_str))

    sql_2 = """
    DELETE FROM ohlcv_indicators
     WHERE symbol=? AND interval=?
       AND datetime_utc >= ?
    """
    conn.execute(sql_2, (symbol, interval, boundary_dt_str))
    conn.commit()

if __name__ == "__main__":
    """
    예시 실행:
      python update_data.py
    """
    SYMBOL = "BTCUSDT"
    INTERVALS = ["1d"]  # 사용자가 원하는 interval
    START_STR = "2019-01-01"
    END_STR   = "2024-12-31"
    DROPNA_INDICATORS = False

    print("[INFO] Running update_data_db with local vars:")
    print(f"  SYMBOL={SYMBOL}, INTERVALS={INTERVALS}, START={START_STR}, END={END_STR}")
    print(f"  dropna_indicators={DROPNA_INDICATORS}")
    print(f"  (Delete all 1d data >= {BOUNDARY_DATE_STR})")

    update_data_db(
        symbol=SYMBOL,
        intervals=INTERVALS,
        start_str=START_STR,
        end_str=END_STR,
        dropna_indicators=DROPNA_INDICATORS
    )