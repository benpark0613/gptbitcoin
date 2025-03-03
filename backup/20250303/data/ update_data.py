# gptbitcoin/data/update_data.py
# 구글 스타일, 최소한의 한글 주석
#
# 이 모듈은 다음 과정을 수행한다:
#   1) 사용자가 지정한 SYMBOL, INTERVALS에 해당하는 DB의 기존 데이터 전부 삭제
#   2) [start_str ~ end_str] 구간 데이터를 바이낸스 API에서 가져와 ohlcv 테이블에 저장
#   3) 가져온 df에 대해 지표 계산 후 ohlcv_indicators 테이블에 저장
#   (시간을 UTC+9, 즉 KST 기준으로 저장)

from datetime import datetime
from typing import List

import pandas as pd
from pytz import timezone

from data.fetch_data import fetch_ohlcv
from data.preprocess import preprocess_ohlcv_data
from utils.db_utils import (
    get_connection,
    create_ohlcv_table_if_not_exists,
    create_indicators_table_if_not_exists,
    delete_ohlcv,
    delete_indicators,
    insert_ohlcv,
    insert_indicators
)

# KST 설정
KST = timezone("Asia/Seoul")


def klines_to_dataframe(klines: list) -> pd.DataFrame:
    """
    바이낸스 klines(list)를 Pandas DataFrame으로 변환한다.
    이때 시간을 UTC+9 (KST)로 변환해
    ["datetime_utc","open","high","low","close","volume"] 열을 갖는 df를 반환한다.
    """
    df_list = []
    for row in klines:
        open_time_ms = int(row[0])
        # KST 로 변환
        dt_kst = datetime.fromtimestamp(open_time_ms / 1000.0, tz=KST)
        dt_str = dt_kst.strftime("%Y-%m-%d %H:%M:%S")

        o_val = float(row[1])
        h_val = float(row[2])
        l_val = float(row[3])
        c_val = float(row[4])
        # 거래량 소수점 반올림
        vol_val = round(float(row[5]))

        df_list.append([dt_str, o_val, h_val, l_val, c_val, vol_val])

    return pd.DataFrame(df_list, columns=["datetime_utc","open","high","low","close","volume"])


def dataframe_to_rows(df: pd.DataFrame) -> list:
    """
    insert_ohlcv(...) 함수에 전달할 형태로 변환한다.
    (datetime_utc, open, high, low, close, volume) 튜플 리스트 생성.
    """
    rows = []
    for _, row in df.iterrows():
        dt_str = row["datetime_utc"]
        o_val = float(row["open"])
        h_val = float(row["high"])
        l_val = float(row["low"])
        c_val = float(row["close"])
        vol_val = float(row["volume"])
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
    DB에 (symbol, interval)별로 데이터를 업데이트:
      1) 기존 데이터 삭제
      2) [start_str ~ end_str] 구간 klines fetch
      3) ohlcv 테이블 INSERT
      4) 지표 계산 후 ohlcv_indicators 테이블 INSERT

    Args:
        symbol: 예) "BTCUSDT"
        intervals: 예) ["1d","4h","1h","15m"]
        start_str: "YYYY-MM-DD HH:MM:SS"
        end_str:   "YYYY-MM-DD HH:MM:SS"
        dropna_indicators: 지표 계산 후 NaN 행 제거 여부
    """
    print("[INFO] update_data_db start")
    print(f"  symbol={symbol}, intervals={intervals}, start={start_str}, end={end_str}")
    print(f"  dropna_indicators={dropna_indicators}")

    # DB에 저장할 지표 컬럼
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
        print(f"[INFO] Interval='{interval}': start={start_str}, end={end_str}")
        # 1) 테이블 생성 + 기존 데이터 삭제
        conn = get_connection()
        try:
            create_ohlcv_table_if_not_exists(conn)
            create_indicators_table_if_not_exists(conn)
            delete_ohlcv(conn, symbol, interval)
            delete_indicators(conn, symbol, interval)
        finally:
            conn.close()

        # 2) klines fetch
        klines = fetch_ohlcv(symbol, interval, start_str, end_str)
        if not klines:
            print(f"[WARN] No klines returned for interval='{interval}'. Skip.")
            continue

        df_raw = klines_to_dataframe(klines)
        if df_raw.empty:
            print(f"[WARN] df_raw empty for interval='{interval}'. Skip.")
            continue

        # 3) ohlcv 테이블에 INSERT
        conn = get_connection()
        try:
            rows = dataframe_to_rows(df_raw)
            insert_ohlcv(conn, symbol, interval, rows)
        finally:
            conn.close()

        # 4) 지표 계산 -> ohlcv_indicators에 INSERT
        df_ind = preprocess_ohlcv_data(df_raw, dropna=dropna_indicators)
        if df_ind.empty:
            print(f"[WARN] After indicator calc, df_ind is empty for '{interval}'. Skip.")
            continue

        conn = get_connection()
        try:
            insert_indicators(conn, symbol, interval, df_ind, all_indicator_cols)
        finally:
            conn.close()

    print("[INFO] update_data_db complete.")


if __name__ == "__main__":
    SYMBOL = "BTCUSDT"
    INTERVALS = ["1d", "4h", "1h", "15m"]
    START_STR = "2019-01-01 00:00:00"
    END_STR   = "2024-12-31 23:59:59"
    DROPNA_INDICATORS = False

    update_data_db(
        symbol=SYMBOL,
        intervals=INTERVALS,
        start_str=START_STR,
        end_str=END_STR,
        dropna_indicators=DROPNA_INDICATORS
    )
