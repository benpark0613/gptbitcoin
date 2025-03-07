# gptbitcoin/main_best.py
# 구글 스타일 docstring, 최소한의 한글 주석
# 특정 combo_info(used_indicators)를 이용해 단일 백테스트.
# main.py와 동일한 방식으로 DB 커버리지, 워밍업, 지표계산을 수행.

import sys
import os
import datetime
import pytz
import pandas as pd
from datetime import timedelta

try:
    from config.config import (
        SYMBOL,
        START_DATE,
        END_DATE,
        DB_BOUNDARY_DATE,
        EXCHANGE_OPEN_DATE,
        LOG_LEVEL,
        RESULTS_DIR,
        INDICATOR_CONFIG
    )
except ImportError:
    print("[main_best.py] config.py import 실패")
    sys.exit(1)

# DB, 전처리
try:
    from utils.db_utils import connect_db, init_db
    from data.update_data import update_data_db
    from data.preprocess import clean_ohlcv, merge_old_recent
except ImportError:
    print("[main_best.py] DB/전처리 모듈 import 에러")
    sys.exit(1)

# 보조지표, 워밍업
try:
    from indicators.indicators import calc_all_indicators
    from utils.indicator_utils import get_required_warmup_bars
except ImportError:
    print("[main_best.py] indicators/indicator_utils import 에러")
    sys.exit(1)

# 단일 콤보 백테스트
try:
    from backtest.run_best import run_best_single
except ImportError:
    print("[main_best.py] run_best.py import 에러")
    sys.exit(1)

def _get_time_delta_for_tf(tf_str: str) -> datetime.timedelta:
    """
    타임프레임 문자열("1d","4h","15m")을 timedelta로 변환.
    """
    tf_lower = tf_str.lower()
    if tf_lower.endswith("d"):
        d_val = int(tf_lower.replace("d", "")) if tf_lower.replace("d", "").isdigit() else 1
        return timedelta(days=d_val)
    elif tf_lower.endswith("h"):
        h_val = int(tf_lower.replace("h", "")) if tf_lower.replace("h", "").isdigit() else 1
        return timedelta(hours=h_val)
    elif tf_lower.endswith("m"):
        m_val = int(tf_lower.replace("m", "")) if tf_lower.replace("m", "").isdigit() else 1
        return timedelta(minutes=m_val)
    else:
        return timedelta(days=1)


def ensure_recent_coverage(symbol: str, timeframe: str, start_date: str, end_date: str) -> None:
    """
    main.py와 동일한 로직으로, (start_date~end_date) 구간의 recent_data가 충분한지 확인.
    부족하면 update_data_db(recent)로 갱신.
    """
    from config.config import DB_PATH, DB_BOUNDARY_DATE
    import sqlite3

    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_start = datetime.datetime.strptime(start_date, dt_format)
    start_utc = utc.localize(naive_start)
    start_ms = int(start_utc.timestamp() * 1000)

    naive_end = datetime.datetime.strptime(end_date, dt_format)
    end_utc = utc.localize(naive_end)
    end_ms = int(end_utc.timestamp() * 1000)

    naive_bound = datetime.datetime.strptime(DB_BOUNDARY_DATE, dt_format)
    bound_utc = utc.localize(naive_bound)
    boundary_ts = int(bound_utc.timestamp() * 1000)

    # end_date <= boundary_date => recent_data 필요 없음
    if end_ms <= boundary_ts:
        print(f"[main_best.py] recent_data 불필요 (end_date <= boundary_date)")
        return

    try:
        conn = connect_db(DB_PATH)
        init_db(conn)
        sql = """
            SELECT COUNT(*) AS c
              FROM recent_data
             WHERE symbol=?
               AND timeframe=?
               AND open_time >= ?
               AND open_time <= ?
        """
        df_count = pd.read_sql_query(sql, conn, params=(symbol, timeframe, start_ms, end_ms))
        conn.close()
    except sqlite3.Error as e:
        print(f"[main_best.py] ensure_recent_coverage DB 에러: {e}")
        return

    cnt_val = df_count.iloc[0]["c"]
    if cnt_val == 0:
        print(f"[main_best.py] recent_data {timeframe} 구간 {start_date}~{end_date} 없음 => update_data_db(recent)")
        try:
            update_data_db(symbol, timeframe, start_date, end_date, update_mode="recent")
        except Exception as err:
            print(f"[main_best.py] update_data_db(recent) 실패: {err}")
    else:
        print(f"[main_best.py] recent_data 구간 일부 존재: {cnt_val}봉")


def select_ohlcv(
    conn,
    table_name: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int
) -> pd.DataFrame:
    """
    DB에서 (symbol, timeframe, open_time in [start_ms, end_ms]) 범위 SELECT
    """
    sql = f"""
        SELECT symbol, timeframe, timestamp_kst, open_time,
               open, high, low, close, volume
          FROM {table_name}
         WHERE symbol=?
           AND timeframe=?
           AND open_time >= ?
           AND open_time <= ?
         ORDER BY open_time ASC
    """
    df = pd.read_sql_query(sql, conn, params=(symbol, timeframe, start_ms, end_ms))
    return df


def load_and_preprocess_data(
    symbol: str,
    timeframe: str,
    start_date: str,
    boundary_date: str,
    end_date: str,
    warmup_bars: int
) -> pd.DataFrame:
    """
    main.py와 동일하게 DB에서 old_data+recent_data를 합쳐 전처리한다.
    (clean_ohlcv, merge_old_recent)
    """
    from config.config import DB_PATH, EXCHANGE_OPEN_DATE
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_start = datetime.datetime.strptime(start_date, dt_format)
    main_start_dt = utc.localize(naive_start)

    naive_boundary = datetime.datetime.strptime(boundary_date, dt_format)
    boundary_dt = utc.localize(naive_boundary)

    naive_end = datetime.datetime.strptime(end_date, dt_format)
    end_dt = utc.localize(naive_end)

    naive_exch_open = datetime.datetime.strptime(EXCHANGE_OPEN_DATE, dt_format)
    exch_open_utc = utc.localize(naive_exch_open)

    delta_per_bar = _get_time_delta_for_tf(timeframe)
    warmup_delta = delta_per_bar * warmup_bars

    warmup_start_dt = main_start_dt - warmup_delta
    if warmup_start_dt < exch_open_utc:
        warmup_start_dt = exch_open_utc

    warmup_start_ms = int(warmup_start_dt.timestamp() * 1000)
    boundary_ms = int(boundary_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    conn = connect_db(DB_PATH)
    init_db(conn)

    df_old = select_ohlcv(conn, "old_data", symbol, timeframe, warmup_start_ms, boundary_ms - 1)
    df_recent = select_ohlcv(conn, "recent_data", symbol, timeframe, boundary_ms, end_ms)
    conn.close()

    df_old = clean_ohlcv(df_old)
    df_recent = clean_ohlcv(df_recent)
    merged = merge_old_recent(df_old, df_recent)
    return merged


def main():
    """
    main.py와 유사하게 DB 데이터 로드/전처리 후,
    특정 combo_info만 run_best_single로 백테스트.
    """

    print(f"[main_best.py] Start - SYMBOL={SYMBOL}, LOG_LEVEL={LOG_LEVEL}")

    # combo_info 예시 (main.py에서 만든 used_indicators JSON을 붙여넣기)
    combo_info = {
        "timeframe": "15m",
        "combo_params": [
            {"type": "MA", "short_period": 20, "long_period": 50, "band_filter": 0.0},
            {"type": "RSI", "length": 30, "overbought": 80, "oversold": 30}
        ]
    }

    timeframe = combo_info["timeframe"]

    # 1) coverage
    ensure_recent_coverage(SYMBOL, timeframe, START_DATE, END_DATE)

    # 2) warmup_bars
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[main_best.py] warmup_bars={warmup_bars}")

    # 3) DB에서 구간 로드 + 전처리
    df_merged = load_and_preprocess_data(
        symbol=SYMBOL,
        timeframe=timeframe,
        start_date=START_DATE,
        boundary_date=DB_BOUNDARY_DATE,
        end_date=END_DATE,
        warmup_bars=warmup_bars
    )
    if df_merged.empty:
        print("[main_best.py] df_merged 비어 있음. 종료.")
        return

    print(f"[main_best.py] merged rows={len(df_merged)}")

    # 4) 지표 계산
    df_ind = calc_all_indicators(df_merged, INDICATOR_CONFIG)

    # 5) 백테스트 범위 필터 (START_DATE~END_DATE)
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_s = datetime.datetime.strptime(START_DATE, dt_format)
    s_utc = utc.localize(naive_s)
    s_ms = int(s_utc.timestamp() * 1000)

    naive_e = datetime.datetime.strptime(END_DATE, dt_format)
    e_utc = utc.localize(naive_e)
    e_ms = int(e_utc.timestamp() * 1000)

    df_test = df_ind[(df_ind["open_time"] >= s_ms) & (df_ind["open_time"] <= e_ms)].copy()
    df_test.reset_index(drop=True, inplace=True)

    if df_test.empty:
        print("[main_best.py] df_test 비어 있음. 종료.")
        return

    # 6) 단일 콤보 백테스트
    run_best_single(df_test, combo_info)

    print("[main_best.py] 완료.")


if __name__ == "__main__":
    main()
