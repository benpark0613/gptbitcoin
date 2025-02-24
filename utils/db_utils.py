# gptbitcoin/utils/db_utils.py
# 구글 스타일, 최소한의 한글 주석
# DB 접근/테이블 생성/데이터 삽입 관련 함수들을 이 모듈에서 관리한다.
# 1) 원본 OHLCV(ohlcv 테이블)
# 2) 보조지표(ohlcv_indicators 테이블) 전용 스키마 포함

import os
import sqlite3
from typing import List
import pandas as pd

from config.config import DB_PATH

def get_connection() -> sqlite3.Connection:
    """
    SQLite DB 커넥션을 생성/반환. DB_PATH는 config.py에서 설정.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    return conn

def create_ohlcv_table_if_not_exists(conn: sqlite3.Connection) -> None:
    """
    원본 OHLCV 데이터를 저장할 테이블(ohlcv).
    (symbol, interval, datetime_utc)를 PK로 잡아 중복을 막는다.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS ohlcv (
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        datetime_utc TEXT NOT NULL,

        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,

        PRIMARY KEY (symbol, interval, datetime_utc)
    )
    """
    conn.execute(sql)
    conn.commit()

def insert_ohlcv(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    rows: List[tuple]
) -> None:
    """
    ohlcv 테이블에 (datetime_utc, open, high, low, close, volume) 를
    INSERT OR REPLACE. rows 예:
    [
      ("2019-01-01 00:00:00", 3500.0, 3600.0, 3450.0, 3550.0, 1234.0),
      ...
    ]
    """
    create_ohlcv_table_if_not_exists(conn)

    sql = """
    INSERT OR REPLACE INTO ohlcv
    (symbol, interval, datetime_utc, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    data_list = []
    for (dt_str, o_val, h_val, l_val, c_val, vol_val) in rows:
        data_list.append((symbol, interval, dt_str, o_val, h_val, l_val, c_val, vol_val))

    conn.executemany(sql, data_list)
    conn.commit()

def create_indicators_table_if_not_exists(conn: sqlite3.Connection) -> None:
    """
    보조지표(ohlcv_indicators) 테이블.
    calc_all_indicators에서 생성되는 대표 컬럼들을 모두 정의한다.
    예: MA, RSI, OBV, Filter, Support_Resistance, Channel_Breakout
    """
    sql = """
    CREATE TABLE IF NOT EXISTS ohlcv_indicators (
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        datetime_utc TEXT NOT NULL,

        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,

        -- MA
        ma_5 REAL,
        ma_10 REAL,
        ma_20 REAL,
        ma_50 REAL,
        ma_100 REAL,
        ma_200 REAL,

        -- RSI
        rsi_14 REAL,
        rsi_21 REAL,
        rsi_30 REAL,

        -- OBV
        obv REAL,
        obv_sma_5 REAL,
        obv_sma_10 REAL,
        obv_sma_30 REAL,
        obv_sma_50 REAL,
        obv_sma_100 REAL,

        -- Filter
        filter_min_10 REAL,
        filter_max_10 REAL,
        filter_min_20 REAL,
        filter_max_20 REAL,

        -- Support_Resistance
        sr_min_10 REAL,
        sr_max_10 REAL,
        sr_min_20 REAL,
        sr_max_20 REAL,

        -- Channel_Breakout
        ch_min_14 REAL,
        ch_max_14 REAL,
        ch_min_20 REAL,
        ch_max_20 REAL,

        PRIMARY KEY (symbol, interval, datetime_utc)
    )
    """
    conn.execute(sql)
    conn.commit()

def insert_indicators(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    df: pd.DataFrame,
    extra_cols: List[str]
) -> None:
    """
    ohlcv_indicators 테이블에 df 안에 있는 지표 컬럼들을 INSERT OR REPLACE.
    extra_cols에 명시된 지표 컬럼을 포함해, open/high/low/close/volume도 저장.

    df에는 최소:
      - 'datetime_utc','open','high','low','close','volume' plus extra_cols
    """
    create_indicators_table_if_not_exists(conn)

    # 베이스 컬럼
    base_cols = ["datetime_utc", "open", "high", "low", "close", "volume"]
    all_cols = base_cols + extra_cols  # DB에 넣을 지표컬럼

    # DB에 들어갈 컬럼 placeholders
    placeholders = ", ".join(["?"] * len(all_cols))
    col_names_str = ", ".join(all_cols)
    insert_sql = f"""
    INSERT OR REPLACE INTO ohlcv_indicators
    (symbol, interval, {col_names_str})
    VALUES (?, ?, {placeholders})
    """

    rows_to_insert = []
    for _, row in df.iterrows():
        dt_str = row["datetime_utc"]
        # df[col]가 없는 경우 None
        vals = [row.get(col, None) for col in all_cols]
        # (symbol, interval) + 나머지
        rows_to_insert.append((symbol, interval) + tuple(vals))

    conn.executemany(insert_sql, rows_to_insert)
    conn.commit()
