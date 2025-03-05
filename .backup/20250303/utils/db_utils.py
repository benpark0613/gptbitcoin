# gptbitcoin/utils/db_utils.py
# 구글 스타일, 최소한의 한글 주석

import os
import sqlite3
from datetime import datetime
from typing import List

import pandas as pd

from config.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """
    SQLite DB 커넥션을 생성하여 반환한다.
    DB_PATH는 config.py에서 설정하며, 폴더가 없으면 생성한다.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def create_ohlcv_table_if_not_exists(conn: sqlite3.Connection) -> None:
    """
    원본 OHLCV를 저장하는 테이블(ohlcv)을 생성한다.
    (symbol, interval, datetime_utc)를 PK로 설정.
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
    );
    """
    conn.execute(sql)
    conn.commit()


def delete_ohlcv(conn: sqlite3.Connection, symbol: str, interval: str) -> None:
    """
    ohlcv 테이블에서 주어진 (symbol, interval)의 기존 레코드를 전부 삭제한다.
    """
    sql = """
    DELETE FROM ohlcv
     WHERE symbol=? AND interval=?;
    """
    conn.execute(sql, (symbol, interval))
    conn.commit()


def insert_ohlcv(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    rows: List[tuple]
) -> None:
    """
    원본 OHLCV 데이터를 ohlcv 테이블에 INSERT OR REPLACE한다.

    Args:
        conn: DB 커넥션
        symbol: 예) "BTCUSDT"
        interval: 예) "1d"
        rows: (datetime_utc, open, high, low, close, volume) 튜플 리스트
    """
    create_ohlcv_table_if_not_exists(conn)
    sql = """
    INSERT OR REPLACE INTO ohlcv
    (symbol, interval, datetime_utc, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    data_list = [
        (symbol, interval, r[0], r[1], r[2], r[3], r[4], r[5])
        for r in rows
    ]
    conn.executemany(sql, data_list)
    conn.commit()


def create_indicators_table_if_not_exists(conn: sqlite3.Connection) -> None:
    """
    보조지표(ohlcv_indicators) 테이블을 생성한다.
    (symbol, interval, datetime_utc)를 PK로 설정.
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

        ma_5 REAL,
        ma_10 REAL,
        ma_20 REAL,
        ma_50 REAL,
        ma_100 REAL,
        ma_200 REAL,

        rsi_14 REAL,
        rsi_21 REAL,
        rsi_30 REAL,

        obv REAL,
        obv_sma_5 REAL,
        obv_sma_10 REAL,
        obv_sma_30 REAL,
        obv_sma_50 REAL,
        obv_sma_100 REAL,

        filter_min_10 REAL,
        filter_max_10 REAL,
        filter_min_20 REAL,
        filter_max_20 REAL,

        sr_min_10 REAL,
        sr_max_10 REAL,
        sr_min_20 REAL,
        sr_max_20 REAL,

        ch_min_14 REAL,
        ch_max_14 REAL,
        ch_min_20 REAL,
        ch_max_20 REAL,

        PRIMARY KEY (symbol, interval, datetime_utc)
    );
    """
    conn.execute(sql)
    conn.commit()


def delete_indicators(conn: sqlite3.Connection, symbol: str, interval: str) -> None:
    """
    ohlcv_indicators 테이블에서 주어진 (symbol, interval)의 기존 레코드를 삭제한다.
    """
    sql = """
    DELETE FROM ohlcv_indicators
     WHERE symbol=? AND interval=?;
    """
    conn.execute(sql, (symbol, interval))
    conn.commit()


def insert_indicators(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    df: pd.DataFrame,
    extra_cols: List[str]
) -> None:
    """
    df를 ohlcv_indicators 테이블에 INSERT OR REPLACE한다.

    Args:
        conn: DB 커넥션
        symbol: 예) "BTCUSDT"
        interval: 예) "1d"
        df: 데이터프레임 (기본 열과 extra_cols 포함)
        extra_cols: 지표 컬럼 목록
    """
    create_indicators_table_if_not_exists(conn)

    base_cols = ["datetime_utc", "open", "high", "low", "close", "volume"]
    all_cols = base_cols + extra_cols

    col_str = ", ".join(all_cols)
    placeholders = ", ".join(["?"] * len(all_cols))
    sql = f"""
    INSERT OR REPLACE INTO ohlcv_indicators
    (symbol, interval, {col_str})
    VALUES (?, ?, {placeholders});
    """

    rows_to_insert = []
    for _, row in df.iterrows():
        # datetime_utc를 문자열로 저장할 때
        dt_str = row["datetime_utc"]
        if isinstance(dt_str, (datetime, pd.Timestamp)):
            dt_str = dt_str.strftime("%Y-%m-%d %H:%M:%S")

        vals = []
        for c in all_cols:
            vals.append(row.get(c, None))
        rows_to_insert.append((symbol, interval, *vals))

    conn.executemany(sql, rows_to_insert)
    conn.commit()


def load_indicators_from_db(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    from_dt: str,
    to_dt: str
) -> pd.DataFrame:
    """
    ohlcv_indicators 테이블에서 (symbol, interval)과 기간 [from_dt ~ to_dt]
    데이터를 SELECT하여 DataFrame으로 반환한다.

    Args:
        conn: DB 커넥션
        symbol: 예) "BTCUSDT"
        interval: 예) "1d"
        from_dt: "YYYY-MM-DD HH:MM:SS" 형태 문자열
        to_dt:   "YYYY-MM-DD HH:MM:SS" 형태 문자열

    Returns:
        pd.DataFrame: 조회된 지표 데이터
    """
    create_indicators_table_if_not_exists(conn)
    sql = """
    SELECT
        datetime_utc, open, high, low, close, volume,

        ma_5, ma_10, ma_20, ma_50, ma_100, ma_200,
        rsi_14, rsi_21, rsi_30,
        obv, obv_sma_5, obv_sma_10, obv_sma_30, obv_sma_50, obv_sma_100,

        filter_min_10, filter_max_10, filter_min_20, filter_max_20,
        sr_min_10, sr_max_10, sr_min_20, sr_max_20,
        ch_min_14, ch_max_14, ch_min_20, ch_max_20

    FROM ohlcv_indicators
    WHERE symbol=? AND interval=?
      AND datetime_utc >= ? AND datetime_utc <= ?
    ORDER BY datetime_utc ASC;
    """
    rows = conn.execute(sql, (symbol, interval, from_dt, to_dt)).fetchall()
    if not rows:
        return pd.DataFrame()

    # 컬럼명 추출
    col_names = [desc[0] for desc in conn.execute(sql, (symbol, interval, from_dt, to_dt)).description]
    df = pd.DataFrame(rows, columns=col_names)

    # 문자열 datetime -> pd.Timestamp
    if "datetime_utc" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce")

    # 나머지 수치형 변환
    for c in df.columns:
        if c not in ("datetime_utc",):
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df.dropna(subset=["datetime_utc"], inplace=True)
    df.sort_values("datetime_utc", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df
