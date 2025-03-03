# gptbitcoin/data/db_manager.py
# DB 연결, 테이블 생성, CRUD 함수
# Binance에서 받아온 원본 데이터를 그대로 저장합니다.
# 원본 Binance 시간은 밀리초(ms) 단위의 open_time_ms 컬럼에 저장하고,
# KST로 변환한 시간은 timestamp_kst 컬럼에 저장합니다.
# PRIMARY KEY는 (symbol, timeframe, open_time_ms)입니다.

import datetime
import sqlite3

import pandas as pd

from config.config import DB_PATH, DB_BOUNDARY_DATE

# 타임존 객체
_UTC = datetime.timezone.utc
_TZ_KST = datetime.timezone(datetime.timedelta(hours=9))  # Asia/Seoul


def _get_boundary_ms() -> int:
    """
    DB_BOUNDARY_DATE (KST 기준, 예: "2025-01-01 00:00:00")를
    UTC 시간으로 변환한 후 epoch 밀리초(ms) 단위로 반환한다.
    """
    # DB_BOUNDARY_DATE는 KST 기준 문자열
    boundary_kst = datetime.datetime.fromisoformat(DB_BOUNDARY_DATE).replace(tzinfo=_TZ_KST)
    boundary_utc = boundary_kst.astimezone(_UTC)
    return int(boundary_utc.timestamp() * 1000)


def _choose_table_by_open_time_ms(open_time_ms: int) -> str:
    """
    open_time_ms 값(원본 Binance 시간, UTC ms)이 DB_BOUNDARY_DATE에 해당하는 경계보다 작은 경우
    "old_data", 크거나 같으면 "recent_data" 테이블로 결정한다.
    """
    boundary_ms = _get_boundary_ms()
    return "old_data" if open_time_ms < boundary_ms else "recent_data"


def get_db_connection() -> sqlite3.Connection:
    """DB에 연결하고, 테이블이 없으면 자동 생성한다."""
    conn = sqlite3.connect(DB_PATH)
    _create_tables_if_not_exists(conn)
    return conn


def _create_tables_if_not_exists(conn: sqlite3.Connection) -> None:
    """old_data와 recent_data 테이블을 생성한다. open_time_ms는 원본 Binance 시간(ms)로 저장."""
    sql_old = """
        CREATE TABLE IF NOT EXISTS old_data (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time_ms INTEGER NOT NULL,  -- Binance에서 받아온 원본 시간 (ms)
            timestamp_kst TEXT NOT NULL,      -- KST로 변환한 시간
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timeframe, open_time_ms)
        )
    """
    sql_recent = """
        CREATE TABLE IF NOT EXISTS recent_data (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time_ms INTEGER NOT NULL,  -- Binance 원본 시간 (ms)
            timestamp_kst TEXT NOT NULL,      -- KST로 변환한 시간
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, timeframe, open_time_ms)
        )
    """
    cur = conn.cursor()
    cur.execute(sql_old)
    cur.execute(sql_recent)
    conn.commit()


def insert_ohlcv_batch(df: pd.DataFrame) -> None:
    """
    DataFrame의 각 행을 DB에 삽입한다.
    DataFrame은 다음 컬럼을 포함해야 합니다:
      ['symbol', 'timeframe', 'open_time_ms', 'timestamp_kst', 'open', 'high', 'low', 'close', 'volume']
    """
    if df.empty:
        return
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        sql_template = """
            INSERT OR REPLACE INTO {table}
            (symbol, timeframe, open_time_ms, timestamp_kst, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for row in df.itertuples(index=False):
            table_name = _choose_table_by_open_time_ms(row.open_time_ms)
            cur.execute(sql_template.format(table=table_name), (
                row.symbol,
                row.timeframe,
                row.open_time_ms,
                row.timestamp_kst,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume
            ))
        conn.commit()
    finally:
        conn.close()


def delete_ohlcv_data(symbol: str, timeframe: str, start_utc_str: str, end_utc_str: str) -> None:
    """
    주어진 (symbol, timeframe)와 UTC 기준 시작 및 종료 시간(문자열, "YYYY-MM-DD HH:MM:SS")에 대해,
    해당하는 open_time_ms 값 범위를 계산하여 데이터를 삭제한다.

    입력된 시간은 UTC 기준입니다.
    """
    # 변환: UTC 문자열 -> datetime -> epoch ms
    start_dt = datetime.datetime.fromisoformat(start_utc_str).replace(tzinfo=_UTC)
    end_dt = datetime.datetime.fromisoformat(end_utc_str).replace(tzinfo=_UTC)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    boundary_ms = _get_boundary_ms()

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # 삭제: old_data
        if start_ms < boundary_ms:
            # 삭제 범위: [start_ms, min(end_ms, boundary_ms))
            upper_ms = min(end_ms, boundary_ms)
            sql_old = """
                DELETE FROM old_data
                WHERE symbol = ?
                  AND timeframe = ?
                  AND open_time_ms >= ?
                  AND open_time_ms < ?
            """
            cur.execute(sql_old, (symbol, timeframe, start_ms, upper_ms))
        # 삭제: recent_data
        if end_ms >= boundary_ms:
            lower_ms = max(start_ms, boundary_ms)
            sql_recent = """
                DELETE FROM recent_data
                WHERE symbol = ?
                  AND timeframe = ?
                  AND open_time_ms >= ?
                  AND open_time_ms <= ?
            """
            cur.execute(sql_recent, (symbol, timeframe, lower_ms, end_ms))
        conn.commit()
    finally:
        conn.close()


def fetch_ohlcv_data(symbol: str, timeframe: str, start_utc_str: str, end_utc_str: str) -> pd.DataFrame:
    """
    주어진 (symbol, timeframe)과 UTC 기준 시작 및 종료 시간에 해당하는 데이터를 조회하여 DataFrame으로 반환한다.

    반환 DataFrame은 다음 컬럼을 포함합니다:
      ['symbol', 'timeframe', 'open_time_ms', 'timestamp_kst', 'open', 'high', 'low', 'close', 'volume']
    """
    start_dt = datetime.datetime.fromisoformat(start_utc_str).replace(tzinfo=_UTC)
    end_dt = datetime.datetime.fromisoformat(end_utc_str).replace(tzinfo=_UTC)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    boundary_ms = _get_boundary_ms()

    conn = get_db_connection()
    all_data = []
    try:
        cur = conn.cursor()
        if start_ms < boundary_ms:
            upper_ms = min(end_ms, boundary_ms)
            sql_old = """
                SELECT symbol, timeframe, open_time_ms, timestamp_kst, open, high, low, close, volume
                FROM old_data
                WHERE symbol = ?
                  AND timeframe = ?
                  AND open_time_ms >= ?
                  AND open_time_ms < ?
                ORDER BY open_time_ms
            """
            rows = cur.execute(sql_old, (symbol, timeframe, start_ms, upper_ms)).fetchall()
            all_data.extend(rows)
        if end_ms >= boundary_ms:
            lower_ms = max(start_ms, boundary_ms)
            sql_recent = """
                SELECT symbol, timeframe, open_time_ms, timestamp_kst, open, high, low, close, volume
                FROM recent_data
                WHERE symbol = ?
                  AND timeframe = ?
                  AND open_time_ms >= ?
                  AND open_time_ms <= ?
                ORDER BY open_time_ms
            """
            rows = cur.execute(sql_recent, (symbol, timeframe, lower_ms, end_ms)).fetchall()
            all_data.extend(rows)
    finally:
        conn.close()
    cols = ["symbol", "timeframe", "open_time_ms", "timestamp_kst", "open", "high", "low", "close", "volume"]
    return pd.DataFrame(all_data, columns=cols) if all_data else pd.DataFrame(columns=cols)
