# gptbitcoin/utils/db_utils.py
# DB 관련 유틸리티 모듈 (open 컬럼 포함)
# prepare_ohlcv_with_warmup 함수를 수정하여
# 실제 타임프레임(timeframe)에 맞춰 워밍업 델타를 계산한다.

import datetime
import sqlite3
from typing import Optional

import pandas as pd
import pytz

from config.config import DB_PATH
from utils.date_time import timeframe_to_timedelta


def connect_db(db_path: str) -> sqlite3.Connection:
    """
    SQLite DB에 연결하고 Connection 객체를 반환한다.

    Args:
        db_path (str): SQLite DB 파일 경로

    Returns:
        sqlite3.Connection: 연결된 Connection 객체

    Raises:
        sqlite3.Error: 연결 실패 시
    """
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        raise sqlite3.Error(f"DB 연결 실패: {e}")


def init_db(conn: sqlite3.Connection) -> None:
    """
    old_data, recent_data 테이블이 없으면 생성한다. (open 컬럼 포함)

    Schema:
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      timestamp_kst TEXT NOT NULL,
      open_time INTEGER NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      volume REAL NOT NULL,
      PRIMARY KEY(symbol, timeframe, open_time)

    Args:
        conn (sqlite3.Connection): DB 연결

    Raises:
        sqlite3.Error: 테이블 생성 실패 시
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS old_data (
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        timestamp_kst TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        PRIMARY KEY(symbol, timeframe, open_time)
    );
    CREATE INDEX IF NOT EXISTS idx_old_data
        ON old_data (symbol, timeframe, open_time);

    CREATE TABLE IF NOT EXISTS recent_data (
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        timestamp_kst TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        PRIMARY KEY(symbol, timeframe, open_time)
    );
    CREATE INDEX IF NOT EXISTS idx_recent_data
        ON recent_data (symbol, timeframe, open_time);
    """
    try:
        conn.executescript(create_sql)
    except sqlite3.Error as e:
        raise sqlite3.Error(f"테이블 생성 실패: {e}")


def delete_ohlcv(
    conn: sqlite3.Connection,
    table_name: str,
    symbol: str,
    timeframe: str,
    start_ot: int,
    end_ot: int
) -> None:
    """
    특정 테이블에서 (symbol, timeframe)에 대해
    open_time이 [start_ot, end_ot] 범위인 행을 삭제한다.

    Args:
        conn (sqlite3.Connection): DB 연결
        table_name (str): "old_data" 또는 "recent_data"
        symbol (str): 예) "BTCUSDT"
        timeframe (str): 예) "1d"
        start_ot (int): UTC ms 시작 시점
        end_ot (int): UTC ms 종료 시점

    Raises:
        sqlite3.Error: DELETE 실패 시
    """
    sql = f"""
        DELETE FROM {table_name}
         WHERE symbol=?
           AND timeframe=?
           AND open_time>=?
           AND open_time<=?
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, (symbol, timeframe, start_ot, end_ot))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise sqlite3.Error(f"{table_name} DELETE 실패: {e}")


def insert_ohlcv(
    conn: sqlite3.Connection,
    table_name: str,
    data_rows: list
) -> None:
    """
    (symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume)
    순서로 된 튜플 리스트를 INSERT OR REPLACE 방식으로 삽입한다.

    Args:
        conn (sqlite3.Connection): DB 연결
        table_name (str): "old_data" 또는 "recent_data"
        data_rows (list): 위 순서대로 된 튜플들의 리스트

    Raises:
        sqlite3.Error: INSERT 실패 시
    """
    sql = f"""
        INSERT OR REPLACE INTO {table_name}
        (symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cur = conn.cursor()
        cur.executemany(sql, data_rows)
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise sqlite3.Error(f"{table_name} INSERT 실패: {e}")


def fetch_ohlcv_merged(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    start_ot: int,
    end_ot: int,
    boundary_ot: int
) -> pd.DataFrame:
    """
    old_data + recent_data에서 주어진 구간을 조회 후 병합한다.
    open_time < boundary_ot -> old_data, >= boundary_ot -> recent_data.
    결과는 open_time ASC로 정렬.

    Args:
        conn (sqlite3.Connection): DB 연결
        symbol (str): 예) "BTCUSDT"
        timeframe (str): 예) "1d"
        start_ot (int): UTC ms 시작 시점
        end_ot (int): UTC ms 종료 시점
        boundary_ot (int): DB_BOUNDARY_DATE(UTC ms)

    Returns:
        pd.DataFrame: [symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume]
    """
    old_start = start_ot
    old_end = min(end_ot, boundary_ot - 1)

    recent_start = max(start_ot, boundary_ot)
    recent_end = end_ot

    df_old = pd.DataFrame()
    df_recent = pd.DataFrame()

    if old_start <= old_end:
        sql_old = """
            SELECT symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume
              FROM old_data
             WHERE symbol=?
               AND timeframe=?
               AND open_time>=?
               AND open_time<=?
             ORDER BY open_time ASC
        """
        df_old = pd.read_sql_query(sql_old, conn, params=(symbol, timeframe, old_start, old_end))

    if recent_start <= recent_end:
        sql_recent = """
            SELECT symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume
              FROM recent_data
             WHERE symbol=?
               AND timeframe=?
               AND open_time>=?
               AND open_time<=?
             ORDER BY open_time ASC
        """
        df_recent = pd.read_sql_query(sql_recent, conn, params=(symbol, timeframe, recent_start, recent_end))

    df_merged = pd.concat([df_old, df_recent], ignore_index=True)
    df_merged.sort_values("open_time", inplace=True)
    return df_merged


def prepare_ohlcv_with_warmup(
    symbol: str,
    timeframe: str,
    start_utc_str: str,
    end_utc_str: str,
    warmup_bars: int,
    exchange_open_date_utc_str: str,
    boundary_date_utc_str: str,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    워밍업 분량을 반영한 구간을 DB에서 old_data+recent_data로 병합 조회한다.

    1) timeframe에 맞춰 warmup_bars만큼 (한 봉의 시간간격) * warmup_bars => warmup_delta 계산
    2) start_utc_str에서 warmup_delta만큼 과거로 거슬러 가되,
       거래소 오픈일(exchange_open_date_utc_str) 이전으로는 가지 않도록 보정
    3) 그 구간부터 end_utc_str까지 old_data/recent_data를 fetch해 merge
    4) 정렬하여 반환

    Args:
        symbol (str): 예) "BTCUSDT"
        timeframe (str): "1d", "4h", "15m" 등
        start_utc_str (str): 백테스트 메인 시작(UTC, "YYYY-MM-DD HH:MM:SS")
        end_utc_str (str): 백테스트 종료(UTC)
        warmup_bars (int): 필요한 워밍업 봉 수
        exchange_open_date_utc_str (str): 거래소 오픈(UTC)
        boundary_date_utc_str (str): DB_BOUNDARY_DATE(UTC)
        db_path (str, optional): DB 경로 (None이면 config.DB_PATH 사용)

    Returns:
        pd.DataFrame: 병합된 OHLCV (open_time ASC)

    Raises:
        sqlite3.Error: DB 문제
        ValueError: 날짜 파싱 실패 등
    """
    if db_path is None:
        db_path = DB_PATH

    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    # 메인 구간 start/end
    naive_start = datetime.datetime.strptime(start_utc_str, dt_format)
    main_start_utc = utc.localize(naive_start)
    start_ms = int(main_start_utc.timestamp() * 1000)

    naive_end = datetime.datetime.strptime(end_utc_str, dt_format)
    main_end_utc = utc.localize(naive_end)
    end_ms = int(main_end_utc.timestamp() * 1000)

    # 거래소 오픈일
    naive_open = datetime.datetime.strptime(exchange_open_date_utc_str, dt_format)
    exch_open_utc = utc.localize(naive_open)

    # DB 경계
    naive_boundary = datetime.datetime.strptime(boundary_date_utc_str, dt_format)
    boundary_utc = utc.localize(naive_boundary)
    boundary_ms = int(boundary_utc.timestamp() * 1000)

    # -------------------------------------------
    # 워밍업 delta: 실제 timeframe 간격 × warmup_bars
    # 예) "1d"이고 warmup_bars=200 => 200일
    # 예) "15m"이고 warmup_bars=100 => 25시간
    # -------------------------------------------
    delta_per_bar = timeframe_to_timedelta(timeframe)
    warmup_delta = delta_per_bar * warmup_bars

    # 최종 워밍업 시작 시점
    warmup_start_dt = main_start_utc - warmup_delta
    if warmup_start_dt < exch_open_utc:
        warmup_start_dt = exch_open_utc
    warmup_start_ms = int(warmup_start_dt.timestamp() * 1000)

    conn = connect_db(db_path)
    init_db(conn)

    df_merged = fetch_ohlcv_merged(
        conn=conn,
        symbol=symbol,
        timeframe=timeframe,
        start_ot=warmup_start_ms,
        end_ot=end_ms,
        boundary_ot=boundary_ms
    )
    conn.close()

    return df_merged
