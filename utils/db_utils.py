# gptbitcoin/utils/db_utils.py
# DB 관련 유틸리티 모듈

import sqlite3

def connect_db(db_path: str) -> sqlite3.Connection:
    """
    DB에 연결하고, Connection 객체를 반환한다.

    Args:
        db_path (str): DB 파일 경로

    Returns:
        sqlite3.Connection: SQLite3 Connection 객체

    Raises:
        sqlite3.Error: 연결 실패 시 발생
    """
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        raise sqlite3.Error(f"DB 연결 실패: {e}")

def init_db(conn: sqlite3.Connection) -> None:
    """
    old_data, recent_data 테이블이 없으면 생성한다.

    Args:
        conn (sqlite3.Connection): DB 연결 객체

    Raises:
        sqlite3.Error: 테이블 생성 실패 시 발생
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS old_data (
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        timestamp_utc INTEGER NOT NULL,
        timestamp_kst TEXT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        PRIMARY KEY(symbol, timeframe, timestamp_utc)
    );
    CREATE INDEX IF NOT EXISTS idx_old_data 
        ON old_data (symbol, timeframe, timestamp_utc);

    CREATE TABLE IF NOT EXISTS recent_data (
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        timestamp_utc INTEGER NOT NULL,
        timestamp_kst TEXT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        PRIMARY KEY(symbol, timeframe, timestamp_utc)
    );
    CREATE INDEX IF NOT EXISTS idx_recent_data 
        ON recent_data (symbol, timeframe, timestamp_utc);
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
    start_ts: int,
    end_ts: int
) -> None:
    """
    (symbol, timeframe, open_time in [start_ts, end_ts]) 범위의 데이터를
    지정된 테이블에서 삭제한다.

    Args:
        conn (sqlite3.Connection): DB 연결 객체
        table_name (str): "old_data" 또는 "recent_data"
        symbol (str): 심볼 (예: "BTCUSDT")
        timeframe (str): 타임프레임 (예: "1d")
        start_ts (int): UTC 밀리초 기준 시작
        end_ts (int): UTC 밀리초 기준 종료

    Raises:
        sqlite3.Error: DELETE 쿼리 실패 시 발생
    """
    sql = f"""
        DELETE FROM {table_name}
         WHERE symbol=?
           AND timeframe=?
           AND timestamp_utc>=?
           AND timestamp_utc<=?
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, (symbol, timeframe, start_ts, end_ts))
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
    OHLCV 데이터를 지정된 테이블에 INSERT OR REPLACE로 저장한다.

    Args:
        conn (sqlite3.Connection): DB 연결 객체
        table_name (str): "old_data" 또는 "recent_data"
        data_rows (list): 테이블에 삽입할 (symbol, timeframe, timestamp_utc, timestamp_kst,
                          open, high, low, close, volume) 튜플의 리스트

    Raises:
        sqlite3.Error: INSERT 실패 시 발생
    """
    sql = f"""
        INSERT OR REPLACE INTO {table_name}
        (symbol, timeframe, timestamp_utc, timestamp_kst, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cur = conn.cursor()
        cur.executemany(sql, data_rows)
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        raise sqlite3.Error(f"{table_name} INSERT 실패: {e}")
