# gptbitcoin/data/update_data.py
"""
Collector 단계에서 신규 OHLCV 데이터를 DB에 반영하는 모듈.
기존의 DB 초기화, DELETE, INSERT 로직을 utils/db_utils.py로 이관하고,
본 모듈은 해당 유틸 함수를 호출하여 수행한다.

데이터베이스에는 바이낸스에서 받아온 원본 open_time(UTC ms)와,
이를 변환한 timestamp_kst (한국시간, "YYYY-MM-DD HH:MM:SS")를 저장한다.
timestamp_kst는 open_time 왼쪽 컬럼에 위치하며, open, high, low, close, volume 등
나머지 컬럼은 원본 그대로 저장된다.
"""

import sqlite3
import sys
from datetime import datetime, timedelta

# fetch_data.py에서 데이터 수집 함수 임포트
from data.fetch_data import get_ohlcv_from_binance

# config.py에서 DB 관련 설정값 로드
try:
    from config.config import (
        DB_PATH,
        DB_BOUNDARY_DATE,
    )
except ImportError:
    print("config.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# DB 관련 유틸 함수 임포트
try:
    from utils.db_utils import (
        connect_db,
        init_db,
        delete_ohlcv,
        insert_ohlcv,
    )
except ImportError:
    print("db_utils.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)


def update_data_db(
        symbol: str,
        timeframe: str,
        start_str: str,
        end_str: str,
        db_path: str = DB_PATH,
        boundary_date: str = DB_BOUNDARY_DATE,
        dropna_indicators: bool = False
) -> None:
    """
    (symbol, timeframe, start_str ~ end_str) 구간의 OHLCV 데이터를
    바이낸스 선물 API에서 받아 DB에 저장한다.

    DB에 old_data, recent_data 테이블이 없으면 init_db()로 생성.
    boundary_date 이전(open_time < boundary_ts)은 old_data,
    이후(open_time >= boundary_ts)은 recent_data에 저장한다.

    원본 데이터의 open_time 컬럼은 그대로 저장하며,
    open_time을 변환한 timestamp_kst 컬럼은 open_time 왼쪽에 저장된다.

    동일 (symbol, timeframe, open_time)이 존재하면 DELETE 후 INSERT 한다.

    Args:
        symbol (str): 예) "BTCUSDT"
        timeframe (str): 예) "1d", "4h", "1h", "15m" 등
        start_str (str): 수집 시작 ("YYYY-MM-DD HH:MM:SS")
        end_str (str): 수집 종료 ("YYYY-MM-DD HH:MM:SS")
        db_path (str): DB 파일 경로 (config.py 기본값)
        boundary_date (str): old/recent 파티션 구분 시점 (config.py 기본값)
        dropna_indicators (bool): 보조지표 NaN 시 제거 대신 예외 처리 (현재 미사용)

    Raises:
        ValueError: DataFrame에 NaN 존재 시
        sqlite3.Error: DB 작업 실패 시
        RuntimeError: 데이터 수집 실패 시
    """
    # DB 연결
    try:
        conn = connect_db(db_path)
    except sqlite3.Error as e:
        print(f"[update_data_db] DB 연결 실패: {e}")
        sys.exit(1)

    # 테이블 생성 (없으면)
    try:
        init_db(conn)
    except sqlite3.Error as e:
        conn.close()
        print(f"[update_data_db] 테이블 생성 실패: {e}")
        sys.exit(1)

    # boundary_date, start, end → UTC ms (open_time 기준)
    boundary_dt = datetime.strptime(boundary_date, "%Y-%m-%d %H:%M:%S")
    boundary_ts = int(boundary_dt.timestamp() * 1000)

    start_ms = int(datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    end_ms = int(datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    print(f"\n[update_data_db] DB 연결: {db_path}")
    print(f" - symbol={symbol}, timeframe={timeframe}")
    print(f" - 기간: {start_str} ~ {end_str}")
    print(f" - boundary_date={boundary_date}")

    # 먼저 old_data, recent_data에서 해당 구간 레코드 DELETE
    try:
        delete_ohlcv(conn, "old_data", symbol, timeframe, start_ms, end_ms)
        delete_ohlcv(conn, "recent_data", symbol, timeframe, start_ms, end_ms)
    except sqlite3.Error as e:
        conn.close()
        raise sqlite3.Error(f"[update_data_db] 기존 레코드 DELETE 실패: {e}")

    print("[update_data_db] 기존 레코드 삭제 완료.")

    # 새 데이터 수집
    try:
        df = get_ohlcv_from_binance(symbol, timeframe, start_str, end_str)
    except Exception as e:
        conn.close()
        raise RuntimeError(f"[update_data_db] 데이터 수집 실패: {e}")

    # 수집된 결과에 NaN 검사
    if df.isnull().any().any():
        conn.close()
        raise ValueError("[update_data_db] 수집된 DataFrame에 NaN 존재. 중단.")

    print(f"[update_data_db] 수집 성공. {len(df)}개 봉 데이터.")

    # 삽입할 데이터 준비 (컬럼 순서: symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume)
    rows_old = []
    rows_recent = []

    for _, row in df.iterrows():
        ot = int(row["open_time"])  # 바이낸스 원본 open_time (UTC ms)
        dt_utc = datetime.utcfromtimestamp(ot / 1000.0)
        dt_kst = dt_utc + timedelta(hours=9)
        timestamp_kst_str = dt_kst.strftime("%Y-%m-%d %H:%M:%S")

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        v = float(row["volume"])

        data_tuple = (
            symbol,
            timeframe,
            timestamp_kst_str,
            ot,
            o,
            h,
            l,
            c,
            v
        )

        # boundary_date 기준 분류 (open_time 사용)
        if ot < boundary_ts:
            rows_old.append(data_tuple)
        else:
            rows_recent.append(data_tuple)

    # 분류된 데이터 INSERT
    try:
        row_count_old = len(rows_old)
        row_count_recent = len(rows_recent)

        if row_count_old > 0:
            insert_ohlcv(conn, "old_data", rows_old)
        if row_count_recent > 0:
            insert_ohlcv(conn, "recent_data", rows_recent)
    except sqlite3.Error as e:
        conn.close()
        raise sqlite3.Error(f"[update_data_db] INSERT 실패: {e}")

    conn.close()
    print(f"[update_data_db] old_data 삽입: {row_count_old}건, recent_data 삽입: {row_count_recent}건")
    print("[update_data_db] DB 업데이트 완료.")


if __name__ == "__main__":
    """
    사용자는 아래 4가지 항목만 직접 수정한다:
      1) symbol
      2) timeframes
      3) start_str
      4) end_str
    나머지(DB 경로 등)는 config.py에서 로드.

    timestamp_kst 컬럼은 "YYYY-MM-DD HH:MM:SS" 형태의 한국시간,
    open_time 컬럼은 바이낸스에서 받아온 원본 값(UTC ms)이다.
    """

    symbol = "BTCUSDT"
    timeframes = ["1d", "4h"]
    start_str = "2019-01-01 00:00:00"
    end_str = "2025-02-01 00:00:00"

    print("=== OHLCV 데이터 업데이트 시작 ===")
    for tf in timeframes:
        try:
            update_data_db(
                symbol=symbol,
                timeframe=tf,
                start_str=start_str,
                end_str=end_str,
            )
        except Exception as e:
            print(f"[main] {tf} 업데이트 중 오류 발생: {e}")
    print("=== 모든 타임프레임 업데이트 종료 ===")