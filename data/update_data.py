# gptbitcoin/data/update_data.py
"""
Collector 단계에서 신규 OHLCV 데이터를 DB에 반영하는 모듈.
utils/db_utils.py의 함수들을 호출해 DB에 INSERT 한다.

[요구사항]
1) __main__으로 실행할 때는 update_mode="full" 방식(구간 내 old_data+recent_data 전부 삭제 후 재수집).
2) main.py에서 (자동) 호출할 때는 update_mode="recent" 방식(절대 old_data 테이블은 건드리지 않고,
   DB_BOUNDARY_DATE 이후(=recent_data 테이블)만 삭제 후 최신 데이터를 저장).
3) DB_BOUNDARY_DATE 이전 old_data는 절대 수정하지 않는다.
4) DB_BOUNDARY_DATE 이후 구간은 항상 삭제 후 최신 데이터를 저장한다.

데이터베이스에는 다음 컬럼을 저장:
  symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume

주의:
- 바이낸스 선물 API 호출 시, 결측(NaN)이 발견되면 즉시 ValueError 발생.
- DELETE → INSERT 로직을 통해 중복을 방지한다.
"""

import sqlite3
import sys
from datetime import datetime, timedelta

# fetch_data.py에서 데이터 수집 함수 임포트
try:
    from data.fetch_data import get_ohlcv_from_binance
except ImportError:
    print("fetch_data.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

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
    update_mode: str = "full"
) -> None:
    """
    바이낸스 선물 API에서 (symbol, timeframe, start_str~end_str) 구간 데이터를 받아
    DB에 저장한다.

    update_mode:
      - "full": old_data + recent_data 테이블 모두 (start_str~end_str) 삭제 후 재수집
      - "recent": DB_BOUNDARY_DATE 이전(old_data)은 건드리지 않고,
                  DB_BOUNDARY_DATE 이후(recent_data) 구간만 삭제 후 최신 데이터로 갱신

    Args:
        symbol (str): 예) "BTCUSDT"
        timeframe (str): 예) "1d", "4h" 등
        start_str (str): "YYYY-MM-DD HH:MM:SS"
        end_str   (str): "YYYY-MM-DD HH:MM:SS"
        db_path (str, optional): DB 경로
        boundary_date (str, optional): DB_BOUNDARY_DATE
        update_mode (str, optional): "full" 또는 "recent"

    Raises:
        RuntimeError: 데이터 수집 실패
        ValueError: 수집된 데이터에 NaN 존재
        sqlite3.Error: DB 작업 실패
    """
    # DB 연결
    try:
        conn = connect_db(db_path)
    except sqlite3.Error as e:
        print(f"[update_data_db] DB 연결 실패: {e}")
        sys.exit(1)

    # 테이블 생성
    try:
        init_db(conn)
    except sqlite3.Error as e:
        conn.close()
        print(f"[update_data_db] 테이블 생성 실패: {e}")
        sys.exit(1)

    boundary_dt = datetime.strptime(boundary_date, "%Y-%m-%d %H:%M:%S")
    boundary_ts = int(boundary_dt.timestamp() * 1000)

    start_ms = int(datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    end_ms = int(datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    print(f"\n[update_data_db] mode={update_mode}, symbol={symbol}, tf={timeframe}")
    print(f" - 기간: {start_str} ~ {end_str}")
    print(f" - boundary_date={boundary_date}")

    # 삭제 구간 결정
    # "full" => old_data + recent_data 둘 다 (start_ms ~ end_ms) 삭제
    # "recent" => old_data는 건드리지 않고, boundary_ts ~ end_ms 구간만 recent_data에서 삭제
    if update_mode == "full":
        try:
            delete_ohlcv(conn, "old_data", symbol, timeframe, start_ms, end_ms)
            delete_ohlcv(conn, "recent_data", symbol, timeframe, start_ms, end_ms)
            print("[update_data_db] (full) old_data + recent_data 레코드 삭제 완료.")
        except sqlite3.Error as e:
            conn.close()
            raise sqlite3.Error(f"[update_data_db] full 모드 DELETE 실패: {e}")
    else:
        # update_mode == "recent"
        # old_data는 절대 건드리지 않고, boundary_ts~end_ms만 recent_data에서 삭제
        recent_start = boundary_ts if start_ms < boundary_ts else start_ms
        try:
            delete_ohlcv(conn, "recent_data", symbol, timeframe, recent_start, end_ms)
            print("[update_data_db] (recent) recent_data 테이블 삭제 완료.")
        except sqlite3.Error as e:
            conn.close()
            raise sqlite3.Error(f"[update_data_db] recent 모드 DELETE 실패: {e}")

    # 새 데이터 수집
    try:
        # 만약 update_mode="recent"인데 start_str이 boundary 이전이라면
        # 실제로는 boundary_dt 이후만 가져오도록 fetch 파라미터를 조정 (사용자 편의)
        if update_mode == "recent" and start_ms < boundary_ts:
            adj_start_dt = datetime.fromtimestamp(boundary_ts / 1000.0)
            adj_start_str = adj_start_dt.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[update_data_db] (recent) start_str={start_str} -> {adj_start_str}로 조정")
            df = get_ohlcv_from_binance(symbol, timeframe, adj_start_str, end_str)
        else:
            df = get_ohlcv_from_binance(symbol, timeframe, start_str, end_str)

    except Exception as e:
        conn.close()
        raise RuntimeError(f"[update_data_db] 데이터 수집 실패: {e}")

    if df.isnull().any().any():
        conn.close()
        raise ValueError("[update_data_db] 수집된 DataFrame에 NaN 존재. 중단.")

    print(f"[update_data_db] 수집 성공. {len(df)}개 봉 데이터.")

    # 테이블 삽입 준비
    rows_old = []
    rows_recent = []

    for _, row in df.iterrows():
        ot = int(row["open_time"])  # UTC ms
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
        if ot < boundary_ts:
            # old_data
            rows_old.append(data_tuple)
        else:
            # recent_data
            rows_recent.append(data_tuple)

    # 삽입
    try:
        rc_old = len(rows_old)
        rc_recent = len(rows_recent)

        # (full) 모드면 old_data도 새로 삽입할 수 있음
        # (recent) 모드라도 수집한 df 내 old_data 부분은 무시하는 게 자연스럽지만,
        #  boundaries를 넘어 수집했을 때는 rows_old가 발생할 수 있다.
        #  그러나 "recent" 모드에선 old_data를 "절대 수정 금지"이므로 insert 하면 안됨.
        #  => Insert 하지 않고 skip.
        if update_mode == "full":
            if rc_old > 0:
                insert_ohlcv(conn, "old_data", rows_old)
        else:
            # update_mode="recent" => old_data 테이블은 건드리지 않음
            if rc_old > 0:
                print(f"[update_data_db] (recent) old_data는 수정하지 않으므로 {rc_old}건 무시")

        if rc_recent > 0:
            insert_ohlcv(conn, "recent_data", rows_recent)

        conn.close()
        print(f"[update_data_db] old_data 삽입: {rc_old}건, recent_data 삽입: {rc_recent}건")
        print("[update_data_db] DB 업데이트 완료.")

    except sqlite3.Error as e:
        conn.close()
        raise sqlite3.Error(f"[update_data_db] INSERT 실패: {e}")


if __name__ == "__main__":
    """
    사용자가 직접 이 스크립트를 실행하여 과거 데이터를 미리 DB에 저장하려는 경우,
    update_mode="full"로 사용한다. => DB_BOUNDARY_DATE와 무관하게 
    (start_str ~ end_str) 구간의 old_data + recent_data 테이블을 모두 삭제 후 재저장.

    사용자 정의 가능항목:
      1) symbol
      2) timeframes
      3) start_str
      4) end_str
      5) update_mode="full" (수정 가능)

    DB_PATH 등 나머지는 config.py에서 로드.
    """

    symbol = "BTCUSDT"
    timeframes = ["1d", "4h"]
    start_str = "2019-01-01 00:00:00"
    end_str = "2025-02-01 00:00:00"
    mode = "full"  # 직접 실행 시 full 모드 권장

    print("=== OHLCV 데이터 업데이트 시작 ===")
    for tf in timeframes:
        try:
            update_data_db(
                symbol=symbol,
                timeframe=tf,
                start_str=start_str,
                end_str=end_str,
                update_mode=mode
            )
        except Exception as e:
            print(f"[__main__] {tf} 업데이트 중 오류 발생: {e}")
    print("=== 모든 타임프레임 업데이트 종료 ===")
