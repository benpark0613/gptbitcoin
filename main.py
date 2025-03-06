# gptbitcoin/main.py
# 구글 스타일 + 필요한 최소 한글 주석
"""
main.py
- DB에서 old_data/recent_data를 불러와 전처리 + 워밍업 구간 처리
- EXCHANGE_OPEN_DATE를 고려해 워밍업 구간 클램핑
- 지표 계산 후 백테스트 대상 구간에서 NaN 여부 검사
  - 만약 확보된 워밍업이 충분하다면 NaN 발견 시 예외
  - 워밍업이 부족하면 경고만 출력하고 진행
- IS/OOS 구분 후 CSV 저장
"""

import sys
from datetime import datetime, timedelta

import pandas as pd

try:
    from config.config import (
        SYMBOL,
        TIMEFRAMES,
        START_DATE,
        END_DATE,
        IS_OOS_BOUNDARY_DATE,
        DB_PATH,
        DB_BOUNDARY_DATE,
        INDICATOR_CONFIG,
        LOG_LEVEL,
        EXCHANGE_OPEN_DATE
    )
except ImportError:
    print("config.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# DB 유틸
try:
    from utils.db_utils import (
        connect_db,
        init_db
    )
except ImportError:
    print("db_utils.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# 전처리
try:
    from data.preprocess import clean_ohlcv, merge_old_recent
except ImportError:
    print("preprocess.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# 지표 계산
try:
    from indicators.indicators import calc_all_indicators
except ImportError:
    print("indicators.py를 찾을 수 없거나 경로 설정이 잘못되었습니다.")
    sys.exit(1)

# 워밍업 길이 계산
try:
    from utils.indicator_utils import get_required_warmup_bars
except ImportError:
    print("indicator_utils.py에서 get_required_warmup_bars를 불러오지 못했습니다.")
    sys.exit(1)


def _get_time_delta_for_tf(timeframe: str) -> timedelta:
    """
    "1d", "4h", "1h", "15m" 등 문자열을 실제 시간간격으로 변환.
    """
    tf_lower = timeframe.lower()
    if tf_lower.endswith("d"):
        # 예: "1d" -> 1일
        days = int(tf_lower.replace("d", ""))
        return timedelta(days=days)
    elif tf_lower.endswith("h"):
        # 예: "4h" -> 4시간
        hours = int(tf_lower.replace("h", ""))
        return timedelta(hours=hours)
    elif tf_lower.endswith("m"):
        # 예: "15m" -> 15분
        mins = int(tf_lower.replace("m", ""))
        return timedelta(minutes=mins)
    else:
        # fallback
        return timedelta(days=1)


def select_ohlcv(conn, table_name: str, symbol: str, timeframe: str,
                 start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    DB에서 (symbol, timeframe, open_time in [start_ms, end_ms]) 범위 레코드 조회.
    """
    sql = f"""
        SELECT symbol, timeframe, timestamp_kst, open_time, open, high, low, close, volume
          FROM {table_name}
         WHERE symbol=?
           AND timeframe=?
           AND open_time>=?
           AND open_time<=?
         ORDER BY open_time ASC
    """
    df = pd.read_sql_query(sql, conn, params=(symbol, timeframe, start_ms, end_ms))
    return df


def load_and_preprocess_data(symbol: str,
                             timeframe: str,
                             start_date: str,
                             boundary_date: str,
                             end_date: str,
                             warmup_bars: int = 0) -> pd.DataFrame:
    """
    DB에서 old_data, recent_data를 구간별로 불러오되,
    warmup_bars만큼 start_date 이전 데이터를 추가 로드(워밍업).
    EXCHANGE_OPEN_DATE를 고려해 워밍업 시작 시점을 클램핑.
    이후 병합 후 중복/이상치 체크.

    Args:
        symbol (str)
        timeframe (str): "1d", "4h" 등
        start_date (str): "YYYY-MM-DD HH:MM:SS"
        boundary_date (str): DB old/recent 파티션 구분
        end_date (str): "YYYY-MM-DD HH:MM:SS"
        warmup_bars (int): 지표 계산에 필요한 워밍업 봉 수

    Returns:
        pd.DataFrame: (워밍업 구간 포함) 전처리된 DF
    """
    conn = connect_db(DB_PATH)
    init_db(conn)  # 혹시 테이블 미존재 시 생성

    main_start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    boundary_dt = datetime.strptime(boundary_date, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

    delta_per_bar = _get_time_delta_for_tf(timeframe)
    warmup_delta = delta_per_bar * warmup_bars

    # 백테스트 시작일에서 warmup_bars만큼 과거로 당긴 시점
    warmup_start_dt = main_start_dt - warmup_delta

    # 거래소 오픈일 고려해 클램핑
    exchange_open_dt = datetime.strptime(EXCHANGE_OPEN_DATE, "%Y-%m-%d %H:%M:%S")
    if warmup_start_dt < exchange_open_dt:
        warmup_start_dt = exchange_open_dt

    warmup_start_ms = int(warmup_start_dt.timestamp() * 1000)
    boundary_ms = int(boundary_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # old_data, recent_data 구간
    df_old = select_ohlcv(conn, "old_data", symbol, timeframe, warmup_start_ms, boundary_ms - 1)
    df_recent = select_ohlcv(conn, "recent_data", symbol, timeframe, boundary_ms, end_ms)
    conn.close()

    # 전처리 (중복/이상치)
    df_old = clean_ohlcv(df_old)
    df_recent = clean_ohlcv(df_recent)

    merged_df = merge_old_recent(df_old, df_recent)
    return merged_df


def run_main():
    """
    1) 지표 파라미터로부터 워밍업 봉 수 산출(get_required_warmup_bars)
    2) EXCHANGE_OPEN_DATE 고려하여 워밍업 구간을 클램핑
    3) 실제 백테스트 구간(START_DATE~END_DATE)에서 NaN 검사
       - 충분한 워밍업 확보 시 NaN -> 예외
       - 부족한 워밍업 시 일부 NaN 경고 후 진행
    4) IS/OOS 구분 후 CSV 저장
    """
    print(f"[main] 시작 - symbol={SYMBOL}, timeframes={TIMEFRAMES}, log_level={LOG_LEVEL}")

    # 필요한 워밍업 봉 수
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[main] 필요한 워밍업 봉 수: {warmup_bars}")

    # 백테스트 구간 start/end -> UTC ms
    main_start_ms = int(datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    main_end_ms = int(datetime.strptime(END_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    # IS/OOS 구분 시점
    boundary_dt = datetime.strptime(IS_OOS_BOUNDARY_DATE, "%Y-%m-%d %H:%M:%S")
    boundary_ms = int(boundary_dt.timestamp() * 1000)

    for tf in TIMEFRAMES:
        print(f"[main] 데이터 로딩(워밍업 포함) + 전처리: {tf}")
        df_merged = load_and_preprocess_data(
            symbol=SYMBOL,
            timeframe=tf,
            start_date=START_DATE,
            boundary_date=DB_BOUNDARY_DATE,
            end_date=END_DATE,
            warmup_bars=warmup_bars
        )

        print(f"[main] 보조지표 계산: {tf}")
        df_ind = calc_all_indicators(df_merged.copy())

        # 워밍업 구간 제외(즉, 백테스트 대상) 구간
        df_no_warmup = df_ind[df_ind["open_time"] >= main_start_ms].copy()

        # 실제 확보한 워밍업 길이
        df_warmup_part = df_ind[df_ind["open_time"] < main_start_ms]
        actual_warmup_bars = len(df_warmup_part)

        print(f" - 워밍업 구간: {actual_warmup_bars}봉 / 필요: {warmup_bars}봉")

        if actual_warmup_bars >= warmup_bars:
            # 데이터가 충분하므로, 백테스트 구간에 NaN이 있으면 예외 발생
            if df_no_warmup.isnull().any().any():
                raise ValueError(f"[main] 백테스트 구간에 NaN 발생 (timeframe={tf}). 중단.")
        else:
            # 워밍업이 부족한 경우, 일부 NaN을 허용(경고만 출력)
            if df_no_warmup.isnull().any().any():
                print("[경고] 충분한 워밍업 데이터를 확보하지 못해 백테스트 초반부에 NaN이 있을 수 있습니다.")
                print("        백테스트는 계속 진행합니다.")

        # IS/OOS 구분
        df_no_warmup["is_oos"] = df_no_warmup["open_time"].apply(
            lambda x: "IS" if x < boundary_ms else "OOS"
        )

        # CSV 저장
        csv_filename = f"merged_{SYMBOL}_{tf}.csv"
        df_no_warmup.to_csv(csv_filename, index=False, encoding="utf-8")
        print(f"[main] CSV 저장 완료: {csv_filename}, rows={len(df_no_warmup)}")

    print("[main] 완료: 모든 타임프레임 처리 종료.")


if __name__ == "__main__":
    run_main()
