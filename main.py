# gptbitcoin/main.py
# 구글 스타일, 필요한 최소 한글 주석
# 업데이트된 요구사항 + 부분 커버리지 체크 로직을 모두 반영:
# 1) 콤보 생성 (combo_generator)
# 2) 백테스트 구간의 recent_data가 부분만 있으면 누락 구간만 update_data_db(update_mode="recent")로 받아온다
# 3) IS/OOS 분리, run_is / run_oos
# 4) 성과지표 CSV + (OHLCV+보조지표) CSV 저장

import sys
import os
import pandas as pd
import datetime
from datetime import timedelta

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
        EXCHANGE_OPEN_DATE,
        RESULTS_DIR
    )
except ImportError:
    print("[main.py] config.py에서 설정값을 가져오지 못했습니다.")
    sys.exit(1)

# update_data_db 호출
try:
    from data.update_data import update_data_db
except ImportError:
    print("[main.py] update_data.py import 에러")
    sys.exit(1)

# DB 유틸
try:
    from utils.db_utils import connect_db, init_db
except ImportError:
    print("[main.py] db_utils.py import 에러")
    sys.exit(1)

# 전처리
try:
    from data.preprocess import clean_ohlcv, merge_old_recent
except ImportError:
    print("[main.py] preprocess.py import 에러")
    sys.exit(1)

# 지표 계산
try:
    from indicators.indicators import calc_all_indicators
except ImportError:
    print("[main.py] indicators.py import 에러")
    sys.exit(1)

# 워밍업 계산
try:
    from utils.indicator_utils import get_required_warmup_bars
except ImportError:
    print("[main.py] indicator_utils.py import 에러")
    sys.exit(1)

# 백테스트(인샘플, 아웃샘플)
try:
    from backtest.run_is import run_is
    from backtest.run_oos import run_oos
except ImportError:
    print("[main.py] run_is.py, run_oos.py import 에러")
    sys.exit(1)

# 콤보 생성
try:
    from backtest.combo_generator import generate_indicator_combos
except ImportError:
    print("[main.py] combo_generator.py import 에러")
    sys.exit(1)


def _get_time_delta_for_tf(timeframe: str) -> timedelta:
    """
    "1d", "4h", "1h", "15m" 등 문자열을 timedelta로 변환.
    """
    tf_lower = timeframe.lower()
    if tf_lower.endswith("d"):
        d_str = tf_lower.replace("d", "")
        d_val = int(d_str) if d_str.isdigit() else 1
        return timedelta(days=d_val)
    elif tf_lower.endswith("h"):
        h_str = tf_lower.replace("h", "")
        h_val = int(h_str) if h_str.isdigit() else 1
        return timedelta(hours=h_val)
    elif tf_lower.endswith("m"):
        m_str = tf_lower.replace("m", "")
        m_val = int(m_str) if m_str.isdigit() else 1
        return timedelta(minutes=m_val)
    else:
        return timedelta(days=1)


def select_ohlcv(conn, table_name: str, symbol: str, timeframe: str,
                 start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    DB에서 (symbol, timeframe, open_time in [start_ms, end_ms]) 범위 SELECT
    """
    sql = f"""
        SELECT symbol, timeframe, timestamp_kst, open_time,
               open, high, low, close, volume
          FROM {table_name}
         WHERE symbol=?
           AND timeframe=?
           AND open_time>=?
           AND open_time<=?
         ORDER BY open_time ASC
    """
    df = pd.read_sql_query(sql, conn, params=(symbol, timeframe, start_ms, end_ms))
    return df


def load_and_preprocess_data(symbol: str, timeframe: str,
                             start_date: str, boundary_date: str,
                             end_date: str, warmup_bars: int = 0) -> pd.DataFrame:
    """
    DB에서 old_data(워밍업+IS) + recent_data(OOS)를 병합 후 clean_ohlcv, merge.
    """
    conn = connect_db(DB_PATH)
    init_db(conn)

    main_start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    boundary_dt = datetime.datetime.strptime(boundary_date, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

    delta_per_bar = _get_time_delta_for_tf(timeframe)
    warmup_delta = delta_per_bar * warmup_bars

    # 워밍업 시작점
    warmup_start_dt = main_start_dt - warmup_delta
    exchange_open_dt = datetime.datetime.strptime(EXCHANGE_OPEN_DATE, "%Y-%m-%d %H:%M:%S")
    if warmup_start_dt < exchange_open_dt:
        warmup_start_dt = exchange_open_dt

    warmup_start_ms = int(warmup_start_dt.timestamp() * 1000)
    boundary_ms = int(boundary_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # old_data
    df_old = select_ohlcv(conn, "old_data", symbol, timeframe, warmup_start_ms, boundary_ms - 1)
    # recent_data
    df_recent = select_ohlcv(conn, "recent_data", symbol, timeframe, boundary_ms, end_ms)
    conn.close()

    df_old = clean_ohlcv(df_old)
    df_recent = clean_ohlcv(df_recent)

    merged = merge_old_recent(df_old, df_recent)
    return merged


def _ms_to_str(ms_val: int) -> str:
    """UTC ms → "YYYY-MM-DD HH:MM:SS" 문자열"""
    dt = datetime.datetime.utcfromtimestamp(ms_val / 1000.0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def ensure_recent_coverage(symbol: str, timeframe: str, start_date: str, end_date: str) -> None:
    """
    DB에서 recent_data 구간이 (start_date ~ end_date)를 완전히 커버하는지 검사.
    일부만 있거나 전혀 없으면, 부족 구간만 update_data_db(update_mode="recent") 호출.
    """
    conn = connect_db(DB_PATH)
    init_db(conn)

    start_ms = int(datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    end_ms = int(datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    df_recent = select_ohlcv(conn, "recent_data", symbol, timeframe, start_ms, end_ms)
    conn.close()

    if df_recent.empty:
        print(f"[main.py] recent_data: {timeframe} 구간 {start_date}~{end_date} 전혀 없음 -> update_data_db(recent)")
        try:
            update_data_db(symbol, timeframe, start_date, end_date, update_mode="recent")
        except Exception as e:
            print(f"[ensure_recent_coverage] update_data_db 실패: {e}")
        return

    # 부분적으로 존재할 경우, min/max로 판단
    db_min = df_recent["open_time"].min()
    db_max = df_recent["open_time"].max()

    # db_min <= start_ms, db_max >= end_ms 이면 완전히 커버
    # 하나라도 안 맞으면 부족한 구간
    need_update = False
    missing_start_ms = None
    missing_end_ms = None

    # 시작 부분 누락
    if db_min > start_ms:
        missing_start_ms = start_ms
        missing_end_ms = db_min - 1
        need_update = True

    # 끝 부분 누락
    if db_max < end_ms:
        if not need_update:
            # 이번이 첫 누락 => 그냥 db_max+1~end_ms
            missing_start_ms = db_max + 1
            missing_end_ms = end_ms
            need_update = True
        else:
            # 이미 앞뒤가 동시에 누락되면, 로직을 세분화해야 함
            # 여기서는 단순화해 "여러번 update_data_db" 호출 or 구간 합친다
            # 편의상 구간 합침(앞뒤 누락이 매우 드물겠으나)
            if missing_start_ms > db_max+1:
                # 중간이 겹치는지는 복잡.. 여기선 가정
                pass
            missing_end_ms = end_ms

    if need_update and (missing_start_ms is not None) and (missing_end_ms is not None):
        str_start = _ms_to_str(missing_start_ms)
        str_end = _ms_to_str(missing_end_ms)
        print(f"[main.py] recent_data: {timeframe} 구간 일부 부족 -> update_data_db(recent) {str_start}~{str_end}")

        try:
            update_data_db(symbol, timeframe, str_start, str_end, update_mode="recent")
        except Exception as e:
            print(f"[ensure_recent_coverage] update_data_db 실패: {e}")


def run_main():
    """
    전체 메인 실행 로직:
      1) combo_generator로 모든 지표 파라미터 조합 생성
      2) (START_DATE~END_DATE) 구간에서 recent_data 테이블이 완전히 커버되는지 확인
         - 부분 누락 시 누락 구간만 update_data_db(update_mode="recent")
      3) (warmup+백테스트) 구간 로드 -> 지표 계산
      4) IS/OOS 분리 -> run_is -> run_oos
      5) 성과지표 CSV + (OHLCV+보조지표) CSV 저장
    """
    print(f"[main.py] Start - SYMBOL={SYMBOL}, TIMEFRAMES={TIMEFRAMES}, LOG_LEVEL={LOG_LEVEL}")

    if not TIMEFRAMES:
        print("[main.py] TIMEFRAMES가 비어있습니다. 종료.")
        return

    # 콤보 생성
    combos = generate_indicator_combos()
    if not combos:
        print("[main.py] combo_generator가 만든 combos가 비어있습니다. 종료.")
        return
    print(f"[main.py] 생성된 지표 콤보 개수: {len(combos)}")

    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[main.py] 필요한 워밍업 봉 수: {warmup_bars}")

    # IS/OOS 분리 시점
    boundary_dt = datetime.datetime.strptime(IS_OOS_BOUNDARY_DATE, "%Y-%m-%d %H:%M:%S")
    boundary_ms = int(boundary_dt.timestamp() * 1000)

    # 결과 폴더
    os.makedirs(RESULTS_DIR, exist_ok=True)

    for tf in TIMEFRAMES:
        print(f"\n[main.py] --- Timeframe: {tf} ---")

        # 1) recent_data 완전 커버 여부 확인 -> 부족 구간 update_data_db
        ensure_recent_coverage(SYMBOL, tf, START_DATE, END_DATE)

        # 2) DB에서 (warmup+백테스트) 구간 로드 + 지표 계산
        df_merged = load_and_preprocess_data(
            symbol=SYMBOL,
            timeframe=tf,
            start_date=START_DATE,
            boundary_date=DB_BOUNDARY_DATE,
            end_date=END_DATE,
            warmup_bars=warmup_bars
        )
        print(f" - 로딩+전처리 완료. rows={len(df_merged)}")

        if df_merged.empty:
            print(f" - 데이터가 비어 있음, Timeframe={tf} 스킵.")
            continue

        df_ind = calc_all_indicators(df_merged.copy(), cfg=INDICATOR_CONFIG)
        print(" - 지표 계산 완료.")

        # 백테스트 구간 필터 (START_DATE~END_DATE)
        start_ms = int(datetime.datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        df_test = df_ind[df_ind["open_time"] >= start_ms].copy()
        df_test.reset_index(drop=True, inplace=True)

        if df_test.empty:
            print(f" - 백테스트 구간 데이터 없음: {tf}")
            continue

        # IS/OOS 분리
        df_is = df_test[df_test["open_time"] < boundary_ms].copy()
        df_oos = df_test[df_test["open_time"] >= boundary_ms].copy()

        print(f" - IS rows={len(df_is)}, OOS rows={len(df_oos)}")

        # 인샘플 -> 아웃샘플 백테스트
        is_rows = run_is(df_is, combos=combos, timeframe=tf)
        final_rows = run_oos(df_oos, is_rows, timeframe=tf)

        # 성과지표 CSV
        columns_needed = [
            "timeframe", "is_start_cap", "is_end_cap", "is_return", "is_trades",
            "is_sharpe", "is_mdd", "is_passed",
            "oos_start_cap", "oos_end_cap", "oos_return", "oos_trades",
            "oos_sharpe", "oos_mdd", "used_indicators", "oos_trades_log"
        ]
        df_result = pd.DataFrame(final_rows)
        for col in columns_needed:
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[columns_needed]

        timeframe_folder = os.path.join(RESULTS_DIR, tf)
        os.makedirs(timeframe_folder, exist_ok=True)

        csv_res = os.path.join(timeframe_folder, f"final_{SYMBOL}_{tf}.csv")
        df_result.to_csv(csv_res, index=False, encoding="utf-8")
        print(f" - 성과지표 CSV 저장: {csv_res}, rows={len(df_result)}")

        # OHLCV + 보조지표 CSV
        csv_ind = os.path.join(timeframe_folder, f"ohlcv_with_indicators_{SYMBOL}_{tf}.csv")
        df_test.to_csv(csv_ind, index=False, encoding="utf-8")
        print(f" - OHLCV+보조지표 CSV 저장: {csv_ind}, rows={len(df_test)}")

    print("[main.py] Done. 모든 타임프레임 처리 완료.")


if __name__ == "__main__":
    run_main()
