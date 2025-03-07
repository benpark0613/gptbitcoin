# gptbitcoin/main.py
# 구글 스타일, 필요한 최소 한글 주석
# USE_IS_OOS=False => run_nosplit.py로 단일 구간 백테스트 (B/H + combos + trades_log)
# USE_IS_OOS=True  => run_is, run_oos 로직으로 IS/OOS 분리

import sys
import os
import pytz
import datetime
import pandas as pd
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
        RESULTS_DIR,
        USE_IS_OOS  # IS/OOS 분리 여부
    )
except ImportError:
    print("[main.py] config.py에서 설정값을 가져오지 못했습니다.")
    sys.exit(1)

try:
    from data.update_data import update_data_db
except ImportError:
    print("[main.py] update_data.py import 에러")
    sys.exit(1)

try:
    from utils.db_utils import connect_db, init_db
except ImportError:
    print("[main.py] db_utils.py import 에러")
    sys.exit(1)

try:
    from data.preprocess import clean_ohlcv, merge_old_recent
except ImportError:
    print("[main.py] preprocess.py import 에러")
    sys.exit(1)

try:
    from indicators.indicators import calc_all_indicators
except ImportError:
    print("[main.py] indicators.py import 에러")
    sys.exit(1)

try:
    from utils.indicator_utils import get_required_warmup_bars
except ImportError:
    print("[main.py] indicator_utils.py import 에러")
    sys.exit(1)

try:
    from backtest.run_is import run_is
    from backtest.run_oos import run_oos
except ImportError:
    print("[main.py] run_is.py, run_oos.py import 에러")
    sys.exit(1)

# "nosplit" 모드의 단일 구간 백테스트 (B/H + combos + trades_log)
try:
    from backtest.run_nosplit import run_nosplit
except ImportError:
    print("[main.py] run_nosplit.py import 에러")
    sys.exit(1)

try:
    from backtest.combo_generator import generate_indicator_combos
except ImportError:
    print("[main.py] combo_generator.py import 에러")
    sys.exit(1)


def _get_time_delta_for_tf(timeframe: str) -> timedelta:
    """
    "1d", "4h", "1h", "15m" 등 문자열을 timedelta로 변환
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
    (start_date, boundary_date, end_date는 모두 UTC 시각 문자열로 가정)
    """
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_start = datetime.datetime.strptime(start_date, dt_format)
    main_start_dt = utc.localize(naive_start)

    naive_boundary = datetime.datetime.strptime(boundary_date, dt_format)
    boundary_dt = utc.localize(naive_boundary)

    naive_end = datetime.datetime.strptime(end_date, dt_format)
    end_dt = utc.localize(naive_end)

    conn = connect_db(DB_PATH)
    init_db(conn)

    delta_per_bar = _get_time_delta_for_tf(timeframe)
    warmup_delta = delta_per_bar * warmup_bars

    # 워밍업 시작점
    naive_exch_open = datetime.datetime.strptime(EXCHANGE_OPEN_DATE, dt_format)
    exch_open_utc = utc.localize(naive_exch_open)

    warmup_start_dt = main_start_dt - warmup_delta
    if warmup_start_dt < exch_open_utc:
        warmup_start_dt = exch_open_utc

    warmup_start_ms = int(warmup_start_dt.timestamp() * 1000)
    boundary_ms = int(boundary_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    df_old = select_ohlcv(conn, "old_data", symbol, timeframe, warmup_start_ms, boundary_ms - 1)
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
    DB에서 recent_data 구간이 (start_date ~ end_date) 범위를 완전히 커버하는지 검사.
    일부만 있거나 전혀 없으면, 부족 구간만 update_data_db(recent) 호출.

    - 만약 end_date <= DB_BOUNDARY_DATE라면 전부 old_data 범위 => recent_data 업데이트 불필요
    """
    from config.config import DB_BOUNDARY_DATE
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

    # end_date가 boundary 이전이면 -> 굳이 recent_data 갱신 안 해도 됨
    if end_ms <= boundary_ts:
        print(f"[ensure_recent_coverage] {start_date}~{end_date} 전부 old_data 구간 => recent_data 불필요")
        return

    conn = connect_db(DB_PATH)
    init_db(conn)

    df_recent = select_ohlcv(conn, "recent_data", symbol, timeframe, start_ms, end_ms)
    conn.close()

    if df_recent.empty:
        print(f"[main.py] recent_data: {timeframe} 구간 {start_date}~{end_date} 전혀 없음 -> update_data_db(recent)")
        try:
            update_data_db(symbol, timeframe, start_date, end_date, update_mode="recent")
        except Exception as e:
            print(f"[ensure_recent_coverage] update_data_db 실패: {e}")
        return

    db_min = df_recent["open_time"].min()
    db_max = df_recent["open_time"].max()

    need_update = False
    missing_start_ms = None
    missing_end_ms = None

    if db_min > start_ms:
        missing_start_ms = start_ms
        missing_end_ms = db_min - 1
        need_update = True

    if db_max < end_ms:
        if not need_update:
            missing_start_ms = db_max + 1
            missing_end_ms = end_ms
            need_update = True
        else:
            if missing_start_ms > db_max + 1:
                pass
            missing_end_ms = end_ms

    if need_update and (missing_start_ms is not None) and (missing_end_ms is not None):
        str_start = _ms_to_str(missing_start_ms)
        str_end = _ms_to_str(missing_end_ms)
        print(f"[main.py] recent_data: {timeframe} 일부 부족 -> update_data_db(recent) {str_start}~{str_end}")
        try:
            update_data_db(symbol, timeframe, str_start, str_end, update_mode="recent")
        except Exception as e:
            print(f"[ensure_recent_coverage] update_data_db 실패: {e}")


def run_main():
    """
    메인 실행 로직:
      1) combo_generator로 모든 지표 파라미터 조합 생성
      2) (START_DATE~END_DATE) 구간에서 recent_data 테이블이 완전히 커버되는지 확인
      3) DB 로드 + 지표 계산 (warmup+백테스트)
      4) USE_IS_OOS=True => IS/OOS 분리(run_is, run_oos), False => run_nosplit
      5) 성과지표 CSV + (OHLCV+보조지표) CSV 저장
      * START_DATE~END_DATE 범위만 CSV에 포함
    """
    print(f"[main.py] Start - SYMBOL={SYMBOL}, TIMEFRAMES={TIMEFRAMES}, LOG_LEVEL={LOG_LEVEL}, USE_IS_OOS={USE_IS_OOS}")

    if not TIMEFRAMES:
        print("[main.py] TIMEFRAMES가 비어있습니다. 종료.")
        return

    # 1) 콤보 생성
    combos = generate_indicator_combos()
    if not combos:
        print("[main.py] combo_generator가 만든 combos가 비어있습니다. 종료.")
        return
    print(f"[main.py] 생성된 지표 콤보 개수: {len(combos)}")

    # 2) 워밍업 봉 계산
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[main.py] 필요한 워밍업 봉 수: {warmup_bars}")

    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_boundary = datetime.datetime.strptime(IS_OOS_BOUNDARY_DATE, dt_format)
    boundary_utc = utc.localize(naive_boundary)
    boundary_ms = int(boundary_utc.timestamp() * 1000)

    # 결과 폴더
    os.makedirs(RESULTS_DIR, exist_ok=True)

    for tf in TIMEFRAMES:
        print(f"\n[main.py] --- Timeframe: {tf} ---")

        # 3) recent_data 누락분 보충
        ensure_recent_coverage(SYMBOL, tf, START_DATE, END_DATE)

        # 4) DB에서 (warmup+백테스트) 구간 로드 + 지표 계산
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

        # 백테스트 구간 필터: START_DATE~END_DATE
        naive_start = datetime.datetime.strptime(START_DATE, dt_format)
        start_utc = utc.localize(naive_start)
        start_ms = int(start_utc.timestamp() * 1000)

        naive_end = datetime.datetime.strptime(END_DATE, dt_format)
        end_utc = utc.localize(naive_end)
        end_ms = int(end_utc.timestamp() * 1000)

        df_test = df_ind[
            (df_ind["open_time"] >= start_ms) &
            (df_ind["open_time"] <= end_ms)
        ].copy()
        df_test.reset_index(drop=True, inplace=True)

        if df_test.empty:
            print(f" - 백테스트 구간 데이터 없음: {tf}")
            continue

        timeframe_folder = os.path.join(RESULTS_DIR, tf)
        os.makedirs(timeframe_folder, exist_ok=True)

        # 5) USE_IS_OOS 분기
        if USE_IS_OOS:
            # --- IS/OOS 분리 ---
            df_is = df_test[df_test["open_time"] < boundary_ms].copy()
            df_oos = df_test[df_test["open_time"] >= boundary_ms].copy()
            print(f" - IS rows={len(df_is)}, OOS rows={len(df_oos)}")

            is_rows = run_is(df_is, combos=combos, timeframe=tf)
            final_rows = run_oos(df_oos, is_rows, timeframe=tf)

            # CSV 저장 (IS/OOS)
            columns_needed = [
                "timeframe",
                "is_start_cap",
                "is_end_cap",
                "is_return",
                "is_trades",
                "is_sharpe",
                "is_mdd",
                "is_passed",
                "oos_start_cap",
                "oos_end_cap",
                "oos_return",
                "oos_trades",
                "oos_sharpe",
                "oos_mdd",
                "used_indicators",
                "oos_trades_log"
            ]
            df_result = pd.DataFrame(final_rows)
            for col in columns_needed:
                if col not in df_result.columns:
                    df_result[col] = None
            df_result = df_result[columns_needed]

            csv_res = os.path.join(timeframe_folder, f"final_{SYMBOL}_{tf}.csv")
            df_result.to_csv(csv_res, index=False, encoding="utf-8")
            print(f" - (IS/OOS) 성과지표 CSV 저장: {csv_res}, rows={len(df_result)}")

        else:
            # --- 단일 구간(nosplit) ---
            # run_nosplit이 "trades_log"까지 포함한 리스트[dict]를 반환
            single_rows = run_nosplit(df_test, combos, timeframe=tf)
            df_single = pd.DataFrame(single_rows)

            # "trades_log" 컬럼이 자동 생성되었더라도, 혹시 누락된 경우를 위해 보정
            columns_needed = [
                "timeframe",
                "start_cap",
                "end_cap",
                "returns",
                "trades",
                "sharpe",
                "mdd",
                "used_indicators",
                "trades_log"  # <-- 추가
            ]
            for col in columns_needed:
                if col not in df_single.columns:
                    df_single[col] = None

            # 원하는 순서대로 재배치
            df_single = df_single[columns_needed]

            csv_res = os.path.join(timeframe_folder, f"final_{SYMBOL}_{tf}_nosplit.csv")
            df_single.to_csv(csv_res, index=False, encoding="utf-8")
            print(f" - (NoSplit) 성과지표 CSV 저장: {csv_res}, rows={len(df_single)}")

        # OHLCV+보조지표 CSV (백테스트에 사용된 df_test)
        csv_ind = os.path.join(timeframe_folder, f"ohlcv_with_indicators_{SYMBOL}_{tf}.csv")
        df_test.to_csv(csv_ind, index=False, encoding="utf-8")
        print(f" - OHLCV+보조지표 CSV 저장: {csv_ind}, rows={len(df_test)}")

    print("[main.py] Done. 모든 타임프레임 처리 완료.")


if __name__ == "__main__":
    run_main()
