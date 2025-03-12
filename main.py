# gptbitcoin/main.py
# 최소한의 한글 주석, 구글 스타일 docstring
"""
메인 실행 스크립트.
1) DB 업데이트 (recent 모드)
2) DB에서 OHLCV 로드 (워밍업 포함)
3) clean_ohlcv → calc_all_indicators (지표는 한 번만 계산)
4) IS/OOS 또는 단일 구간 백테스트 (run_is, run_oos, run_nosplit)
5) CSV/Excel 출력
"""

import os
import datetime
import pytz
import pandas as pd

# 백테스트/콤보 관련
from backtest.combo_generator import generate_indicator_combos
from backtest.run_is import run_is
from backtest.run_oos import run_oos
from backtest.run_nosplit import run_nosplit

# 환경설정
from config.config import (
    SYMBOL,
    TIMEFRAMES,
    START_DATE,
    END_DATE,
    IS_OOS_BOUNDARY_DATE,
    DB_PATH,
    DB_BOUNDARY_DATE,
    EXCHANGE_OPEN_DATE,
    RESULTS_DIR,
    LOG_LEVEL,
    USE_IS_OOS
)
from config.indicator_config import INDICATOR_CONFIG

# DB 업데이트
from data.update_data import update_data_db

# 전처리, 지표 계산
from data.preprocess import clean_ohlcv
from indicators.aggregator import calc_all_indicators

# DB에서 병합 조회
from utils.db_utils import prepare_ohlcv_with_warmup

# 지표 파라미터 유틸
from utils.indicator_utils import get_required_warmup_bars

# 결과 출력
from utils.data_export import (
    export_performance,
    export_ohlcv_with_indicators
)


def run_main():
    """
    메인 실행 함수.

    Steps:
      1) Generate combos
      2) For each timeframe:
         - Update DB (recent mode)
         - Load OHLCV with warmup
         - Clean data, calc indicators (once)
         - Filter main period
         - Run IS/OOS or single backtest
         - Export OHLCV+indicators
      3) Collect performance rows, export CSV/Excel
    """
    print(f"[main.py] Start - SYMBOL={SYMBOL}, TIMEFRAMES={TIMEFRAMES}, "
          f"LOG_LEVEL={LOG_LEVEL}, USE_IS_OOS={USE_IS_OOS}")

    if not TIMEFRAMES:
        print("[main.py] No TIMEFRAMES. Exiting.")
        return

    # 1) combos
    combos = generate_indicator_combos()
    if not combos:
        print("[main.py] No combos generated. Exiting.")
        return

    # 필요 워밍업 (전체 지표 기준)
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)

    # IS/OOS boundary
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc
    naive_is_boundary = datetime.datetime.strptime(IS_OOS_BOUNDARY_DATE, dt_format)
    is_boundary_utc = utc.localize(naive_is_boundary)
    is_boundary_str = is_boundary_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    all_perf_rows = []

    for tf in TIMEFRAMES:
        print(f"\n[main.py] --- Timeframe: {tf} ---")

        # 2) Update DB in "recent" mode
        try:
            print(f"[main.py] Update DB from {DB_BOUNDARY_DATE} to {END_DATE}, TF={tf}, mode=recent")
            update_data_db(
                symbol=SYMBOL,
                timeframe=tf,
                start_str=DB_BOUNDARY_DATE,  # UTC
                end_str=END_DATE,            # UTC
                update_mode="recent"
            )
        except Exception as e:
            print(f"[main.py] update_data_db error: {e}")
            continue

        # 3) Prepare data from DB with warmup
        try:
            df_merged = prepare_ohlcv_with_warmup(
                symbol=SYMBOL,
                timeframe=tf,
                start_utc_str=START_DATE,
                end_utc_str=END_DATE,
                warmup_bars=warmup_bars,
                exchange_open_date_utc_str=EXCHANGE_OPEN_DATE,
                boundary_date_utc_str=DB_BOUNDARY_DATE,
                db_path=DB_PATH
            )
            df_merged = clean_ohlcv(df_merged)
            if df_merged.empty:
                print(f"[main.py] Merged DF empty. TF={tf}")
                continue

            # (a) 여기서 지표를 한 번만 계산
            df_with_ind = calc_all_indicators(df_merged, cfg=INDICATOR_CONFIG)

            # (b) 메인 기간 필터
            naive_start = datetime.datetime.strptime(START_DATE, dt_format)
            start_utc_dt = utc.localize(naive_start)
            start_ms = int(start_utc_dt.timestamp() * 1000)

            naive_end = datetime.datetime.strptime(END_DATE, dt_format)
            end_utc_dt = utc.localize(naive_end)
            end_ms = int(end_utc_dt.timestamp() * 1000)

            df_test = df_with_ind[
                (df_with_ind["open_time"] >= start_ms) & (df_with_ind["open_time"] <= end_ms)
            ].copy()
            df_test.reset_index(drop=True, inplace=True)
            if df_test.empty:
                print(f"[main.py] Backtest DF empty. TF={tf}")
                continue

        except Exception as e:
            print(f"[main.py] prepare data error: {e}")
            continue

        # 4) Backtest
        if USE_IS_OOS:
            print(f"[main.py] IS/OOS mode, boundary={is_boundary_str}")
            is_boundary_ms = int(is_boundary_utc.timestamp() * 1000)
            df_is = df_test[df_test["open_time"] < is_boundary_ms].copy()
            df_oos = df_test[df_test["open_time"] >= is_boundary_ms].copy()
            print(f" - IS rows={len(df_is)}, OOS rows={len(df_oos)}")

            # run_is
            is_rows = run_is(df_is, combos=combos, timeframe=tf)
            # run_oos
            oos_rows = run_oos(df_oos, combos=combos, timeframe=tf)

            df_is_ = pd.DataFrame(is_rows)
            df_oos_ = pd.DataFrame(oos_rows)

            merged_df = pd.merge(
                df_is_, df_oos_,
                on=["used_indicators", "timeframe"],
                how="outer",
                suffixes=("_is", "_oos")
            )

            columns_order = [
                "timeframe",
                "is_start_cap", "is_end_cap", "is_return", "is_trades",
                "is_sharpe", "is_mdd", "is_passed",
                "oos_start_cap", "oos_end_cap", "oos_return", "oos_trades",
                "oos_sharpe", "oos_mdd", "oos_current_position",
                "used_indicators"
            ]
            if "is_trades_log" in merged_df.columns:
                columns_order.append("is_trades_log")
            if "oos_trades_log" in merged_df.columns:
                columns_order.append("oos_trades_log")

            for col in columns_order:
                if col not in merged_df.columns:
                    merged_df[col] = None
            merged_df = merged_df[columns_order]

            final_rows = merged_df.to_dict("records")
            all_perf_rows.extend(final_rows)
        else:
            print("[main.py] Single (No IS/OOS) mode")
            single_rows = run_nosplit(df_test, combos, timeframe=tf)
            all_perf_rows.extend(single_rows)

        # 5) Export OHLCV+indicators CSV
        tf_folder = os.path.join(RESULTS_DIR, tf)
        export_ohlcv_with_indicators(df_test, SYMBOL, tf, tf_folder)

    # 6) Export performance
    if not all_perf_rows:
        print("[main.py] No performance data.")
    else:
        df_perf = pd.DataFrame(all_perf_rows)
        export_performance(df_perf, SYMBOL, RESULTS_DIR, "final_performance")
        print("[main.py] All timeframes done. Output saved.")


if __name__ == "__main__":
    run_main()
