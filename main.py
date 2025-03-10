# gptbitcoin/main.py
"""
메인 실행 스크립트.

1) 지표 콤보 생성
2) DB 업데이트 (recent 모드)
3) prepare_ohlcv_with_warmup로 DB 병합 (워밍업 고려)
4) clean_ohlcv, 보조지표 calc_all_indicators
5) IS/OOS 분할 시 run_is → run_oos, 단일이면 run_nosplit
6) 결과를 data_export 모듈을 통해 CSV/Excel 저장
"""

import csv
import datetime
import os
import pandas as pd
import pytz

# 백테스트 관련 모듈
from backtest.combo_generator import generate_indicator_combos
from backtest.run_is import run_is
from backtest.run_oos import run_oos
from backtest.run_nosplit import run_nosplit

# 환경설정 (보조지표 외 설정)
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

# 전처리(NaN/이상치 검사)
from data.preprocess import clean_ohlcv

# DB 업데이트 (API 요청 → SQLite 저장)
from data.update_data import update_data_db

# 지표 계산
from indicators.indicators import calc_all_indicators

# DB 유틸 (prepare_ohlcv_with_warmup)
from utils.db_utils import prepare_ohlcv_with_warmup

# 지표 파라미터 콤보 계산 시 필요
from utils.indicator_utils import get_required_warmup_bars

# 데이터 내보내기 모듈
from utils.data_export import (
    export_performance,
    export_ohlcv_with_indicators
)


def run_main():
    """
    메인 실행 함수.
    타임프레임별로 DB 업데이트 → 백테스트 → 결과 저장을 수행한다.
    """
    print(f"[main.py] Start - SYMBOL={SYMBOL}, TIMEFRAMES={TIMEFRAMES}, "
          f"LOG_LEVEL={LOG_LEVEL}, USE_IS_OOS={USE_IS_OOS}")

    if not TIMEFRAMES:
        print("[main.py] TIMEFRAMES가 비어있음. 종료.")
        return

    # 1) 지표 파라미터 콤보 생성
    combos = generate_indicator_combos()
    if not combos:
        print("[main.py] combo_generator 결과가 비어있음. 종료.")
        return

    # 2) 보조지표 계산에 필요한 워밍업 봉 수
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)

    # IS/OOS 경계(UTC)
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc
    naive_is_boundary = datetime.datetime.strptime(IS_OOS_BOUNDARY_DATE, dt_format)
    is_boundary_utc = utc.localize(naive_is_boundary)
    is_boundary_str = is_boundary_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # 결과 폴더
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # 모든 TF 결과를 합칠 리스트
    all_perf_rows = []

    # 3) 각 타임프레임 순회
    for tf in TIMEFRAMES:
        print(f"\n[main.py] --- Timeframe: {tf} ---")

        # DB에서 DB_BOUNDARY_DATE ~ END_DATE 구간 삭제 후 재수집 (recent 모드)
        try:
            print(f"[main.py] Delete+ReDownload from {DB_BOUNDARY_DATE} to {END_DATE}, TF={tf}")
            update_data_db(
                symbol=SYMBOL,
                timeframe=tf,
                start_str=DB_BOUNDARY_DATE,  # UTC
                end_str=END_DATE,            # UTC
                update_mode="recent"         # old_data는 수정 안 함
            )
        except Exception as e:
            print(f"[main.py] update_data_db(recent) 오류: {e}")
            continue

        # prepare_ohlcv_with_warmup
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

            # 전처리
            df_merged = clean_ohlcv(df_merged)
            if df_merged.empty:
                print(f"[main.py] 병합 후 DF가 비어 있음: TF={tf}")
                continue

            # 지표 계산
            df_ind = calc_all_indicators(df_merged)

            # 백테스트 메인 구간 필터링
            naive_start = datetime.datetime.strptime(START_DATE, dt_format)
            start_utc_dt = utc.localize(naive_start)
            start_ms = int(start_utc_dt.timestamp() * 1000)

            naive_end = datetime.datetime.strptime(END_DATE, dt_format)
            end_utc_dt = utc.localize(naive_end)
            end_ms = int(end_utc_dt.timestamp() * 1000)

            df_test = df_ind[
                (df_ind["open_time"] >= start_ms) & (df_ind["open_time"] <= end_ms)
            ].copy()
            df_test.reset_index(drop=True, inplace=True)

            if df_test.empty:
                print(f"[main.py] 백테스트 구간 DF가 없음. TF={tf}")
                continue

        except Exception as e:
            print(f"[main.py] prepare/전처리 중 오류: {e}")
            continue

        # IS/OOS 모드
        if USE_IS_OOS:
            print(f"[main.py] IS/OOS 모드, boundary={is_boundary_str}")
            is_boundary_ms = int(is_boundary_utc.timestamp() * 1000)

            df_is = df_test[df_test["open_time"] < is_boundary_ms].copy()
            df_oos = df_test[df_test["open_time"] >= is_boundary_ms].copy()

            print(f" - IS rows={len(df_is)}, OOS rows={len(df_oos)}")

            # 1) IS 전체 콤보 백테스트
            is_rows = run_is(df_is, combos=combos, timeframe=tf)

            # 2) OOS 전체 콤보 백테스트 (IS 통과 여부와 무관)
            oos_rows = run_oos(df_oos, combos=combos, timeframe=tf)

            # 3) IS/OOS 결과 병합 (used_indicators 기준 outer join)
            df_is_ = pd.DataFrame(is_rows)
            df_oos_ = pd.DataFrame(oos_rows)

            merged_df = pd.merge(
                df_is_,
                df_oos_,
                on=["used_indicators", "timeframe"],
                how="outer",
                suffixes=("_is", "_oos")
            )

            # 컬럼 정렬
            columns_order = [
                "timeframe",
                "is_start_cap", "is_end_cap", "is_return", "is_trades",
                "is_sharpe", "is_mdd", "is_passed",
                "oos_start_cap", "oos_end_cap", "oos_return", "oos_trades",
                "oos_sharpe", "oos_mdd", "oos_current_position",
                "used_indicators"
            ]
            # 로그 컬럼 추가
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
            # 단일 구간만 (no IS/OOS)
            print("[main.py] USE_IS_OOS=False => run_nosplit")
            single_rows = run_nosplit(df_test, combos, timeframe=tf)
            all_perf_rows.extend(single_rows)

        # OHLCV+지표 CSV 저장 (data_export 모듈 사용)
        tf_folder = os.path.join(RESULTS_DIR, tf)
        export_ohlcv_with_indicators(df_test, SYMBOL, tf, tf_folder)

    # 최종 성과 지표 (all_perf_rows) 취합 후 저장
    if not all_perf_rows:
        print("[main.py] 수집된 성과지표가 없습니다.")
    else:
        df_perf = pd.DataFrame(all_perf_rows)
        export_performance(df_perf, SYMBOL, RESULTS_DIR, "final_performance")
        print("[main.py] 모든 TF 결과 저장 완료.")

    print("[main.py] Done. 모든 타임프레임 처리 끝.")


if __name__ == "__main__":
    run_main()
