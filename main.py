# gptbitcoin/main.py
# 구글 스타일, 필요한 최소 한글 주석
# USE_IS_OOS=False => run_nosplit.py로 단일 구간 백테스트 (B/H + combos + trades_log)
# USE_IS_OOS=True  => run_is, run_oos 로직으로 IS/OOS 분리
# 이 버전에서는 DB_BOUNDARY_DATE 이후 구간은 항상 삭제 후 바이낸스 API로 재수집.
# 타임프레임별로 분리해서 저장하던 성과지표 CSV를 최종적으로 하나로 합쳐서
# results 폴더 아래에 저장한다.
# "prepare_ohlcv_with_warmup" 함수를 사용해
# 워밍업 계산 + 조회시점 조정 + DB 병합 로딩을 한 번에 처리.

import os
import datetime
import pandas as pd
import pytz

# 백테스트 관련 모듈
from backtest.combo_generator import generate_indicator_combos
from backtest.run_is import run_is
from backtest.run_nosplit import run_nosplit
from backtest.run_oos import run_oos

# config.py에서 가져오는 설정값들
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
    USE_IS_OOS
)

# DB 업데이트 (API에서 OHLCV 수집 및 저장)
from data.update_data import update_data_db

# 전처리(결측/이상치 등)
from data.preprocess import clean_ohlcv

# 지표 계산
from indicators.indicators import calc_all_indicators

# 지표 파라미터(워밍업 봉) 계산
from utils.indicator_utils import get_required_warmup_bars

# DB 유틸 (prepare_ohlcv_with_warmup 함수)
from utils.db_utils import prepare_ohlcv_with_warmup


def run_main():
    """
    메인 실행 로직:
      1) generate_indicator_combos로 모든 지표 파라미터 조합 생성
      2) DB_BOUNDARY_DATE~END_DATE 구간을 recent_data에서 삭제 후 재수집(update_data_db)
      3) prepare_ohlcv_with_warmup 함수를 통해 DB에서 old_data+recent_data 병합 로드
         - 워밍업(warmup_bars) 고려, EXCHANGE_OPEN_DATE 비교로 실제 시작시점 조정
         - DB_BOUNDARY_DATE를 기준으로 old_data/recent_data 분기
      4) clean_ohlcv, calc_all_indicators 수행 후, 백테스트 메인 구간 필터링
      5) USE_IS_OOS=True 이면 (run_is -> run_oos), 아니면 run_nosplit
      6) (중요) 타임프레임별로 나온 결과를 하나의 CSV로 합치고, OHLCV+지표 CSV는 TF별로 따로 저장
    """
    print(f"[main.py] Start - SYMBOL={SYMBOL}, TIMEFRAMES={TIMEFRAMES}, "
          f"LOG_LEVEL={LOG_LEVEL}, USE_IS_OOS={USE_IS_OOS}")

    # 타임프레임이 비어있으면 종료
    if not TIMEFRAMES:
        print("[main.py] TIMEFRAMES가 비어있습니다. 종료.")
        return

    # 1) 지표 파라미터 조합 생성
    combos = generate_indicator_combos()
    if not combos:
        print("[main.py] combo_generator가 만든 combos가 비어있습니다. 종료.")
        return
    print(f"[main.py] 생성된 지표 콤보 개수: {len(combos)}")

    # 2) 보조지표 계산 시 필요한 워밍업 봉 수 계산
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[main.py] 필요한 워밍업 봉 수: {warmup_bars}")

    # IS/OOS 경계(UTC ms)
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_is_boundary = datetime.datetime.strptime(IS_OOS_BOUNDARY_DATE, dt_format)
    is_boundary_utc = utc.localize(naive_is_boundary)
    is_boundary_ms = int(is_boundary_utc.timestamp() * 1000)

    # 결과를 저장할 디렉토리
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # 모든 타임프레임의 백테스트 결과를 합칠 리스트
    all_performance_rows = []

    # 3) 타임프레임별로 순회
    for tf in TIMEFRAMES:
        print(f"\n[main.py] --- Timeframe: {tf} ---")

        # (A) DB를 업데이트 (DB_BOUNDARY_DATE ~ END_DATE 구간)
        try:
            print(f"[main.py] Delete+ReDownload recent_data {DB_BOUNDARY_DATE} ~ {END_DATE}, TF={tf}")
            update_data_db(
                symbol=SYMBOL,
                timeframe=tf,
                start_str=DB_BOUNDARY_DATE,   # UTC 문자열
                end_str=END_DATE,            # UTC 문자열
                update_mode="recent"         # old_data 테이블은 건드리지 않음
            )
        except Exception as e:
            print(f"[main.py] update_data_db(recent) 오류: {e}")
            continue

        # (B) prepare_ohlcv_with_warmup 함수를 통해
        #     워밍업을 고려하여 DB에서 old_data+recent_data 병합 로딩
        try:
            df_merged = prepare_ohlcv_with_warmup(
                symbol=SYMBOL,
                timeframe=tf,
                start_utc_str=START_DATE,            # 백테스트 메인 시작(UTC)
                end_utc_str=END_DATE,               # 백테스트 종료(UTC)
                warmup_bars=warmup_bars,
                exchange_open_date_utc_str=EXCHANGE_OPEN_DATE,  # 거래소 오픈일(UTC)
                boundary_date_utc_str=DB_BOUNDARY_DATE,         # DB old/recent 경계
                db_path=DB_PATH
            )

            # 전처리(NaN/이상치 검사)
            df_merged = clean_ohlcv(df_merged)
            if df_merged.empty:
                print(f"[main.py] 병합 후 데이터가 비어 있습니다. TF={tf}")
                continue

            # 보조지표 계산
            df_ind = calc_all_indicators(df_merged.copy(), INDICATOR_CONFIG)

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
                print(f"[main.py] 백테스트 구간 데이터가 없음. TF={tf}")
                continue

        except Exception as e:
            print(f"[main.py] prepare_ohlcv_with_warmup/전처리 중 오류: {e}")
            continue

        # (C) IS/OOS 분리 vs 단일구간 백테스트
        timeframe_folder = os.path.join(RESULTS_DIR, tf)
        os.makedirs(timeframe_folder, exist_ok=True)

        if USE_IS_OOS:
            # IS/OOS 분리
            print(f" - IS/OOS 모드: is_boundary_ms={is_boundary_ms}")
            df_is = df_test[df_test["open_time"] < is_boundary_ms].copy()
            df_oos = df_test[df_test["open_time"] >= is_boundary_ms].copy()
            print(f" - IS rows={len(df_is)}, OOS rows={len(df_oos)}")

            # run_is -> run_oos
            is_rows = run_is(df_is, combos=combos, timeframe=tf)
            final_rows = run_oos(df_oos, is_rows, timeframe=tf)
            all_performance_rows.extend(final_rows)

        else:
            # 단일구간 백테스트
            single_rows = run_nosplit(df_test, combos, timeframe=tf)
            all_performance_rows.extend(single_rows)

        # (D) OHLCV+보조지표 CSV 저장
        csv_ind_path = os.path.join(timeframe_folder, f"ohlcv_with_indicators_{SYMBOL}_{tf}.csv")
        df_test.to_csv(csv_ind_path, index=False, encoding="utf-8")
        print(f"[main.py] OHLCV+지표 CSV 저장: {csv_ind_path}, rows={len(df_test)}")

    # (E) 모든 타임프레임 결과를 하나로 합쳐 CSV 저장
    if not all_performance_rows:
        print("[main.py] 성과지표가 없습니다. (모든 TF가 스킵되었을 가능성)")
    else:
        df_perf = pd.DataFrame(all_performance_rows)

        if USE_IS_OOS:
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
        else:
            columns_needed = [
                "timeframe",
                "start_cap",
                "end_cap",
                "returns",
                "trades",
                "sharpe",
                "mdd",
                "used_indicators",
                "trades_log"
            ]

        # 필요한 컬럼이 없으면 채워둠
        for col in columns_needed:
            if col not in df_perf.columns:
                df_perf[col] = None

        df_perf = df_perf[columns_needed]

        final_csv_path = os.path.join(RESULTS_DIR, f"final_performance_{SYMBOL}.csv")
        df_perf.to_csv(final_csv_path, index=False, encoding="utf-8")
        print(f"[main.py] 모든 타임프레임 성과지표 CSV 저장 완료: {final_csv_path}")

    print("[main.py] Done. 모든 타임프레임 처리 완료.")


if __name__ == "__main__":
    run_main()
