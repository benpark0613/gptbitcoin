# gptbitcoin/test/aggregator_test.py
# param_generator_for_aggregation.py 모듈(= 한번에 보조지표 계산)을 활용하여,
# DB에서 불러온 후처리된 OHLCV에 모든 지표를 일괄 계산하고 CSV로 내보내는 테스트 스크립트.
# combo_generator_for_backtest.py 같은 백테스트용 조합은 여기서 사용하지 않는다.

"""
이 스크립트는 다음 과정을 수행한다:
  1) DB 업데이트 (update_data_db)
  2) prepare_ohlcv_with_warmup()로 OHLCV (워밍업 포함) 가져오기
  3) clean_ohlcv()로 전처리
  4) param_generator_for_aggregation.calc_all_indicators_for_aggregation() 호출 → 모든 보조지표 일괄 계산
  5) 최종적으로 [START_DATE, END_DATE] 구간만 필터링 후 CSV 파일로 저장

주의:
 - config/config.py에 정의된 SYMBOL, TIMEFRAMES, START_DATE, END_DATE, DB_PATH 등이 사용됨
 - param_generator_for_aggregation.py의 calc_all_indicators_for_aggregation에서
   config/indicator_config.py 설정을 바탕으로 모든 지표를 계산.
 - combo_generator_for_backtest.py 등은 이 스크립트에서 사용하지 않는다.

사용 예:
  python -m gptbitcoin.test.aggregator_test
"""

import os
import datetime
import pytz
import pandas as pd

# 설정
from config.config import (
    SYMBOL,
    TIMEFRAMES,
    START_DATE,
    END_DATE,
    DB_PATH,
    DB_BOUNDARY_DATE,
    EXCHANGE_OPEN_DATE,
    RESULTS_DIR,
)

# DB 업데이트, 전처리
from data.update_data import update_data_db
from data.preprocess import clean_ohlcv

# 워밍업, DB 병합
from utils.db_utils import prepare_ohlcv_with_warmup
from utils.indicator_utils import get_required_warmup_bars

# "보조지표 일괄 계산" 함수
from indicators.param_generator_for_aggregation import calc_all_indicators_for_aggregation
from config.indicator_config import INDICATOR_CONFIG


def run_aggregator_test() -> None:
    """
    1) DB 업데이트
    2) prepare_ohlcv_with_warmup 로딩 & clean
    3) param_generator_for_aggregation.calc_all_indicators_for_aggregation로
       config에 정의된 모든 보조지표 계산
    4) CSV 저장
    """
    print(f"[aggregator_test] Start - SYMBOL={SYMBOL}, TIMEFRAMES={TIMEFRAMES}")

    if not TIMEFRAMES:
        print("[aggregator_test] TIMEFRAMES가 비어 있습니다. 종료.")
        return

    # 날짜
    dt_format = "%Y-%m-%d %H:%M:%S"
    utc = pytz.utc

    naive_start = datetime.datetime.strptime(START_DATE, dt_format)
    start_utc_dt = utc.localize(naive_start)
    start_ms = int(start_utc_dt.timestamp() * 1000)

    naive_end = datetime.datetime.strptime(END_DATE, dt_format)
    end_utc_dt = utc.localize(naive_end)
    end_ms = int(end_utc_dt.timestamp() * 1000)

    # 워밍업 계산
    # config/indicator_config.py를 보고 필요한 봉 수를 구할 수 있음
    warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)

    # 결과 폴더
    os.makedirs(RESULTS_DIR, exist_ok=True)

    for tf in TIMEFRAMES:
        print(f"\n[aggregator_test] Timeframe: {tf}")

        # (A) DB 업데이트
        print(f"[aggregator_test] Update DB from {DB_BOUNDARY_DATE} to {END_DATE}, TF={tf}, mode=recent")
        try:
            update_data_db(
                symbol=SYMBOL,
                timeframe=tf,
                start_str=DB_BOUNDARY_DATE,
                end_str=END_DATE,
                update_mode="recent"
            )
        except Exception as e:
            print(f"[aggregator_test] update_data_db 예외 발생: {e}")
            continue

        # (B) OHLCV 준비 + warmup
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
            print(f"[aggregator_test] df_merged.shape={df_merged.shape}")

            # (C) clean
            df_clean = clean_ohlcv(df_merged)
            if df_clean.empty:
                print(f"[aggregator_test] 데이터프레임이 비어 있음. TF={tf}")
                continue

            # DatetimeIndex 설정
            if "open_time" not in df_clean.columns:
                print("[aggregator_test] 'open_time' 칼럼이 없음. 종료.")
                continue
            df_clean["datetime"] = pd.to_datetime(df_clean["open_time"], unit="ms")
            df_clean.set_index("datetime", inplace=True)
            df_clean.sort_index(inplace=True)

            print(f"[aggregator_test] df_clean.shape={df_clean.shape}")

            # (D) config 기반으로 보조지표 일괄 계산
            df_with_ind = df_clean.copy()
            df_with_ind = calc_all_indicators_for_aggregation(df_with_ind, INDICATOR_CONFIG)
            print(f"[aggregator_test] After calc_all_indicators. shape={df_with_ind.shape}")

            # (E) 최종 필터링 (START_DATE ~ END_DATE 구간)
            df_result = df_with_ind[
                (df_with_ind["open_time"] >= start_ms) &
                (df_with_ind["open_time"] <= end_ms)
            ].copy()
            df_result.reset_index(drop=True, inplace=True)

            if df_result.empty:
                print(f"[aggregator_test] 최종 필터 후 데이터가 없음. TF={tf}")
                continue

            # (F) CSV 저장
            csv_filename = f"aggregator_test_{tf}.csv"
            csv_path = os.path.join(RESULTS_DIR, csv_filename)
            df_result.to_csv(csv_path, index=False)
            print(f"[aggregator_test] TF={tf} → CSV export 완료: {csv_path}")

        except Exception as e:
            print(f"[aggregator_test] 예외 발생: {e}")
            continue


def main():
    run_aggregator_test()


if __name__ == "__main__":
    main()
