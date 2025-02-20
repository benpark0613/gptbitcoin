# gptbitcoin/main.py

import os
import pandas as pd

from settings import config
from utils.binance_data import BinanceDataFetcher
from settings.param_combinations import generate_all_combinations
from signals.generate_signals import generate_signals_func
from backtester.oos_evaluation import run_is_oos_evaluation

# 결과 요약 모듈
from utils.results_summary import (
    append_buy_and_hold_result,
    append_combo_results
)
from utils.file_io import save_summary_to_csv

# time_utils (if used for timeframe_hours)
from utils.time_utils import infer_timeframe_hours

def main():
    """
    1) config.TIMEFRAMES 순회해 데이터 로드
    2) run_is_oos_evaluation() -> IS/OOS 백테스트 (IS=lite, OOS=상세)
       - 백테스트 후 metrics에 'start_cap','end_cap'이 들어 있음
    3) B/H + 콤보 성과 append_*함수로 summary_rows에 추가
    4) CSV 저장
    """
    all_combos = generate_all_combinations()
    print(f"Total param combos: {len(all_combos)}")

    summary_rows = []

    for interval in config.TIMEFRAMES:
        print(f"\n=== Processing timeframe: {interval} ===")

        # (1) 바이낸스 데이터 로드
        fetcher = BinanceDataFetcher()
        df = fetcher.fetch_futures_klines(
            symbol=config.SYMBOL,
            interval=interval,
            start_date=config.START_DATE,
            end_date=config.END_DATE
        )
        if df.empty:
            print(f"No data_fetcher fetched for {interval}. Skipping.")
            continue

        df = df.set_index("open_time").sort_index()

        # (2) IS/OOS
        tf_hours = infer_timeframe_hours(interval)
        results = run_is_oos_evaluation(
            df=df,
            param_combinations=all_combos,
            generate_signals_func=generate_signals_func,
            initial_capital=config.INIT_CAPITAL,
            train_ratio=config.TRAIN_RATIO,
            timeframe_hours=tf_hours,
            scale_slippage=True,
            compare_to_buyandhold=True,
            cagr_threshold=0.0,
            sharpe_threshold=0.0
        )

        # B/H 성과
        bh_is  = results["metrics_bh_is"]   # dict
        bh_oos = results["metrics_bh_oos"]  # dict

        # (3) B/H 결과 요약
        append_buy_and_hold_result(summary_rows, interval, bh_is, bh_oos)

        # 콤보별
        is_results     = results["is_results"]
        oos_results    = results["oos_results"]
        excluded_in_is = results["excluded_in_is"]

        # (4) 콤보 결과 요약
        append_combo_results(summary_rows, interval, is_results, oos_results, excluded_in_is)

    # (5) CSV 저장
    if summary_rows:
        df_summary = pd.DataFrame(summary_rows)
        out_csv = os.path.join(config.RESULTS_PATH, "results_summary.csv")
        save_summary_to_csv(df_summary, out_csv)
        print(f"\n[INFO] Summary saved to {out_csv}. Rows={len(df_summary)}")
    else:
        print("[INFO] No summary rows. Possibly no data_fetcher or no TIMEFRAMES processed.")


if __name__ == "__main__":
    main()
