# gptbitcoin/main.py

import pandas as pd
from settings import config
from utils.binance_data import BinanceDataFetcher
from settings.param_combinations import generate_all_combinations
from backtester.oos_evaluation import run_is_oos_evaluation
from signals.generate_signals import generate_signals_func

def main():
    """
    메인 함수:
      1) 바이낸스에서 선물 K라인 데이터를 불러옴
      2) 모든 파라미터 조합(all_combos)에 대해 In-Sample(IS) 평가 후 저조 전략 제외
      3) 남은 전략들만 Out-of-Sample(OOS) 구간에서 재평가
      4) Buy & Hold(IS/OOS) 성과와 최종 통과 전략 수 출력
    """
    # 1) 바이낸스 데이터 불러오기
    fetcher = BinanceDataFetcher()
    symbol = config.SYMBOL
    interval = config.TIMEFRAMES[0] if config.TIMEFRAMES else "4h"
    df = fetcher.fetch_futures_klines(
        symbol=symbol,
        interval=interval,
        start_date=config.START_DATE,
        end_date=config.END_DATE
    )
    if df.empty:
        print("No data fetched. Check API or date range.")
        return

    # 시계열 인덱스로 전처리
    df = df.set_index("open_time").sort_index()

    # 2) 전수 파라미터 조합 생성
    all_combos = generate_all_combinations()

    # 3) IS/OOS 평가 파이프라인
    results = run_is_oos_evaluation(
        df=df,
        param_combinations=all_combos,
        generate_signals_func=generate_signals_func,
        initial_capital=config.INIT_CAPITAL,
        train_ratio=config.TRAIN_RATIO,
        timeframe_hours=4.0 if interval.endswith("h") else 24.0,
        scale_slippage=True,
        compare_to_buyandhold=True,  # Buy & Hold와 성과 비교
        cagr_threshold=0.0,         # 추가 필터(연수익률 등) 없으면 0
        sharpe_threshold=0.0
    )

    # 4) 결과 요약 출력
    print("=== In-Sample Buy & Hold Metrics ===")
    print(results["metrics_bh_is"])
    print("\n=== Out-of-Sample Buy & Hold Metrics ===")
    print(results["metrics_bh_oos"])

    excl_count = len(results["excluded_in_is"])
    print(f"\nExcluded in IS (performance below threshold or B&H): {excl_count}")
    oos_ok_count = len(results["oos_results"])
    print(f"OOS Passed combos: {oos_ok_count}")

    # 예시로 OOS 통과 전략 중 첫 번째 전략의 파라미터와 성과 지표를 출력
    if oos_ok_count > 0:
        sample_key = next(iter(results["oos_results"]))
        sample_params = results["oos_results"][sample_key]["params"]
        sample_metrics = results["oos_results"][sample_key]["metrics"]
        print("\n=== Example OOS Strategy ===")
        print("Params:", sample_params)
        print("Metrics:", sample_metrics)
    else:
        print("\nNo strategies passed OOS evaluation.")

if __name__ == "__main__":
    main()
