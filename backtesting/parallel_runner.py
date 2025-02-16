# backtesting/parallel_runner.py

import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

from strategies.strategy import Strategy
from backtesting.backtester import Backtester

class ParallelBacktester:
    def __init__(self, cases, max_workers=None):
        """
        :param cases: list of dict, 각각의 케이스(백테스트 파라미터)를 담고 있음.
            예: {
               "symbol": "BTCUSDT",
               "interval": "1h",
               "start_date": "2024-01-01",
               "end_date": "2024-12-31",
               "config": {  // 인디케이터 + 전략 파라미터 통합
                   "MA": {"short_period":12, "long_period":26},
                   "shorting_allowed": true,
                   "time_delay": 2,
                   "holding_period": 6,
                   ...
               },
               "initial_capital": 100000,
               "data": (OHLCV DataFrame)
            }
        :param max_workers: 병렬 실행 시 워커(프로세스) 수 (None이면 기본)
        """
        self.cases = cases
        self.max_workers = max_workers

    def _run_single_case(self, case):
        """
        각 케이스에 대해 Strategy + Backtester를 실행, 성과 반환
        """
        symbol = case["symbol"]
        interval = case["interval"]
        start_date = case["start_date"]
        end_date = case["end_date"]
        config = case["config"]  # 인디케이터 + 전략 파라미터 통합
        initial_capital = case.get("initial_capital", 100000)
        data = case["data"]

        # 전략 생성
        strategy = Strategy(config, initial_capital=initial_capital)
        backtester = Backtester(strategy, data, initial_capital=initial_capital)
        result_df, final_value = backtester.run_backtest()
        metrics = backtester.calculate_metrics()

        return {
            "symbol": symbol,
            "interval": interval,
            "start_date": start_date,
            "end_date": end_date,
            "final_portfolio_value": final_value,
            "metrics": metrics,
            "result_df": result_df
        }

    def run(self):
        """
        모든 케이스를 병렬로 처리
        """
        results = []
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_case = {executor.submit(self._run_single_case, case): case for case in self.cases}
            for future in as_completed(future_to_case):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    case = future_to_case[future]
                    print(f"[ERROR] Case {case} raised an exception: {e}")
        return results


if __name__ == "__main__":
    import numpy as np

    # 샘플 data 생성
    dates = pd.date_range("2025-01-01", periods=100, freq="D")
    sample_data = pd.DataFrame({
        "open": np.random.uniform(30000, 40000, size=100),
        "high": np.random.uniform(40000, 50000, size=100),
        "low": np.random.uniform(20000, 30000, size=100),
        "close": np.random.uniform(30000, 40000, size=100),
        "volume": np.random.uniform(100, 1000, size=100)
    }, index=dates)

    # 예시 case: 인디케이터+전략 파라미터 통합 config
    config_example = {
        "MA": {"short_period": 12, "long_period": 26},
        "shorting_allowed": True,
        "time_delay": 2,
        "holding_period": 5,
        "transaction_fee_rate": 0.0004
        # ...
    }
    case1 = {
        "symbol": "BTCUSDT",
        "interval": "1h",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "config": config_example,
        "initial_capital": 100000,
        "data": sample_data
    }

    # 여러 case 예시 (실제로는 여러 파라미터 조합을 cases에 추가)
    cases = [case1]

    parallel_runner = ParallelBacktester(cases, max_workers=2)
    results = parallel_runner.run()

    for res in results:
        print(f"Symbol: {res['symbol']}, Interval: {res['interval']}, FinalValue: {res['final_portfolio_value']}")
        print("Metrics:", res["metrics"])
        # res["result_df"]에는 백테스트 결과 시계열이 있음
