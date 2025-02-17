# statistics/rolling_oos.py

import pandas as pd
from backup.multi_inticator.backtesting.backtester import Backtester
from backup.multi_inticator.strategies.strategy import Strategy
from backup.multi_inticator.statistics.multiple_testing import combine_configs

def rolling_oos_evaluation(data, indicator_configs, strategy_configs,
                           lookback_period=365, oos_period=30, initial_capital=100000):
    """
    1) 일정 구간(lookback_period일)을 in-sample로 써서 모든 (indicator_config×strategy_config) 테스트
    2) 그중 우수 규칙을 선별(간단히 예: 샤프 상위5)
    3) 다음 구간(oos_period일)에서 ensemble(또는 best single)로 OOS 성과 측정
    4) 윈도우를 한 칸씩 굴려가며 반복
    """

    data = data.sort_index()
    start_date = data.index.min()
    end_date = data.index.max()

    results_oos = []
    current_start = start_date

    while True:
        in_sample_end = current_start + pd.Timedelta(days=lookback_period)
        oos_start = in_sample_end + pd.Timedelta(days=1)
        oos_end = oos_start + pd.Timedelta(days=oos_period)

        if in_sample_end >= end_date:
            break
        if oos_end > end_date:
            oos_end = end_date

        df_in = data[current_start:in_sample_end]
        df_oos = data[oos_start:oos_end]
        if len(df_in) < 10 or len(df_oos) < 2:
            break

        # 1) in-sample에서 모든 config 테스트
        ins_results = []
        for ind_cfg in indicator_configs:
            for strat_cfg in strategy_configs:
                merged_cfg = combine_configs(ind_cfg, strat_cfg)
                strategy = Strategy(merged_cfg, initial_capital=initial_capital)
                backtester = Backtester(strategy, df_in, initial_capital=initial_capital)
                backtester.run_backtest()
                metrics = backtester.calculate_metrics()

                ins_results.append({
                    "indicator_config": ind_cfg,
                    "strategy_config": strat_cfg,
                    "metrics": metrics
                })

        df_ins = pd.DataFrame(ins_results)

        # 2) 예: 샤프비율 상위 3개만 선정
        df_ins["sharpe"] = df_ins["metrics"].apply(lambda m: m["Sharpe Ratio"])
        df_ins.sort_values("sharpe", ascending=False, inplace=True)
        top_configs = df_ins.head(3)

        # 3) OOS 성과 평가(동일비중 포트폴리오)
        if len(top_configs) == 0:
            # 우수 규칙 없음 => 현금
            results_oos.append({
                "in_sample_start": current_start,
                "in_sample_end": in_sample_end,
                "oos_start": oos_start,
                "oos_end": oos_end,
                "oos_value": initial_capital,
                "oos_return": 0.0
            })
        else:
            # ensemble
            sum_ret = 0
            for _, rowp in top_configs.iterrows():
                cfg_merged = combine_configs(rowp["indicator_config"], rowp["strategy_config"])
                strategy_oos = Strategy(cfg_merged, initial_capital=initial_capital)
                backtester_oos = Backtester(strategy_oos, df_oos, initial_capital=initial_capital)
                result_df, final_val = backtester_oos.run_backtest()
                # 총수익률
                ret = final_val/initial_capital - 1
                sum_ret += ret
            avg_ret = sum_ret / len(top_configs)
            oos_val = initial_capital*(1.0 + avg_ret)

            results_oos.append({
                "in_sample_start": current_start,
                "in_sample_end": in_sample_end,
                "oos_start": oos_start,
                "oos_end": oos_end,
                "oos_value": oos_val,
                "oos_return": avg_ret
            })

        # 다음 구간으로
        current_start = oos_end + pd.Timedelta(days=1)
        if current_start >= end_date:
            break

    return pd.DataFrame(results_oos)

def main_rolling():
    # 샘플 데이터
    import numpy as np
    dates = pd.date_range("2025-01-01", periods=500, freq="D")
    df = pd.DataFrame({
        "open": np.random.uniform(30000,40000,500),
        "high": np.random.uniform(40000,50000,500),
        "low": np.random.uniform(20000,30000,500),
        "close": np.random.uniform(30000,40000,500),
        "volume": np.random.uniform(100,1000,500)
    }, index=dates)

    # 예시: indicator_configs, strategy_configs
    from backup.multi_inticator.statistics.multiple_testing import (load_indicator_params, load_strategy_params,
                                                                    generate_indicator_configs, generate_strategy_configs)

    indicator_params = load_indicator_params("config/parameters.json")
    strategy_params = load_strategy_params("config/strategy_config.json")

    indicator_configs = generate_indicator_configs(indicator_params)
    strategy_configs = generate_strategy_configs(strategy_params)

    # rolling OOS
    df_oos = rolling_oos_evaluation(df, indicator_configs, strategy_configs,
                                    lookback_period=200, oos_period=60,
                                    initial_capital=100000)
    print(df_oos)

if __name__ == "__main__":
    main_rolling()
