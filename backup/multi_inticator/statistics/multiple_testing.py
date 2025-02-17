# statistics/multiple_testing.py

import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from backup.multi_inticator.backtesting.backtester import Backtester
from backup.multi_inticator.strategies.strategy import Strategy

def run_single_case(case):
    """
    프로세스에서 개별 케이스 실행
    """
    symbol= case["symbol"]
    interval= case["interval"]
    config= case["config"]
    data  = case["data"]
    init_cap= case.get("initial_capital", 100000)

    # 백테스트
    strategy= Strategy(config, initial_capital=init_cap)
    backtester= Backtester(strategy, data, init_cap)
    result_df, final_val= backtester.run_backtest()
    metrics= backtester.calculate_metrics()

    # 딕트로 -> 각 metric을 컬럼화 (소수점 2자리 반올림)
    row={
        "symbol": symbol,
        "interval": interval,
        "final_portfolio_value": round(final_val,2)
    }
    for k,v in metrics.items():
        row[k]= round(v,2)

    # config (인디케이터+전략) 저장 => dict.
    # 필요 시 문자열(json)로 변환 가능
    row["config"]= str(config)
    return row

def run_multiple_tests_parallel(cases):
    """
    cases: list of dict
        각 dict = {"symbol":..., "interval":..., "config":..., "data": DataFrame, "initial_capital":...}
    """
    total= len(cases)
    results=[]
    with ProcessPoolExecutor(max_workers=None) as executor:
        futs=[]
        for c in cases:
            fut= executor.submit(run_single_case, c)
            futs.append(fut)

        # tqdm
        for fut in tqdm(as_completed(futs), total=total, desc="Parallel Testing"):
            row= fut.result()
            results.append(row)

    df_results= pd.DataFrame(results)
    return df_results
