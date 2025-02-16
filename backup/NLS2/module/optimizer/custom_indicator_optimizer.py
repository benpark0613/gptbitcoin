# module/optimizer/custom_indicator_optimizer.py

import json
import pandas as pd
from itertools import product
from datetime import datetime
import multiprocessing
from multiprocessing import Pool
from functools import partial

# 백테스트 함수와 전략 클래스를 프로젝트 내부 모듈에서 임포트합니다.
from backup.NLS2.module.backtester.backtester_bt import run_backtest_bt
from backup.NLS2.module.strategies.nls2_combined import NLS2Combined


def worker_func(combo, keys, df, start_cash, commission, strategy_cls):
    """
    각 조합에 대해 백테스트를 수행하는 글로벌 워커 함수.
    combo: 각 조합 (tuple)
    keys: 파라미터 키 리스트
    df: 백테스트에 사용할 DataFrame
    start_cash: 시작 자금
    commission: 수수료
    strategy_cls: 전략 클래스
    """
    params_dict = dict(zip(keys, combo))
    try:
        bt_result = run_backtest_bt(
            df,
            strategy_cls=strategy_cls,
            strategy_params=params_dict,
            start_cash=start_cash,
            commission=commission,
            plot=False,
            use_progress=False
        )
    except Exception as e:
        return None

    # 백테스트 도중 cash가 0이 되어 조기 종료된 경우 건너뜁니다.
    if hasattr(bt_result, "final_cash") and bt_result.final_cash == 0:
        return None

    result_entry = {
        "total_trades": bt_result.total_trades,
        "sharpe": bt_result.sharpe,
        "max_drawdown_pct": bt_result.max_drawdown_pct,
        "final_value": bt_result.final_value,
        "net_profit": bt_result.net_profit,
        "won_trades": bt_result.won_trades,
        "lost_trades": bt_result.lost_trades,
        "strike_rate": bt_result.strike_rate,
        "annual_return_pct": bt_result.annual_return_pct,
        "sqn": bt_result.sqn,
        "profit_factor": bt_result.profit_factor,
        "avg_win": bt_result.avg_win,
        "avg_loss": bt_result.avg_loss,
        "win_streak": bt_result.win_streak,
        "lose_streak": bt_result.lose_streak,
        "pnl_net": bt_result.pnl_net,
        "pnl_gross": bt_result.pnl_gross,
        "strategy_params": json.dumps(params_dict, ensure_ascii=False)
    }
    return result_entry


def run_all_combinations(df, start_cash, commission, out_csv, config_path, strategy_cls=NLS2Combined):
    """
    JSON 설정 파일(config_path)에 정의된 보조지표 파라미터 조합을 전수 탐색하여,
    각 조합에 대해 백테스트를 멀티프로세싱으로 수행하고 그 결과를 CSV 파일(out_csv)에 저장합니다.
    단, 이 함수는 백테스트 결과의 성과 지표(전략 관련 부분)만 반환하며, 최종 DataFrame을 반환합니다.

    Parameters:
      df (pd.DataFrame): 백테스트에 사용할 데이터
      start_cash (float): 백테스트 시작 자금
      commission (float): 수수료 비율
      out_csv (str): 결과를 저장할 CSV 파일 경로
      config_path (str): 보조지표 조합이 정의된 JSON 설정 파일 경로
      strategy_cls: 사용할 전략 클래스 (기본값: NLS2Combined)
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        param_space = json.load(f)

    keys = list(param_space.keys())
    all_combos = list(product(*param_space.values()))
    total_combinations = len(all_combos)
    print(f"[INFO] Total combinations to test: {total_combinations}")

    results = []
    start_time = datetime.now()
    completed = 0

    worker = partial(worker_func, keys=keys, df=df, start_cash=start_cash, commission=commission,
                     strategy_cls=strategy_cls)

    pool = Pool(processes=multiprocessing.cpu_count())

    for res in pool.imap_unordered(worker, all_combos):
        completed += 1
        print(f"\rProgress: {completed}/{total_combinations}", end="", flush=True)
        if res is not None:
            results.append(res)
    pool.close()
    pool.join()
    print()  # 줄바꿈

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[INFO] Completed testing all combinations in {duration}")

    df_out = pd.DataFrame(results)
    # 결과 컬럼 순서 지정
    columns_order = ["data_start", "data_end", "total_trades", "sharpe", "max_drawdown_pct",
                     "final_value", "net_profit", "won_trades", "lost_trades", "strike_rate",
                     "annual_return_pct", "sqn", "profit_factor", "avg_win", "avg_loss",
                     "win_streak", "lose_streak", "pnl_net", "pnl_gross", "interval", "symbol", "strategy_params"]
    # data_start, data_end, interval, symbol은 이후에 외부에서 추가하므로 임시로 빈 열 추가
    df_out["data_start"] = ""
    df_out["data_end"] = ""
    df_out["interval"] = ""
    df_out["symbol"] = ""
    df_out = df_out.reindex(columns=columns_order)
    df_out.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f"[INFO] All combinations saved to {out_csv}")
    return df_out
