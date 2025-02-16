# optimize_multi_interval.py

import os
import time
import json
import shutil
from datetime import datetime
import pandas as pd

from module.optimizer.scipy_optimizer import optimize_strategy_parameters
from module.strategies.nls2_combined import NLS2Combined
from module.data_manager.tf_data_updater import update_csv


def date_to_ms(dstr: str) -> int:
    dt = datetime.strptime(dstr, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


def load_csv_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"[ERROR] CSV {csv_path} is empty.")
        return df
    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    return df


def optimize_for_interval(symbol: str, interval: str, start_ts: int, end_ts: int, csv_dir: str,
                          param_config, initial_guess, bounds):
    csv_path = os.path.join(csv_dir, f"{symbol}_{interval}.csv")
    # CSV 업데이트 (필요한 구간만 추가 fetch)
    df_updated = update_csv(symbol, interval, start_ts, end_ts, csv_path)
    print(f"[INFO] CSV updated rows for {interval}: {len(df_updated)}")

    df = load_csv_data(csv_path)
    if df.empty:
        print(f"[ERROR] DataFrame for {interval} is empty.")
        return None

    # 최적화 실행 (NLS2Combined 전략 기준)
    opt_result = optimize_strategy_parameters(
        df,
        NLS2Combined,
        param_config,
        bounds,
        initial_guess,
        start_cash=100000.0,
        commission=0.002,
        method='Nelder-Mead'
    )
    return opt_result


def main():
    # 사용자 설정
    symbol = "BTCUSDT"
    intervals = ["15m", "1h", "4h"]
    start_str = "2023-01-01"  # 최적화 대상 기간 시작
    end_str = None  # None이면 현재 시각까지
    csv_dir = "data"

    start_ts = date_to_ms(start_str)
    if end_str is None:
        end_ts = int(time.time() * 1000)
    else:
        end_ts = date_to_ms(end_str)

    # 최적화할 파라미터 설정 (연구 논문 기본값)
    param_config = [
        ('bb_period', 'int'),
        ('bb_dev', 'float'),
        ('vol_fast', 'int'),
        ('vol_slow', 'int')
    ]
    initial_guess = [20, 2.0, 10, 50]
    bounds = [(5, 50), (1.0, 5.0), (5, 50), (5, 200)]

    optimization_results = []

    for interval in intervals:
        print(f"\n===== Optimizing for interval: {interval} =====")
        opt_result = optimize_for_interval(symbol, interval, start_ts, end_ts, csv_dir,
                                           param_config, initial_guess, bounds)
        if opt_result is not None:
            best_params = opt_result.x.tolist()
            best_objective = opt_result.fun
            # 최적화 결과에 사용한 파라미터 이름과 최적값을 JSON 문자열로 저장
            params_dict = dict(zip([p[0] for p in param_config], best_params))
            result_entry = {
                "interval": interval,
                "best_params": json.dumps(params_dict, ensure_ascii=False),
                "best_objective": round(best_objective, 2),
                "timestamp": datetime.now().isoformat()
            }
            optimization_results.append(result_entry)
            print(f"Best parameters for {interval}: {best_params}")
            print(f"Best objective (negative Sharpe) for {interval}: {best_objective}")

    # CSV 결과 저장: results_optimized 폴더 아래
    if optimization_results:
        df_results = pd.DataFrame(optimization_results)
        results_folder = "results_optimized"
        os.makedirs(results_folder, exist_ok=True)
        output_csv = os.path.join(results_folder, "optimization_results.csv")
        df_results.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\n[INFO] Optimization results saved to {output_csv}")
    else:
        print("[WARN] No optimization results to save.")


if __name__ == "__main__":
    main()
