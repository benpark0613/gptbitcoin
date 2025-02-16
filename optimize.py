# optimize.py

import os
import time
from datetime import datetime
import pandas as pd

# 최적화 관련 모듈 임포트
from module.optimizer.scipy_optimizer import optimize_strategy_parameters
from module.strategies.nls2_combined import NLS2Combined
from module.data_manager.tf_data_updater import update_csv


def date_to_ms(dstr: str) -> int:
    dt = datetime.strptime(dstr, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


def load_csv_data(csv_path: str) -> pd.DataFrame:
    """CSV 파일을 읽어와서 datetime 인덱스로 변환 후 반환"""
    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"[ERROR] CSV {csv_path} is empty.")
        return df
    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    return df


def main():
    # 사용자 설정
    symbol = "BTCUSDT"
    interval = "15m"
    csv_dir = "data"
    csv_path = os.path.join(csv_dir, f"{symbol}_{interval}.csv")

    start_str = "2023-01-01"  # 최적화 대상 기간 시작
    end_str = None  # None이면 현재 시각까지

    start_ts = date_to_ms(start_str)
    if end_str is None:
        end_ts = int(time.time() * 1000)
    else:
        end_ts = date_to_ms(end_str)

    # CSV 업데이트 (이미 CSV에 데이터가 누적되어 있다면 필요한 구간만 업데이트)
    df_updated = update_csv(symbol, interval, start_ts, end_ts, csv_path)
    print(f"[INFO] CSV updated rows: {len(df_updated)}")

    # CSV 데이터 로드
    df = load_csv_data(csv_path)
    if df.empty:
        return

    # 최적화할 파라미터 설정 (연구 논문 기본값)
    param_config = [
        ('bb_period', 'int'),
        ('bb_dev', 'float'),
        ('vol_fast', 'int'),
        ('vol_slow', 'int')
    ]
    initial_guess = [20, 2.0, 10, 50]
    bounds = [(5, 50), (1.0, 5.0), (5, 50), (5, 200)]

    # 최적화 실행: NLS2Combined 전략을 대상으로 Sharpe Ratio 최대화를 위해 최적화 진행
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

    print("Best parameter vector:", opt_result.x)
    print("Best objective value (negative Sharpe):", opt_result.fun)


if __name__ == "__main__":
    main()
