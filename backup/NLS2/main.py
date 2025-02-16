# main.py

import os
import time
import json
import shutil
import pandas as pd
from datetime import datetime

from backup.NLS2.module import update_csv
from backup.NLS2.module.backtester import run_backtest_bt
from backup.NLS2.module import BuyAndHoldStrategy


##############################################################################
# 1) 유틸 함수
##############################################################################
def clear_results_folder(path: str = "results"):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)

def date_to_ms(dstr: str) -> int:
    dt = datetime.strptime(dstr, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)

def load_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

##############################################################################
# 2) CSV -> DataFrame 슬라이싱
##############################################################################
def fetch_and_prepare_data(symbol, interval, start_ts, end_ts, csv_path):
    """
    update_csv로 전체 히스토리 누적,
    CSV 로드 -> Datetime 인덱스 변환 -> (start_ts~end_ts) 슬라이싱
    """
    df_updated = update_csv(symbol, interval, start_ts, end_ts, csv_path)
    print(f"[INFO] CSV updated rows: {len(df_updated)}")

    df_csv = pd.read_csv(csv_path)
    if df_csv.empty:
        print("[ERROR] CSV is empty after read_csv.")
        return pd.DataFrame()

    df_csv["datetime"] = pd.to_datetime(df_csv["open_time"], unit="ms", utc=True)
    df_csv.set_index("datetime", inplace=True)
    df_csv.sort_index(inplace=True)

    if df_csv.empty:
        print("[ERROR] CSV is empty after datetime conversion.")
        return pd.DataFrame()

    start_dt = pd.to_datetime(start_ts, unit='ms', utc=True)
    end_dt   = pd.to_datetime(end_ts,   unit='ms', utc=True)

    df_sliced = df_csv.loc[(df_csv.index >= start_dt) & (df_csv.index < end_dt)]
    return df_sliced

##############################################################################
# 3) CSV 결과 저장 (성과지표)
##############################################################################
def save_result_to_csv(result, out_csv="results/backtest_results.csv", extra_info=None):
    """
    BacktestResult 객체를 CSV에 누적 저장.
    - 백테스트 구간(data_start, data_end) 및 사용된 보조지표 파라미터(strategy_params)를 함께 기록.
    - 모든 float 값은 소수점 둘째자리까지 반올림.
    """
    result_dict = result.to_dict()

    # float 값 2자리 반올림
    for key, value in result_dict.items():
        if isinstance(value, float):
            result_dict[key] = round(value, 2)

    if extra_info:
        for k, v in extra_info.items():
            # 만약 v가 float이면 반올림 처리
            if isinstance(v, float):
                extra_info[k] = round(v, 2)
            result_dict[k] = v

    df_out = pd.DataFrame([result_dict])

    preferred_order = [
        "data_start",
        "data_end",
        "total_trades",
        "sharpe",
        "max_drawdown_pct",
        "final_value",
        "net_profit",
        "won_trades",
        "lost_trades",
        "strike_rate",
        "annual_return_pct",
        "sqn",
        "profit_factor",
        "avg_win",
        "avg_loss",
        "win_streak",
        "lose_streak",
        "pnl_net",
        "pnl_gross",
        "interval",
        "symbol",
        "strategy_params"  # 새로 추가된 보조지표 파라미터
    ]
    remaining_cols = [c for c in df_out.columns if c not in preferred_order]
    new_order = preferred_order + remaining_cols

    df_out = df_out.reindex(columns=new_order)

    write_header = not os.path.exists(out_csv)
    df_out.to_csv(out_csv, mode='a', header=write_header, index=False, encoding='utf-8-sig')
    print(f"[INFO] CSV 저장 완료: {out_csv}")

##############################################################################
# 4) 백테스트 실행 + 콘솔/CSV 기록
##############################################################################
def run_single_backtest(df, interval, symbol, start_ts, end_ts, strategy_cls, strategy_params, start_cash, commission):
    if df.empty:
        print(f"[ERROR] {interval} df empty => skip")
        return

    data_start_dt = df.index.min()
    data_end_dt   = df.index.max()
    data_start_str = data_start_dt.isoformat()
    data_end_str   = data_end_dt.isoformat()

    print(f"[INFO] Backtest {interval}: Rows={len(df)}, Range={data_start_str} ~ {data_end_str}")

    result = run_backtest_bt(
        df=df,
        strategy_cls=strategy_cls,
        strategy_params=strategy_params,
        start_cash=start_cash,
        commission=commission,
        plot=False,
        use_progress=True
    )

    print("\n===== 백테스트 결과 =====")
    print("Interval:", interval)
    print("Data Start:", data_start_str, " / Data End:", data_end_str)
    print("Sharpe Ratio:", result.sharpe)
    print("MDD (%):", result.max_drawdown_pct)
    print("Final Value:", result.final_value)
    print("Net Profit:", result.net_profit)
    print("Total Trades:", result.total_trades)
    print("PnL net:", result.pnl_net, "/ gross:", result.pnl_gross)

    # extra_info에 strategy_params 추가 (JSON 문자열)
    extra_info = {
        "interval": interval,
        "symbol": symbol,
        "data_start": data_start_str,
        "data_end": data_end_str,
        "strategy_params": json.dumps(strategy_params, ensure_ascii=False)
    }
    save_result_to_csv(
        result,
        out_csv="results/backtest_results.csv",
        extra_info=extra_info
    )

##############################################################################
# 5) 메인 함수
##############################################################################
def main():
    clear_results_folder("../../results")

    symbol = "BTCUSDT"
    intervals = ["4h", "1h"]

    start_str = "2024-01-01"
    end_str = None

    csv_dir = "./data"
    start_cash = 100000.0
    commission = 0.002

    paramfile = "module/config/nls2_params.json"
    try:
        strategy_params = load_json(paramfile)
    except:
        strategy_params = {}

    start_ts = date_to_ms(start_str)
    if end_str is None:
        end_ts = int(time.time() * 1000)
    else:
        end_ts = date_to_ms(end_str)

    # 기존 전략으로 각 interval 백테스트 실행
    for interval in intervals:
        print("\n======================================")
        print(f"=== Interval {interval} 백테스트 시작 ===")
        print("======================================\n")

        csv_path = os.path.join(csv_dir, f"{symbol}_{interval}.csv")
        df_prepared = fetch_and_prepare_data(symbol, interval, start_ts, end_ts, csv_path)
        if df_prepared.empty:
            print(f"[WARN] {interval}: empty df_prepared => skip.")
            continue

        run_single_backtest(
            df=df_prepared,
            interval=interval,
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            strategy_cls=None,  # 기존 논문 전략 사용 (strategy_cls=None이면 내부에서 결정)
            strategy_params=strategy_params,
            start_cash=start_cash,
            commission=commission
        )

    # 가장 큰 타임프레임에 대해 Buy & Hold 전략 실행 (예: 4h)
    largest_tf = intervals[-1]
    csv_path = os.path.join(csv_dir, f"{symbol}_{largest_tf}.csv")
    df_bnh = fetch_and_prepare_data(symbol, largest_tf, start_ts, end_ts, csv_path)
    if not df_bnh.empty:
        run_single_backtest(
            df=df_bnh,
            interval=largest_tf + "(BnH)",
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            strategy_cls=BuyAndHoldStrategy,
            strategy_params={},
            start_cash=start_cash,
            commission=commission
        )

if __name__ == "__main__":
    main()
