# gptbitcoin/main.py

import os
import sys
import csv
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

from config.config import (
    SYMBOL,
    START_DATE,
    END_DATE,
    WARMUP_BARS,
    INSAMPLE_RATIO,
    TIMEFRAMES,
    RESULTS_DIR
)
from data.fetch_data import fetch_ohlcv_csv
from data.preprocess import preprocess_csv
from backtest.combo_generator import generate_multi_indicator_combos
from backtest.run_is import run_is
from backtest.run_oos import run_oos

def main() -> None:
    print("[INFO] 메인 파이프라인 시작")

    for tf in TIMEFRAMES:
        print(f"\n[INFO] Timeframe: {tf}")
        try:
            # 1) 원본 CSV 다운로드(워밍업 포함)
            csv_in = fetch_ohlcv_csv(
                symbol=SYMBOL,
                interval=tf,
                start_str=START_DATE,
                end_str=END_DATE,
                warmup_bars=WARMUP_BARS
            )
            print(f"[INFO] 원본 CSV 다운로드 -> {csv_in}")

            # 2) 보조지표 계산 + CSV 저장
            csv_out = csv_in.replace(".csv", "_with_indicators.csv")
            preprocess_csv(csv_in, csv_out, dropna=False)
            print(f"[INFO] 지표추가 CSV -> {csv_out}")

            # 3) CSV 로드, 워밍업 제거
            df = pd.read_csv(csv_out)
            df.sort_values("datetime_utc", inplace=True)
            df.reset_index(drop=True, inplace=True)

            df["datetime_utc"] = pd.to_datetime(
                df["datetime_utc"],
                format="%Y-%m-%d %H:%M:%S",
                errors="coerce"
            )
            start_dt = pd.to_datetime(START_DATE)
            end_dt   = pd.to_datetime(END_DATE)
            df = df[(df["datetime_utc"] >= start_dt) & (df["datetime_utc"] <= end_dt)]
            df.reset_index(drop=True, inplace=True)

            n = len(df)
            if n < 2:
                print(f"[WARN] {tf}: 데이터={n}개, 테스트 불가")
                continue

            # 4) IS/OOS 분할
            is_count = int(n * INSAMPLE_RATIO)
            df_is = df.iloc[:is_count].copy()
            df_oos = df.iloc[is_count:].copy()
            print(f"[INFO] 분할: total={n}, IS={len(df_is)}, OOS={len(df_oos)}")

            # 5) 콤보 생성
            combos = generate_multi_indicator_combos()
            print(f"[INFO] 콤보 갯수={len(combos)}")

            # 6) IS 백테스트
            is_rows = run_is(df_is, combos, timeframe=tf)

            # 7) OOS 백테스트
            if not df_oos.empty:
                final_rows = run_oos(df_oos, is_rows, timeframe=tf)
            else:
                final_rows = is_rows

            # 8) 최종 CSV (IS+OOS)
            final_csv = os.path.join(RESULTS_DIR, f"final_results_{tf}.csv")
            _save_final_csv(final_rows, final_csv)
            print(f"[INFO] 최종 CSV -> {final_csv}")

        except Exception as e:
            print(f"[ERROR] {tf} 처리 오류: {e}")
            sys.exit(1)

    print("[INFO] 메인 파이프라인 완료")


def _save_final_csv(rows: List[Dict[str, Any]], out_path: str) -> None:
    fieldnames = [
        "timeframe",
        "is_start_cap",
        "is_end_cap",
        "is_return",
        "is_trades",
        "is_passed",
        "oos_start_cap",
        "oos_end_cap",
        "oos_return",
        "oos_trades",
        "oos_cagr",
        "oos_sharpe",
        "oos_mdd",
        "oos_win_rate",
        "oos_profit_factor",
        "oos_avg_holding_period",
        "oos_avg_pnl_per_trade",
        "used_indicators",
        # 별도 OOS 거래로그: oos_trades_log
        "oos_trades_log"
    ]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    def fmt_f(val):
        if isinstance(val, float):
            return f"{val:.2f}"
        return str(val) if val is not None else ""

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "timeframe": row.get("timeframe",""),
                "is_start_cap": fmt_f(row.get("is_start_cap",0)),
                "is_end_cap": fmt_f(row.get("is_end_cap",0)),
                "is_return": fmt_f(row.get("is_return",0)),
                "is_trades": str(row.get("is_trades",0)),
                "is_passed": row.get("is_passed","False"),
                "oos_start_cap": fmt_f(row.get("oos_start_cap","")),
                "oos_end_cap": fmt_f(row.get("oos_end_cap","")),
                "oos_return": fmt_f(row.get("oos_return","")),
                "oos_trades": str(row.get("oos_trades","")),
                "oos_cagr": fmt_f(row.get("oos_cagr","")),
                "oos_sharpe": fmt_f(row.get("oos_sharpe","")),
                "oos_mdd": fmt_f(row.get("oos_mdd","")),
                "oos_win_rate": fmt_f(row.get("oos_win_rate","")),
                "oos_profit_factor": fmt_f(row.get("oos_profit_factor","")),
                "oos_avg_holding_period": fmt_f(row.get("oos_avg_holding_period","")),
                "oos_avg_pnl_per_trade": fmt_f(row.get("oos_avg_pnl_per_trade","")),
                "used_indicators": row.get("used_indicators",""),
                # OOS 거래기록
                "oos_trades_log": row.get("oos_trades_log","")
            })


if __name__ == "__main__":
    main()
