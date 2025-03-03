# gptbitcoin/main.py
# 구글 스타일, 최소한의 한글 주석
#
# Filter, Support_Resistance, Channel_Breakout 컬럼까지 모두 DB에 저장/조회하도록 수정
# Timestamp 객체를 DB 쿼리에 직접 넘기지 않도록 from_dt, to_dt를 문자열로 변환해서 사용.

import csv
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd

from backtest.combo_generator import generate_multi_indicator_combos
from backtest.run_is import run_is
from backtest.run_oos import run_oos
from config.config import (
    SYMBOL, START_DATE, END_DATE, BOUNDARY_DATE,
    INSAMPLE_RATIO, TIMEFRAMES, RESULTS_DIR,
    START_CAPITAL
)
from data.fetch_data import fetch_ohlcv, klines_to_dataframe
from data.preprocess import preprocess_incremental_ohlcv_data
from utils.db_utils import (
    get_connection,
    load_indicators_from_db,
    insert_indicators
)


def main() -> None:
    """
    메인 파이프라인:
      1) DB에서 BOUNDARY_DATE 이전 구간 old_df 로드
      2) BOUNDARY_DATE ~ END_DATE 구간을 fetch → 지표 계산 → DB 저장 → new_df
      3) old_df + new_df => final_df
      4) In-Sample / Out-of-Sample 분할 후 백테스트 (run_is, run_oos)
      5) 결과 CSV 기록 (is_sharpe, is_mdd 포함)
    """
    print("[INFO] Main pipeline start")

    # 문자열로 먼저 받아서 datetime 변환
    start_dt = pd.to_datetime(START_DATE)
    end_dt = pd.to_datetime(END_DATE)
    boundary_dt = pd.to_datetime(BOUNDARY_DATE)

    for tf in TIMEFRAMES:
        print(f"\n[INFO] Timeframe: {tf}")

        try:
            # 1) DB에서 old_df
            #    → DB 쿼리 시점에 Timestamp가 넘어가지 않도록 반드시 문자열 변환 후 호출
            old_df = _load_old_data_from_db(
                symbol=SYMBOL,
                interval=tf,
                from_dt=start_dt,
                to_dt=boundary_dt - timedelta(days=1)
            )

            # 2) API에서 new_df
            #    boundary_dt, end_dt도 문자열로 변환하여 fetch_ohlcv에 넘긴다.
            new_df = _fetch_new_ohlcv(SYMBOL, tf, boundary_dt, end_dt)
            if new_df.empty:
                print(f"[WARN] {tf}: No new data fetched. Using only old_df.")
                final_df = old_df
            else:
                # 과거 tail (최대 200행) + 새 구간 병합 → 지표 계산
                df_old_tail = old_df.tail(200) if len(old_df) >= 200 else old_df
                df_new_ind = preprocess_incremental_ohlcv_data(
                    df_new=new_df,
                    df_old_tail=df_old_tail,
                    dropna_indicators=False
                )

                # 새 구간 지표 DB 저장
                _insert_new_indicators(SYMBOL, tf, df_new_ind, boundary_dt)
                # 최종 병합 (start_dt ~ end_dt 구간만)
                final_df = _merge_two_periods(old_df, df_new_ind, start_dt, end_dt)

            # 데이터가 없거나 너무 짧으면 스킵
            if final_df.empty or len(final_df) < 2:
                print(f"[WARN] {tf}: final_df empty or too short, skip.")
                continue

            # 3) 백테스트 (IS/OOS)
            combos = generate_multi_indicator_combos()
            print(f"[INFO] combos count = {len(combos)}")

            df_is, df_oos = _split_is_oos(final_df, INSAMPLE_RATIO)
            is_rows = run_is(df_is, combos, timeframe=tf, start_capital=START_CAPITAL)
            if len(df_oos) > 0:
                final_rows = run_oos(df_oos, is_rows, timeframe=tf, start_capital=START_CAPITAL)
            else:
                final_rows = is_rows

            # 4) 결과 CSV 저장
            out_csv = os.path.join(RESULTS_DIR, f"final_results_{tf}.csv")
            _save_final_csv(final_rows, out_csv)
            print(f"[INFO] CSV saved => {out_csv}")

        except Exception as e:
            print(f"[ERROR] {tf} error: {e}")
            sys.exit(1)

    print("[INFO] Main pipeline done")


def _load_old_data_from_db(
    symbol: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime
) -> pd.DataFrame:
    """
    DB에서 (symbol, interval) + [from_dt ~ to_dt] 범위 지표를 불러온다.
    SQLite 쿼리에 Timestamp를 직접 넘기지 않도록 문자열 변환 후 호출.
    """
    if from_dt > to_dt:
        return pd.DataFrame()

    start_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_connection()
    try:
        df = load_indicators_from_db(conn, symbol, interval, start_str, end_str)
    finally:
        conn.close()

    return df


def _fetch_new_ohlcv(
    symbol: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime
) -> pd.DataFrame:
    """
    바이낸스 API에서 [from_dt ~ to_dt] 구간 OHLCV를 가져와 DataFrame 변환.
    """
    if from_dt > to_dt:
        return pd.DataFrame()

    start_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[INFO] fetch_ohlcv => symbol={symbol}, interval={interval}, "
          f"start={start_str}, end={end_str}")

    klines = fetch_ohlcv(symbol, interval, start_str, end_str)
    if not klines:
        return pd.DataFrame()

    df_raw = klines_to_dataframe(klines)
    if df_raw.empty:
        return pd.DataFrame()

    # datetime 변환 및 정렬
    df_raw["datetime_utc"] = pd.to_datetime(df_raw["datetime_utc"], errors="coerce")
    df_raw.sort_values("datetime_utc", inplace=True)
    df_raw.reset_index(drop=True, inplace=True)

    return df_raw


def _insert_new_indicators(
    symbol: str,
    interval: str,
    df: pd.DataFrame,
    boundary_dt: datetime
) -> None:
    """
    boundary_dt 이후 데이터만 DB에 INSERT OR REPLACE.
    """
    if df.empty:
        return

    boundary_str = boundary_dt.strftime("%Y-%m-%d %H:%M:%S")
    df_use = df[df["datetime_utc"] >= boundary_str].copy()
    if df_use.empty:
        return

    all_indicator_cols = [
        "ma_5","ma_10","ma_20","ma_50","ma_100","ma_200",
        "rsi_14","rsi_21","rsi_30",
        "obv","obv_sma_5","obv_sma_10","obv_sma_30","obv_sma_50","obv_sma_100",
        "filter_min_10","filter_max_10","filter_min_20","filter_max_20",
        "sr_min_10","sr_max_10","sr_min_20","sr_max_20",
        "ch_min_14","ch_max_14","ch_min_20","ch_max_20"
    ]

    conn = get_connection()
    try:
        insert_indicators(conn, symbol, interval, df_use, all_indicator_cols)
    finally:
        conn.close()


def _merge_two_periods(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime
) -> pd.DataFrame:
    """
    old_df + new_df를 concat하여 [start_dt ~ end_dt] 범위만 남긴다.
    """
    df_all = pd.concat([old_df, new_df], ignore_index=True)
    df_all.sort_values("datetime_utc", inplace=True)
    df_all.reset_index(drop=True, inplace=True)

    mask = (df_all["datetime_utc"] >= start_dt) & (df_all["datetime_utc"] <= end_dt)
    final_df = df_all.loc[mask].copy()
    final_df.reset_index(drop=True, inplace=True)
    return final_df


def _split_is_oos(df: pd.DataFrame, ratio: float):
    """
    IS/OOS 분할.
    전체 행 개수 * ratio → IS 구간, 나머지 → OOS 구간
    """
    n = len(df)
    is_count = int(n * ratio)
    df_is = df.iloc[:is_count].copy()
    df_oos = df.iloc[is_count:].copy()
    print(f"[INFO] total={n}, IS={len(df_is)}, OOS={len(df_oos)}")
    return df_is, df_oos


def _save_final_csv(rows: List[Dict[str, Any]], out_path: str):
    """
    IS/OOS 결과를 CSV로 저장. (is_sharpe, is_mdd 등도 함께 기록)
    """
    fieldnames = [
        "timeframe",
        "is_start_cap",
        "is_end_cap",
        "is_return",
        "is_trades",
        "is_sharpe",
        "is_mdd",
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
        "oos_trades_log"
    ]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    def fmt_val(v):
        if isinstance(v, float):
            return f"{v:.2f}"
        return str(v) if v is not None else ""

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "timeframe": row.get("timeframe", ""),
                "is_start_cap": fmt_val(row.get("is_start_cap", 0)),
                "is_end_cap": fmt_val(row.get("is_end_cap", 0)),
                "is_return": fmt_val(row.get("is_return", 0)),
                "is_trades": str(row.get("is_trades", "")),
                "is_sharpe": fmt_val(row.get("is_sharpe", "")),
                "is_mdd": fmt_val(row.get("is_mdd", "")),
                "is_passed": str(row.get("is_passed", "False")),
                "oos_start_cap": fmt_val(row.get("oos_start_cap", "")),
                "oos_end_cap": fmt_val(row.get("oos_end_cap", "")),
                "oos_return": fmt_val(row.get("oos_return", "")),
                "oos_trades": str(row.get("oos_trades", "")),
                "oos_cagr": fmt_val(row.get("oos_cagr", "")),
                "oos_sharpe": fmt_val(row.get("oos_sharpe", "")),
                "oos_mdd": fmt_val(row.get("oos_mdd", "")),
                "oos_win_rate": fmt_val(row.get("oos_win_rate", "")),
                "oos_profit_factor": fmt_val(row.get("oos_profit_factor", "")),
                "oos_avg_holding_period": fmt_val(row.get("oos_avg_holding_period", "")),
                "oos_avg_pnl_per_trade": fmt_val(row.get("oos_avg_pnl_per_trade", "")),
                "used_indicators": row.get("used_indicators", ""),
                "oos_trades_log": row.get("oos_trades_log", "")
            })


if __name__ == "__main__":
    main()
