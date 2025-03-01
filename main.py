# gptbitcoin/main.py
# 구글 스타일, 최소한의 한글 주석
#
# 주요 변경점:
#   - config.py의 START_CAPITAL 값을 백테스트(run_is, run_oos)에 전달하여
#     디폴트 100000 대신, 사용자가 config에서 설정한 초기자금이 반영되도록 수정.

import os
import sys
import csv
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

from config.config import (
    DB_PATH, SYMBOL, START_DATE, END_DATE, BOUNDARY_DATE,
    INSAMPLE_RATIO, TIMEFRAMES, RESULTS_DIR,
    START_CAPITAL  # <-- 중요: 이 값을 실제로 백테스트에 전달
)
from data.fetch_data import fetch_ohlcv, klines_to_dataframe
from data.preprocess import (
    preprocess_ohlcv_data,
    preprocess_incremental_ohlcv_data
)
from backtest.combo_generator import generate_multi_indicator_combos
from backtest.run_is import run_is
from backtest.run_oos import run_oos

def main() -> None:
    """
    main.py:
      1) DB에서 BOUNDARY_DATE 이전 구간을 읽어 old_df
      2) BOUNDARY_DATE ~ END_DATE 구간을 fetch -> 지표 계산 -> DB에 저장 -> new_df
      3) old_df + new_df => final_df
      4) In-Sample / Out-of-Sample 분할 후 (run_is, run_oos)로 백테스트
      5) 결과를 CSV에 저장
      - 이번 버전에서는 config.START_CAPITAL을 run_is / run_oos에 넘겨
        실제로 사용자 지정 초기자금을 반영
    """
    print("[INFO] Main pipeline start")

    start_dt = pd.to_datetime(START_DATE)
    end_dt   = pd.to_datetime(END_DATE)
    boundary_dt = pd.to_datetime(BOUNDARY_DATE)

    for tf in TIMEFRAMES:
        print(f"\n[INFO] Timeframe: {tf}")

        try:
            # 1) DB에서 old_df (start_date ~ boundary_date - 1)
            old_df = _load_indicators_from_db(
                symbol=SYMBOL,
                interval=tf,
                from_dt=start_dt,
                to_dt=boundary_dt - timedelta(days=1)
            )

            # 2) API에서 new_df (boundary_date ~ end_date)
            new_df = _fetch_new_ohlcv(SYMBOL, tf, boundary_dt, end_dt)
            if new_df.empty:
                print(f"[WARN] {tf}: No new data fetched. Using only old_df.")
                final_df = old_df
            else:
                # tail + new => incremental 지표
                max_tail = 200
                df_old_tail = old_df.tail(max_tail) if len(old_df) >= max_tail else old_df
                new_ind = preprocess_incremental_ohlcv_data(
                    df_new=new_df,
                    df_old_tail=df_old_tail,
                    dropna_indicators=False
                )
                # DB에 저장
                _insert_indicators(SYMBOL, tf, new_ind, boundary_dt)

                # 3) old_df + new_ind => final_df
                final_df = _merge_two_periods(old_df, new_ind, start_dt, end_dt)

            if final_df.empty or len(final_df) < 2:
                print(f"[WARN] {tf}: final_df empty or too short, skip.")
                continue

            # 4) 백테스트 (IS/OOS)
            combos = generate_multi_indicator_combos()
            print(f"[INFO] combos count = {len(combos)}")

            # 여기서 START_CAPITAL을 넘겨 준다:
            df_is, df_oos = _split_is_oos(final_df, INSAMPLE_RATIO)
            is_rows = run_is(df_is, combos, timeframe=tf, start_capital=START_CAPITAL)
            if len(df_oos) > 0:
                final_rows = run_oos(df_oos, is_rows, timeframe=tf, start_capital=START_CAPITAL)
            else:
                final_rows = is_rows

            # 5) 결과 CSV 저장
            out_csv = os.path.join(RESULTS_DIR, f"final_results_{tf}.csv")
            _save_final_csv(final_rows, out_csv)
            print(f"[INFO] CSV saved => {out_csv}")

        except Exception as e:
            print(f"[ERROR] {tf} error: {e}")
            sys.exit(1)

    print("[INFO] Main pipeline done")


def _fetch_new_ohlcv(symbol: str, interval: str,
                     from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """
    바이낸스 API에서 [from_dt ~ to_dt] 봉 데이터를 fetch -> DataFrame
    """
    if from_dt > to_dt:
        return pd.DataFrame()

    start_str = from_dt.strftime("%Y-%m-%d")
    end_str   = to_dt.strftime("%Y-%m-%d")
    print(f"[INFO] fetch_ohlcv => symbol={symbol}, interval={interval}, start={start_str}, end={end_str}")

    klines = fetch_ohlcv(symbol, interval, start_str, end_str)
    if not klines:
        return pd.DataFrame()

    df_raw = klines_to_dataframe(klines)
    if df_raw.empty:
        return pd.DataFrame()

    # datetime 변환
    df_raw["datetime_utc"] = pd.to_datetime(df_raw["datetime_utc"], errors="coerce")
    df_raw.sort_values("datetime_utc", inplace=True)
    df_raw.reset_index(drop=True, inplace=True)
    return df_raw


def _save_final_csv(rows: List[Dict[str,Any]], out_path: str):
    """
    백테스트 결과(각 combo)의 IS/OOS 성과를 CSV로 저장
    """
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
                "timeframe": row.get("timeframe",""),
                "is_start_cap": fmt_val(row.get("is_start_cap",0)),
                "is_end_cap": fmt_val(row.get("is_end_cap",0)),
                "is_return": fmt_val(row.get("is_return",0)),
                "is_trades": str(row.get("is_trades","")),
                "is_passed": str(row.get("is_passed","False")),
                "oos_start_cap": fmt_val(row.get("oos_start_cap","")),
                "oos_end_cap": fmt_val(row.get("oos_end_cap","")),
                "oos_return": fmt_val(row.get("oos_return","")),
                "oos_trades": str(row.get("oos_trades","")),
                "oos_cagr": fmt_val(row.get("oos_cagr","")),
                "oos_sharpe": fmt_val(row.get("oos_sharpe","")),
                "oos_mdd": fmt_val(row.get("oos_mdd","")),
                "oos_win_rate": fmt_val(row.get("oos_win_rate","")),
                "oos_profit_factor": fmt_val(row.get("oos_profit_factor","")),
                "oos_avg_holding_period": fmt_val(row.get("oos_avg_holding_period","")),
                "oos_avg_pnl_per_trade": fmt_val(row.get("oos_avg_pnl_per_trade","")),
                "used_indicators": row.get("used_indicators",""),
                "oos_trades_log": row.get("oos_trades_log","")
            })


def _merge_two_periods(old_df: pd.DataFrame, new_df: pd.DataFrame,
                       start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """
    old_df + new_df => [start_dt~end_dt] 범위
    """
    df_all = pd.concat([old_df, new_df], ignore_index=True)
    df_all.sort_values("datetime_utc", inplace=True)
    df_all.reset_index(drop=True, inplace=True)
    mask = (df_all["datetime_utc"] >= start_dt) & (df_all["datetime_utc"] <= end_dt)
    final_df = df_all.loc[mask].copy()
    final_df.reset_index(drop=True, inplace=True)
    return final_df


def _split_is_oos(df: pd.DataFrame, ratio: float):
    """IS/OOS 분할"""
    n = len(df)
    is_count = int(n*ratio)
    df_is = df.iloc[:is_count].copy()
    df_oos= df.iloc[is_count:].copy()
    print(f"[INFO] total={n}, IS={len(df_is)}, OOS={len(df_oos)}")
    return df_is, df_oos


def _load_indicators_from_db(
    symbol: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime
) -> pd.DataFrame:
    """
    DB에서 ohlcv_indicators 테이블 SELECT
    """
    if from_dt > to_dt:
        return pd.DataFrame()

    conn = _get_connection()
    try:
        _create_indicators_table_if_not_exists(conn)
        sql = """
        SELECT datetime_utc, open, high, low, close, volume,
               ma_5, ma_10, ma_20, ma_50, ma_100, ma_200,
               rsi_14, rsi_21, rsi_30,
               obv, obv_sma_5, obv_sma_10, obv_sma_30, obv_sma_50, obv_sma_100
        FROM ohlcv_indicators
        WHERE symbol=? AND interval=?
          AND datetime_utc>=? AND datetime_utc<=?
        ORDER BY datetime_utc ASC
        """
        rows = conn.execute(sql, (
            symbol, interval,
            from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            to_dt.strftime("%Y-%m-%d %H:%M:%S")
        )).fetchall()

        if not rows:
            return pd.DataFrame()

        cols = [desc[0] for desc in conn.execute(sql, (
            symbol, interval,
            from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            to_dt.strftime("%Y-%m-%d %H:%M:%S")
        )).description]
        df = pd.DataFrame(rows, columns=cols)

        for c in df.columns:
            if c!="datetime_utc":
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce")
        df.dropna(subset=["datetime_utc","open","high","low","close","volume"], inplace=True)
        df.sort_values("datetime_utc", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    finally:
        conn.close()


def _insert_indicators(symbol: str, interval: str, df: pd.DataFrame, boundary_dt: datetime):
    """
    df 중 boundary_dt 이상만 ohlcv_indicators 테이블에 INSERT OR REPLACE
    """
    if df.empty:
        return

    df_use = df[df["datetime_utc"] >= boundary_dt].copy()
    if df_use.empty:
        return

    df_use["datetime_utc"] = df_use["datetime_utc"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

    conn = _get_connection()
    try:
        _create_indicators_table_if_not_exists(conn)
        cols = [
            "datetime_utc","open","high","low","close","volume",
            "ma_5","ma_10","ma_20","ma_50","ma_100","ma_200",
            "rsi_14","rsi_21","rsi_30",
            "obv","obv_sma_5","obv_sma_10","obv_sma_30","obv_sma_50","obv_sma_100"
        ]
        col_str = ",".join(cols)
        placeholders = ",".join(["?"]*len(cols))
        sql = f"""
        INSERT OR REPLACE INTO ohlcv_indicators
        (symbol, interval, {col_str})
        VALUES(?, ?, {placeholders})
        """

        rows_to_insert = []
        for _, row in df_use.iterrows():
            vals = [row.get(c, None) for c in cols]
            rows_to_insert.append((symbol, interval, *vals))

        conn.executemany(sql, rows_to_insert)
        conn.commit()
        print(f"[INFO] Insert {len(rows_to_insert)} rows => ohlcv_indicators.")
    finally:
        conn.close()


def _create_indicators_table_if_not_exists(conn: sqlite3.Connection):
    sql = """
    CREATE TABLE IF NOT EXISTS ohlcv_indicators (
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        datetime_utc TEXT NOT NULL,

        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,

        ma_5 REAL,
        ma_10 REAL,
        ma_20 REAL,
        ma_50 REAL,
        ma_100 REAL,
        ma_200 REAL,

        rsi_14 REAL,
        rsi_21 REAL,
        rsi_30 REAL,

        obv REAL,
        obv_sma_5 REAL,
        obv_sma_10 REAL,
        obv_sma_30 REAL,
        obv_sma_50 REAL,
        obv_sma_100 REAL,

        PRIMARY KEY (symbol, interval, datetime_utc)
    )
    """
    conn.execute(sql)
    conn.commit()


def _get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


if __name__ == "__main__":
    main()