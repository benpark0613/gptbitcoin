# gptbitcoin/main.py
# 구글 스타일, 최소한의 한글 주석
# BOUNDARY_DATE 이전 구간의 보조지표(특히 OBV)값은 DB에 이미 누적 완료된 상태.
# BOUNDARY_DATE 이후 구간만 새로 API fetch 후, incremental 지표 계산(OBV 이어붙이기) 수행.

import os
import sys
import csv
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List

from config.config import (
    DB_PATH, SYMBOL, START_DATE, END_DATE, BOUNDARY_DATE,
    INSAMPLE_RATIO, TIMEFRAMES, INDICATOR_CONFIG, RESULTS_DIR
)

from data.fetch_data import fetch_ohlcv
from data.preprocess import (
    preprocess_ohlcv_data,
    preprocess_incremental_ohlcv_data
)
from backtest.combo_generator import generate_multi_indicator_combos
from backtest.run_is import run_is
from backtest.run_oos import run_oos

def main() -> None:
    """
    Main pipeline:
      1) BOUNDARY_DATE 이전 구간: 이미 DB에 보조지표(OBV 등) 저장 완료.
      2) BOUNDARY_DATE 이후 구간: 새 데이터 fetch ->
         - 이전 구간 마지막 OBV, 마지막 종가를 DB에서 읽음 -> incremental 지표 계산
      3) 두 구간을 합쳐 백테스트 + 결과 CSV
    """
    print("[INFO] Main pipeline start")

    start_dt = pd.to_datetime(START_DATE)
    end_dt = pd.to_datetime(END_DATE)
    boundary_dt = pd.to_datetime(BOUNDARY_DATE)

    for tf in TIMEFRAMES:
        print(f"\n[INFO] Timeframe: {tf}")

        try:
            # A. 과거 구간(START_DATE ~ BOUNDARY_DATE-1) DB에서 읽기
            old_df = _load_indicators_from_db(
                symbol=SYMBOL,
                interval=tf,
                from_dt=start_dt,
                to_dt=boundary_dt - timedelta(days=1)  # boundary 이전까지
            )

            # B. 과거 구간의 마지막 OBV, 마지막 종가 확인
            old_obv_final, old_close_final = _get_old_obv_close_final(old_df)

            # C. 새 구간(BOUNDARY_DATE ~ END_DATE) fetch
            new_df = _fetch_new_ohlcv(SYMBOL, tf, boundary_dt, end_dt)
            if new_df.empty:
                print(f"[WARN] {tf}: new_df empty, skip.")
                final_df = old_df  # 혹시나
            else:
                # D. incremental 지표 계산 (OBV 이어붙이기)
                #    dropna_indicators=False/True는 필요에 맞게
                new_ind = preprocess_incremental_ohlcv_data(
                    df_new=new_df,
                    old_obv_final=old_obv_final,
                    compare_prev_close=old_close_final,
                    dropna_indicators=False
                )

                # E. DB에 (BOUNDARY_DATE 이후) 저장
                _insert_indicators(SYMBOL, tf, new_ind, boundary_dt)

                # F. 과거 + 신규 합침
                final_df = _merge_two_periods(old_df, new_ind, start_dt, end_dt)

            if final_df.empty or len(final_df) < 2:
                print(f"[WARN] {tf}: final_df empty or too short, skip.")
                continue

            # G. 백테스트
            combos = generate_multi_indicator_combos()
            print(f"[INFO] combos={len(combos)}")

            df_is, df_oos = _split_is_oos(final_df, INSAMPLE_RATIO)
            is_rows = run_is(df_is, combos, timeframe=tf)
            if len(df_oos) > 0:
                final_rows = run_oos(df_oos, is_rows, timeframe=tf)
            else:
                final_rows = is_rows

            # H. CSV 저장
            out_csv = os.path.join(RESULTS_DIR, f"final_results_{tf}.csv")
            _save_final_csv(final_rows, out_csv)
            print(f"[INFO] CSV saved => {out_csv}")

        except Exception as e:
            print(f"[ERROR] {tf} error: {e}")
            sys.exit(1)

    print("[INFO] Main pipeline done")


def _load_indicators_from_db(symbol: str, interval: str,
                             from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """DB에서 ohlcv_indicators 테이블을 SELECT해서 DataFrame으로."""
    if from_dt > to_dt:
        return pd.DataFrame()

    conn = _get_connection()
    try:
        _create_indicators_table_if_not_exists(conn)

        sql = """
        SELECT datetime_utc, open, high, low, close, volume,
               ma_5, ma_10, ma_20, ma_50, ma_100, ma_200,
               rsi_14, rsi_21, rsi_30,
               obv, obv_sma_5, obv_sma_10, obv_sma_30, obv_sma_50, obv_sma_100,
               filter_min_10, filter_max_10, filter_min_20, filter_max_20,
               sr_min_10, sr_max_10, sr_min_20, sr_max_20,
               ch_min_14, ch_max_14, ch_min_20, ch_max_20
        FROM ohlcv_indicators
        WHERE symbol=? AND interval=?
          AND datetime_utc >= ? AND datetime_utc <= ?
        ORDER BY datetime_utc ASC
        """
        rows = conn.execute(sql, (
            symbol, interval,
            from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            to_dt.strftime("%Y-%m-%d %H:%M:%S")
        )).fetchall()

        if not rows:
            return pd.DataFrame()

        cols = [desc[0] for desc in conn.execute(sql, (symbol, interval,
            from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            to_dt.strftime("%Y-%m-%d %H:%M:%S"))).description]
        df = pd.DataFrame(rows, columns=cols)
        # float 변환
        for c in df.columns:
            if c != "datetime_utc":
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce")
        df.dropna(subset=["datetime_utc","open","high","low","close","volume"], inplace=True)
        df.sort_values("datetime_utc", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    finally:
        conn.close()


def _get_old_obv_close_final(df: pd.DataFrame):
    """df의 마지막 행에서 obv, close를 반환. 없으면 (0.0, 0.0)."""
    if df.empty:
        return 0.0, 0.0
    last_row = df.iloc[-1]
    old_obv = last_row.get("obv", 0.0)
    old_close = last_row.get("close", 0.0)
    if pd.isna(old_obv):
        old_obv = 0.0
    if pd.isna(old_close):
        old_close = 0.0
    return float(old_obv), float(old_close)


def _fetch_new_ohlcv(symbol: str, interval: str,
                     from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """API fetch -> DataFrame(open,high,low,close,volume)."""
    from data.fetch_data import fetch_ohlcv

    if from_dt > to_dt:
        return pd.DataFrame()

    start_str = from_dt.strftime("%Y-%m-%d")
    end_str   = to_dt.strftime("%Y-%m-%d")
    print(f"[INFO] fetch_ohlcv => symbol={symbol}, interval={interval}, start={start_str}, end={end_str}")

    klines = fetch_ohlcv(symbol, interval, start_str, end_str)
    if not klines:
        return pd.DataFrame()

    df = _klines_to_dataframe(klines)
    return df


def _insert_indicators(symbol: str, interval: str,
                       df: pd.DataFrame, boundary_dt: datetime):
    """df 중 boundary_dt 이상 행을 ohlcv_indicators에 INSERT. datetime->str 변환."""
    if df.empty:
        print("[INFO] _insert_indicators: df empty, skip.")
        return

    # boundary_dt 이상만
    df_use = df[df["datetime_utc"] >= boundary_dt].copy()
    if df_use.empty:
        print("[INFO] no row pass boundary_dt, skip insert.")
        return

    # datetime -> str
    df_use["datetime_utc"] = df_use["datetime_utc"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

    conn = _get_connection()
    try:
        _create_indicators_table_if_not_exists(conn)

        # SQL
        cols = [
            "datetime_utc","open","high","low","close","volume",
            "ma_5","ma_10","ma_20","ma_50","ma_100","ma_200",
            "rsi_14","rsi_21","rsi_30",
            "obv","obv_sma_5","obv_sma_10","obv_sma_30","obv_sma_50","obv_sma_100",
            "filter_min_10","filter_max_10","filter_min_20","filter_max_20",
            "sr_min_10","sr_max_10","sr_min_20","sr_max_20",
            "ch_min_14","ch_max_14","ch_min_20","ch_max_20"
        ]
        col_str = ",".join(cols)
        placeholders = ",".join(["?"]*len(cols))

        sql = f"""
        INSERT OR REPLACE INTO ohlcv_indicators
        (symbol, interval, {col_str})
        VALUES (?, ?, {placeholders})
        """

        rows_to_insert = []
        for _, row in df_use.iterrows():
            vals = [row.get(c, None) for c in cols]
            rows_to_insert.append((symbol, interval, *vals))

        conn.executemany(sql, rows_to_insert)
        conn.commit()
        print(f"[INFO] Inserted {len(rows_to_insert)} rows into ohlcv_indicators.")
    finally:
        conn.close()


def _merge_two_periods(old_df: pd.DataFrame, new_df: pd.DataFrame,
                       start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """과거 + 신규 df 합침 -> start_dt~end_dt 범위."""
    df_all = pd.concat([old_df, new_df], ignore_index=True)
    df_all.sort_values("datetime_utc", inplace=True)
    df_all.reset_index(drop=True, inplace=True)
    mask = (df_all["datetime_utc"] >= start_dt) & (df_all["datetime_utc"] <= end_dt)
    final_df = df_all.loc[mask].copy()
    final_df.reset_index(drop=True, inplace=True)
    return final_df


def _split_is_oos(df: pd.DataFrame, ratio: float):
    n = len(df)
    is_count = int(n*ratio)
    df_is = df.iloc[:is_count].copy()
    df_oos = df.iloc[is_count:].copy()
    print(f"[INFO] total={n}, IS={len(df_is)}, OOS={len(df_oos)}")
    return df_is, df_oos


def _save_final_csv(rows, out_path):
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


def _klines_to_dataframe(klines: list) -> pd.DataFrame:
    data = []
    for row in klines:
        open_ms = int(row[0])
        dt_utc = datetime.utcfromtimestamp(open_ms / 1000.0)
        o_val = float(row[1])
        h_val = float(row[2])
        l_val = float(row[3])
        c_val = float(row[4])
        vol_val= float(row[5])
        data.append([dt_utc, o_val, h_val, l_val, c_val, vol_val])

    df = pd.DataFrame(data, columns=["datetime_utc","open","high","low","close","volume"])
    return df


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

        filter_min_10 REAL,
        filter_max_10 REAL,
        filter_min_20 REAL,
        filter_max_20 REAL,

        sr_min_10 REAL,
        sr_max_10 REAL,
        sr_min_20 REAL,
        sr_max_20 REAL,

        ch_min_14 REAL,
        ch_max_14 REAL,
        ch_min_20 REAL,
        ch_max_20 REAL,

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
