# gptbitcoin/main_best.py
# 구글 스타일, 최소한의 한글 주석
#
# 요구사항:
#   - IS/OOS 분할 없이 전체 구간 백테스트
#   - 오직 거래기록만, 줄바꿈해서 출력
#   - 예) [1] SHORT Entry=2023-08-13 00:00:00, Exit=2023-08-20 00:00:00
#   - 그 외 잡다한 콘솔 로그는 전혀 없어야 함

import os
import sys
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

from config.config import (
    DB_PATH,
    BOUNDARY_DATE,
    START_CAPITAL
)
from data.fetch_data import fetch_ohlcv, klines_to_dataframe
from data.preprocess import preprocess_incremental_ohlcv_data
from backtest.run_best import run_best_combo

def today():
    now = datetime.now()
    a = now.strftime("%Y-%m-%d")
    return a

def main():
    # 사용자가 직접 설정하는 기간 (원하는 대로 수정 가능)
    START_DATE = "2024-01-01"
    END_DATE   = today()

    # 하나의 전략(조합) JSON (원하는 대로 수정 가능)
    example_used_indicators = """
{"timeframe": "1d", "allow_short": true, "indicators": [{"indicator": "MA", "short_period": 5, "long_period": 50, "band_filter": 0.0}]}
    """

    # JSON 파싱
    try:
        user_strat = json.loads(example_used_indicators)
    except json.JSONDecodeError:
        sys.exit(1)

    user_tf = user_strat.get("timeframe", "1d")
    allow_short = user_strat.get("allow_short", False)
    best_combo = user_strat.get("indicators", [])
    if not best_combo:
        sys.exit(1)

    symbol = "BTCUSDT"
    start_dt = pd.to_datetime(START_DATE)
    end_dt   = pd.to_datetime(END_DATE)
    boundary_dt = pd.to_datetime(BOUNDARY_DATE)

    old_df = _load_indicators_from_db(
        symbol=symbol,
        interval=user_tf,
        from_dt=start_dt,
        to_dt=boundary_dt - timedelta(days=1)
    )

    new_df = _fetch_new_ohlcv(symbol, user_tf, boundary_dt, end_dt)
    if new_df.empty:
        final_df = old_df
    else:
        df_old_tail = old_df.tail(200) if len(old_df) >= 200 else old_df
        df_new_ind = preprocess_incremental_ohlcv_data(
            df_new=new_df,
            df_old_tail=df_old_tail,
            dropna_indicators=False
        )
        _insert_indicators(symbol, user_tf, df_new_ind, boundary_dt)
        final_df = _merge_two_periods(old_df, df_new_ind, start_dt, end_dt)

    if final_df.empty or len(final_df) < 2:
        # 데이터가 거의 없으면 거래 기록도 없음
        return

    # 백테스트
    out = run_best_combo(
        df=final_df,
        best_combo=best_combo,
        start_capital=START_CAPITAL,
        allow_short=allow_short
    )

    trades = out["engine_out"]["trades"] if "engine_out" in out else []
    _print_trades_in_one_block(trades, final_df)


def _print_trades_in_one_block(trades: List[Dict[str,Any]], df: pd.DataFrame):
    """
    거래기록만 오직 줄바꿈 형식으로 출력한다.
    예)
      [1] SHORT Entry=2023-08-13 00:00:00, Exit=2023-08-20 00:00:00
      [2] SHORT Entry=..., Exit=...
      ...
    """
    if not trades:
        return

    lines = []
    for i, t in enumerate(trades, start=1):
        pos_type = t.get("position_type","N/A").upper()
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)

        # entry/exit 날짜 추출
        if e_idx is not None and 0 <= e_idx < len(df):
            e_dt = str(df.iloc[e_idx]["datetime_utc"])
        else:
            e_dt = "N/A"
        # exit_index가 len(df) 이상이거나 'End' 표시를 원할 경우
        if x_idx is not None and 0 <= x_idx < len(df):
            x_dt = str(df.iloc[x_idx]["datetime_utc"])
        elif isinstance(x_idx, int) and x_idx >= len(df):
            x_dt = "End"
        else:
            x_dt = "N/A"

        line = f"[{i}] {pos_type} Entry={e_dt}, Exit={x_dt}"
        lines.append(line)

    # 줄바꿈하여 출력
    # 요구대로 "오직 거래기록만" 출력, 다른 로그 없음
    for ln in lines:
        print(ln)


def _fetch_new_ohlcv(symbol: str, interval: str,
                     from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    if from_dt > to_dt:
        return pd.DataFrame()
    start_str = from_dt.strftime("%Y-%m-%d")
    end_str   = to_dt.strftime("%Y-%m-%d")
    klines = fetch_ohlcv(symbol, interval, start_str, end_str)
    if not klines:
        return pd.DataFrame()
    df_raw = klines_to_dataframe(klines)
    if df_raw.empty:
        return pd.DataFrame()
    df_raw["datetime_utc"] = pd.to_datetime(df_raw["datetime_utc"], errors="coerce")
    df_raw.sort_values("datetime_utc", inplace=True)
    df_raw.reset_index(drop=True, inplace=True)
    return df_raw

def _load_indicators_from_db(symbol: str, interval: str,
                             from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    if from_dt>to_dt:
        return pd.DataFrame()
    conn = _get_connection()
    try:
        _create_indicators_table_if_not_exists(conn)
        sql = """
        SELECT
          datetime_utc, open, high, low, close, volume,
          ma_5, ma_10, ma_20, ma_50, ma_100, ma_200,
          rsi_14, rsi_21, rsi_30,
          obv, obv_sma_5, obv_sma_10, obv_sma_30, obv_sma_50, obv_sma_100
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

def _insert_indicators(symbol: str, interval: str,
                       df: pd.DataFrame, boundary_dt: datetime):
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
        VALUES (?, ?, {placeholders})
        """
        rows_to_insert=[]
        for _, row in df_use.iterrows():
            vals=[row.get(c,None) for c in cols]
            rows_to_insert.append((symbol, interval, *vals))
        conn.executemany(sql, rows_to_insert)
        conn.commit()
    finally:
        conn.close()

def _merge_two_periods(old_df: pd.DataFrame, new_df: pd.DataFrame,
                       start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    df_all = pd.concat([old_df, new_df], ignore_index=True)
    df_all.sort_values("datetime_utc", inplace=True)
    df_all.reset_index(drop=True, inplace=True)
    mask = (df_all["datetime_utc"] >= start_dt) & (df_all["datetime_utc"] <= end_dt)
    final_df = df_all.loc[mask].copy()
    final_df.reset_index(drop=True, inplace=True)
    return final_df

def _get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

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

if __name__ == "__main__":
    main()
