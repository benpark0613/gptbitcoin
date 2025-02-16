import os
import pandas as pd
from pathlib import Path
from test.nls2.binance_api.binance_fetch import fetch_binance_klines

def update_csv(symbol: str, interval: str, start_ts: int, end_ts: int, csv_path: str) -> pd.DataFrame:
    """로컬 CSV를 업데이트하고 최종 DataFrame을 반환."""
    p = Path(csv_path)
    if p.is_file():
        df_local = pd.read_csv(p)
    else:
        df_local = pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

    if not df_local.empty:
        df_local["open_time"] = pd.to_numeric(df_local["open_time"], errors='coerce')
        df_local.sort_values("open_time", inplace=True)
        df_local.drop_duplicates(subset="open_time", inplace=True)
        local_min = df_local["open_time"].min()
        local_max = df_local["open_time"].max()
    else:
        local_min = end_ts
        local_max = start_ts

    fetch_needed = False
    fetch_start = start_ts
    fetch_end = end_ts

    if start_ts < local_min:
        fetch_needed = True
        fetch_start = start_ts
        fetch_end_1 = local_min
        df_fetched_1 = fetch_binance_klines(symbol, interval, fetch_start, fetch_end_1)
        df_local = pd.concat([df_local, df_fetched_1], ignore_index=True)

    if end_ts > local_max:
        fetch_needed = True
        fetch_start_2 = local_max + 1
        fetch_end_2 = end_ts
        df_fetched_2 = fetch_binance_klines(symbol, interval, fetch_start_2, fetch_end_2)
        df_local = pd.concat([df_local, df_fetched_2], ignore_index=True)

    if fetch_needed:
        df_local.sort_values("open_time", inplace=True)
        df_local.drop_duplicates(subset="open_time", inplace=True)
        df_local.to_csv(p, index=False)

    df_local = df_local[(df_local["open_time"] >= start_ts) & (df_local["open_time"] < end_ts)]
    return df_local.reset_index(drop=True)
