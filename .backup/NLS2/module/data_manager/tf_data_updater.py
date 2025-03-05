# module/data_manager/tf_data_updater.py

import pandas as pd
from pathlib import Path
from backup.NLS2.module.binance_api.binance_fetch import fetch_binance_klines


def update_csv(symbol: str, interval: str, start_ts: int, end_ts: int, csv_path: str) -> pd.DataFrame:
    """
    로컬 CSV를 업데이트하고 최종 DataFrame을 반환.
    1) csv_path에 파일이 있으면 읽어와서 min/max time 확인
    2) start_ts~end_ts 범위 중, 로컬에 없는 부분만 binance_fetch
    3) fetch 결과를 concat → CSV 저장
    4) 최종적으로 start_ts~end_ts 범위를 슬라이싱한 df 반환
    """
    p = Path(csv_path)
    p.parent.mkdir(parents=True, exist_ok=True)
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

    # ----------------------------------------------------
    # 1) 앞 구간(Fetch_1): start_ts ~ local_min
    # ----------------------------------------------------
    if start_ts < local_min:
        fetch_needed = True
        fetch_start_1 = start_ts
        fetch_end_1 = local_min
        df_fetched_1 = fetch_binance_klines(symbol, interval, fetch_start_1, fetch_end_1)

        # 비어있지 않고 전부 NA가 아닌 경우에만 concat
        if not df_fetched_1.empty and not df_fetched_1.dropna(how='all').empty:
            df_local = pd.concat([df_local, df_fetched_1], ignore_index=True)

    # ----------------------------------------------------
    # 2) 뒤 구간(Fetch_2): local_max~end_ts
    # ----------------------------------------------------
    if end_ts > local_max:
        fetch_needed = True
        fetch_start_2 = local_max + 1
        fetch_end_2 = end_ts
        df_fetched_2 = fetch_binance_klines(symbol, interval, fetch_start_2, fetch_end_2)

        if not df_fetched_2.empty and not df_fetched_2.dropna(how='all').empty:
            df_local = pd.concat([df_local, df_fetched_2], ignore_index=True)

    if fetch_needed:
        df_local.sort_values("open_time", inplace=True)
        df_local.drop_duplicates(subset="open_time", inplace=True)
        df_local.to_csv(p, index=False)

    # 필요한 구간만 슬라이싱
    df_local = df_local[(df_local["open_time"] >= start_ts) & (df_local["open_time"] < end_ts)]
    return df_local.reset_index(drop=True)
