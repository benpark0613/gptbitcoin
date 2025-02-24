# gptbitcoin/data/preprocess.py
# 구글 스타일, 최소한의 한글 주석

import os
import pandas as pd
from typing import Dict, Any
from config.config import INDICATOR_CONFIG, WARMUP_BARS, START_DATE, END_DATE, SYMBOL
from indicators.indicators import calc_all_indicators
from data.fetch_data import fetch_ohlcv_csv

def preprocess_csv(
    csv_in_path: str,
    csv_out_path: str,
    dropna: bool = False
) -> None:
    """원본 CSV에 지표를 추가하고 저장한다."""
    if not os.path.isfile(csv_in_path):
        raise FileNotFoundError(f"File not found: {csv_in_path}")

    df = pd.read_csv(csv_in_path)
    req_cols = ["datetime_utc", "open", "high", "low", "close", "volume"]
    for c in req_cols:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    df["datetime_utc"] = pd.to_datetime(
        df["datetime_utc"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )
    if df["datetime_utc"].isnull().any():
        raise ValueError("Invalid or unrecognized datetime format in 'datetime_utc'")

    df.sort_values("datetime_utc", inplace=True)
    df.reset_index(drop=True, inplace=True)

    for i, row in df.iterrows():
        if any([
            row["open"] <= 0,
            row["high"] <= 0,
            row["low"] <= 0,
            row["close"] <= 0,
            row["volume"] < 0
        ]):
            raise ValueError(f"Non-positive values at row {i}")

    if df.empty:
        raise ValueError("No valid data")

    df = calc_all_indicators(df, INDICATOR_CONFIG)
    if dropna:
        df.dropna(inplace=True)

    df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(csv_out_path, index=False)
    print(f"[INFO] Preprocessed CSV saved -> {csv_out_path}")

def test_fetch_and_preprocess(
    symbol: str = SYMBOL,
    interval: str = "1d",
    start_str: str = START_DATE,
    end_str: str = END_DATE,
    warmup_bars: int = WARMUP_BARS
) -> None:
    """fetch_data.py를 호출하여 origin CSV를 만들고, 보조지표를 추가 후 저장한다."""
    csv_in = fetch_ohlcv_csv(
        symbol=symbol,
        interval=interval,
        start_str=start_str,
        end_str=end_str,
        warmup_bars=warmup_bars
    )
    csv_out = csv_in.replace(".csv", "_with_indicators.csv")
    preprocess_csv(csv_in, csv_out, dropna=False)
    print(f"[TEST] Completed fetch + indicator preprocess: {csv_out}")

if __name__ == "__main__":
    # 예시 실행
    test_fetch_and_preprocess()
