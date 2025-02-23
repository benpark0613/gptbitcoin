# gptbitcoin/data/preprocess.py

import os
import pandas as pd
from typing import Dict, Any
from config.config import INDICATOR_CONFIG
from indicators.indicators import calc_all_indicators

def preprocess_csv(
    csv_in_path: str,
    csv_out_path: str,
    dropna: bool = False
) -> None:
    if not os.path.isfile(csv_in_path):
        raise FileNotFoundError(f"File not found: {csv_in_path}")

    df = pd.read_csv(csv_in_path)
    req_cols = ["datetime_utc", "open", "high", "low", "close", "volume"]
    for c in req_cols:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    # Parse datetime with full format
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

    # Add indicator columns
    df = calc_all_indicators(df, INDICATOR_CONFIG)

    if dropna:
        df.dropna(inplace=True)

    # Reformat datetime column so times remain in YYYY-MM-DD HH:MM:SS
    df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(csv_out_path, index=False, float_format="%.6f")
    print(f"[INFO] Preprocessed CSV saved -> {csv_out_path}")
