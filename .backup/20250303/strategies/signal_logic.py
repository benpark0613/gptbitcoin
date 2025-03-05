# gptbitcoin/strategies/signal_logic.py

import pandas as pd
import numpy as np

def generate_signal_ma(df: pd.DataFrame, short_period: int, long_period: int, band_filter: float = 0.0) -> pd.Series:
    short_col = f"ma_{short_period}"
    long_col = f"ma_{long_period}"
    sig = pd.Series(0, index=df.index, dtype=int)
    up_th = df[long_col] * (1 + band_filter)
    down_th = df[long_col] * (1 - band_filter)
    sig = np.where(df[short_col] >= up_th, 1, sig)
    sig = np.where(df[short_col] <= down_th, -1, sig)
    return pd.Series(sig, index=df.index, dtype=int)

def generate_signal_rsi(
    df: pd.DataFrame,
    period: int,
    overbought: float,
    oversold: float
) -> pd.Series:
    rsi_col = f"rsi_{period}"
    rsi_vals = df[rsi_col]
    sig = [0]*len(df)
    for i in range(1, len(df)):
        prev_rsi = rsi_vals.iloc[i-1]
        curr_rsi = rsi_vals.iloc[i]
        if prev_rsi < oversold <= curr_rsi:
            sig[i] = 1
        elif prev_rsi > overbought >= curr_rsi:
            sig[i] = -1
        else:
            sig[i] = 0
    return pd.Series(sig, index=df.index, dtype=int)

def generate_signal_filter(
    df: pd.DataFrame,
    window: int,
    x: float,
    y: float
) -> pd.Series:
    min_col = f"filter_min_{window}"
    max_col = f"filter_max_{window}"
    sig = pd.Series(0, index=df.index, dtype=int)
    c = df["close"]
    up_th = (1 + x) * df[min_col]
    down_th = (1 - y) * df[max_col]
    sig = np.where(c > up_th, 1, sig)
    sig = np.where(c < down_th, -1, sig)
    return pd.Series(sig, index=df.index, dtype=int)

def generate_signal_snr(
    df: pd.DataFrame,
    window: int,
    band_pct: float
) -> pd.Series:
    min_col = f"sr_min_{window}"
    max_col = f"sr_max_{window}"
    c = df["close"]
    sig = pd.Series(0, index=df.index, dtype=int)
    up_th = df[max_col] * (1 + band_pct)
    down_th = df[min_col] * (1 - band_pct)
    sig = np.where(c > up_th, 1, sig)
    sig = np.where(c < down_th, -1, sig)
    return pd.Series(sig, index=df.index, dtype=int)

def generate_signal_channel_breakout(
    df: pd.DataFrame,
    window: int,
    c_value: float
) -> pd.Series:
    min_col = f"ch_min_{window}"
    max_col = f"ch_max_{window}"
    c = df["close"]
    sig = np.zeros(len(df), dtype=int)
    hi = df[max_col]
    lo = df[min_col]
    channel_ratio = (hi - lo).where(lo != 0, 0) / lo.replace(0, 1e-10)
    in_channel = (channel_ratio <= c_value)
    sig = np.where(in_channel & (c > hi), 1, sig)
    sig = np.where(in_channel & (c < lo), -1, sig)
    return pd.Series(sig, index=df.index, dtype=int)

def generate_signal_obv(
    df: pd.DataFrame,
    short_period: int,
    long_period: int
) -> pd.Series:
    obv_short = f"obv_sma_{short_period}"
    obv_long = f"obv_sma_{long_period}"
    sig = pd.Series(0, index=df.index, dtype=int)
    s = df[obv_short]
    l = df[obv_long]
    sig = np.where(s >= l, 1, -1)
    return pd.Series(sig, index=df.index, dtype=int)
