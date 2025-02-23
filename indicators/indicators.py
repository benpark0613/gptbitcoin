# gptbitcoin/indicators/indicators.py

import pandas as pd

def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()

def calc_rsi_series(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff(1)
    gain = delta.where(delta > 0, 0).rolling(window=period, min_periods=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=period).mean()
    loss = loss.replace({0.0: 1e-10})
    rs = gain / loss
    return 100.0 - (100.0 / (1.0 + rs))

def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    c_shift = close_s.shift(1)
    sign_diff = (close_s - c_shift).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (sign_diff * vol_s).fillna(0).cumsum()

def rolling_min_series(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).min()

def rolling_max_series(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).max()

def calc_all_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if "close" not in df.columns or "volume" not in df.columns:
        raise ValueError("close, volume columns required")

    if "MA" in cfg:
        for sp in cfg["MA"].get("short_periods", []):
            df[f"ma_{sp}"] = calc_sma_series(df["close"], sp)
        for lp in cfg["MA"].get("long_periods", []):
            df[f"ma_{lp}"] = calc_sma_series(df["close"], lp)

    if "RSI" in cfg:
        for length in cfg["RSI"].get("lengths", []):
            df[f"rsi_{length}"] = calc_rsi_series(df["close"], length)

    if "Filter" in cfg:
        for w in cfg["Filter"].get("windows", []):
            df[f"filter_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"filter_max_{w}"] = rolling_max_series(df["close"], w)

    if "Support_Resistance" in cfg:
        for w in cfg["Support_Resistance"].get("windows", []):
            df[f"sr_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"sr_max_{w}"] = rolling_max_series(df["close"], w)

    if "Channel_Breakout" in cfg:
        for w in cfg["Channel_Breakout"].get("windows", []):
            df[f"ch_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"ch_max_{w}"] = rolling_max_series(df["close"], w)

    if "OBV" in cfg:
        if "obv" not in df.columns:
            df["obv"] = calc_obv_series(df["close"], df["volume"])
        for sp in cfg["OBV"].get("short_periods", []):
            df[f"obv_sma_{sp}"] = calc_sma_series(df["obv"], sp)
        for lp in cfg["OBV"].get("long_periods", []):
            df[f"obv_sma_{lp}"] = calc_sma_series(df["obv"], lp)

    return df
