# gptbitcoin/indicators/indicators.py
# 구글 스타일, 최소한의 한글 주석

import pandas as pd
from config.config import INDICATOR_CONFIG


def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    """단순 이동평균. min_periods=period로 설정해 초기 구간 NaN."""
    return series.rolling(window=period, min_periods=period).mean()


def calc_rsi_series(close_s: pd.Series, period: int) -> pd.Series:
    """
    RSI 계산 (Wilder 방식).
    초기 구간 이후에 재귀식으로 평활화하여 계산.
    """
    diffs = close_s.diff()
    gains = diffs.where(diffs > 0, 0.0)
    losses = (-diffs).where(diffs < 0, 0.0)
    rsi_vals = [None] * len(close_s)

    if len(close_s) < period:
        return pd.Series(rsi_vals, index=close_s.index)

    avg_gain = gains.iloc[1:period + 1].mean()
    avg_loss = losses.iloc[1:period + 1].mean()

    if avg_loss == 0:
        rsi_vals[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_vals[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, len(close_s)):
        cur_gain = gains.iloc[i] if gains.iloc[i] > 0 else 0.0
        cur_loss = losses.iloc[i] if losses.iloc[i] > 0 else 0.0

        avg_gain = (avg_gain * (period - 1) + cur_gain) / period
        avg_loss = (avg_loss * (period - 1) + cur_loss) / period

        if avg_loss == 0:
            rsi_vals[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_vals[i] = 100.0 - (100.0 / (1.0 + rs))

    return pd.Series(rsi_vals, index=close_s.index)


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """
    OBV 계산. 첫 봉=0, 종가 상승이면 +volume, 하락이면 -volume
    """
    obv_vals = [0] * len(close_s)
    for i in range(1, len(close_s)):
        if close_s.iloc[i] > close_s.iloc[i - 1]:
            obv_vals[i] = obv_vals[i - 1] + vol_s.iloc[i]
        else:
            obv_vals[i] = obv_vals[i - 1] - vol_s.iloc[i]
    return pd.Series(obv_vals, index=close_s.index)


def rolling_min_series(series: pd.Series, window: int) -> pd.Series:
    """최소값을 window 길이로 계산."""
    return series.rolling(window=window, min_periods=window).min()


def rolling_max_series(series: pd.Series, window: int) -> pd.Series:
    """최대값을 window 길이로 계산."""
    return series.rolling(window=window, min_periods=window).max()


def calc_all_indicators(df: pd.DataFrame, cfg: dict = None) -> pd.DataFrame:
    """
    config.config의 INDICATOR_CONFIG 사용하여
    MA, RSI, OBV, Filter, S/R, Channel_Breakout 등을 계산 후 df에 추가.
    """
    if cfg is None:
        cfg = INDICATOR_CONFIG

    if "close" not in df.columns or "volume" not in df.columns:
        raise ValueError("DataFrame에 'close','volume' 열이 필요합니다.")

    # MA
    if "MA" in cfg:
        sp_list = cfg["MA"].get("short_periods", [])
        lp_list = cfg["MA"].get("long_periods", [])
        for sp in sp_list:
            s = calc_sma_series(df["close"], sp)
            df[f"ma_{sp}"] = s
        for lp in lp_list:
            s = calc_sma_series(df["close"], lp)
            df[f"ma_{lp}"] = s

    # RSI
    if "RSI" in cfg:
        length_list = cfg["RSI"].get("lengths", [])
        for length in length_list:
            df[f"rsi_{length}"] = calc_rsi_series(df["close"], length)

    # OBV
    if "OBV" in cfg:
        if "obv_raw" not in df.columns:
            df["obv_raw"] = calc_obv_series(df["close"], df["volume"])
        df["obv"] = df["obv_raw"]

        sp_list = cfg["OBV"].get("short_periods", [])
        lp_list = cfg["OBV"].get("long_periods", [])
        all_obv_periods = sp_list + lp_list
        for p in all_obv_periods:
            col_name = f"obv_sma_{p}"
            s = df["obv_raw"].rolling(window=p, min_periods=p).mean()
            df[col_name] = s

    # Filter
    if "Filter" in cfg:
        windows = cfg["Filter"].get("windows", [])
        for w in windows:
            df[f"filter_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"filter_max_{w}"] = rolling_max_series(df["close"], w)

    # Support/Resistance
    if "Support_Resistance" in cfg:
        sr_windows = cfg["Support_Resistance"].get("windows", [])
        for w in sr_windows:
            df[f"sr_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"sr_max_{w}"] = rolling_max_series(df["close"], w)

    # Channel Breakout
    if "Channel_Breakout" in cfg:
        ch_windows = cfg["Channel_Breakout"].get("windows", [])
        for w in ch_windows:
            df[f"ch_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"ch_max_{w}"] = rolling_max_series(df["close"], w)

    return df
