# gptbitcoin/indicators/momentum_indicators.py
# MACD, DMI(+DI/-DI), ADX 등 모멘텀 지표를 계산 (핵심 파라미터만 사용).

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_macd(
    close_s: pd.Series,
    fast_period: int,
    slow_period: int,
    signal_period: int
) -> pd.DataFrame:
    """MACD 지표 계산."""
    macd_df = ta.macd(
        close_s,
        fast=fast_period,
        slow=slow_period,
        signal=signal_period
    )
    if macd_df is None or macd_df.empty:
        na = [np.nan] * len(close_s)
        return pd.DataFrame({
            "macd_line": na,
            "macd_signal": na,
            "macd_hist": na
        }, index=close_s.index)

    cols = list(macd_df.columns)
    line_col = [c for c in cols if "MACD" in c and "h" not in c and "s" not in c]
    hist_col = [c for c in cols if "MACDh" in c]
    sig_col = [c for c in cols if "MACDs" in c]

    ln = line_col[0] if line_col else None
    hi = hist_col[0] if hist_col else None
    sg = sig_col[0] if sig_col else None

    return pd.DataFrame({
        "macd_line": macd_df[ln] if ln else np.nan,
        "macd_signal": macd_df[sg] if sg else np.nan,
        "macd_hist": macd_df[hi] if hi else np.nan
    }, index=close_s.index)


def calc_dmi_adx(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    period: int
) -> pd.DataFrame:
    """DMI(+DI, -DI)와 ADX 지표 계산."""
    adx_df = ta.adx(high=high_s, low=low_s, close=close_s, length=period)
    if adx_df is None or adx_df.empty:
        na = [np.nan] * len(close_s)
        return pd.DataFrame({
            "plus_di": na,
            "minus_di": na,
            "adx": na
        }, index=close_s.index)

    cols = list(adx_df.columns)
    adx_col = [c for c in cols if c.startswith("ADX")]
    plus_col = [c for c in cols if c.startswith("DMP")]
    minus_col = [c for c in cols if c.startswith("DMN")]

    ac = adx_col[0] if adx_col else None
    pc = plus_col[0] if plus_col else None
    mc = minus_col[0] if minus_col else None

    return pd.DataFrame({
        "plus_di": adx_df[pc] if pc else np.nan,
        "minus_di": adx_df[mc] if mc else np.nan,
        "adx": adx_df[ac] if ac else np.nan
    }, index=close_s.index)
