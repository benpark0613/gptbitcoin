# gptbitcoin/indicators/basic_indicators.py
# 핵심 보조지표 파라미터만을 사용하여 기본 지표를 계산하는 모듈.
# band_filters, time_delays, holding_periods 등 추가 파라미터는 여기서 처리하지 않는다.

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    """단순이동평균(SMA)을 계산한다."""
    sma = ta.sma(series, length=period)
    if sma is None:
        return pd.Series([np.nan] * len(series), index=series.index)
    return sma


def calc_rsi_series(close_s: pd.Series, period: int) -> pd.Series:
    """RSI 지표를 계산한다."""
    rsi = ta.rsi(close_s, length=period)
    if rsi is None:
        return pd.Series([np.nan] * len(close_s), index=close_s.index)
    return rsi


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """OBV 지표를 계산한다."""
    obv = ta.obv(close_s, volume=vol_s)
    if obv is None:
        return pd.Series([0.0] * len(close_s), index=close_s.index)
    return obv


def calc_filter_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """주어진 기간(window) 동안의 최솟값과 최댓값을 구한다."""
    fmin = series.rolling(window=window, min_periods=window).min()
    fmax = series.rolling(window=window, min_periods=window).max()
    return pd.DataFrame({
        f"filter_min_{window}": fmin,
        f"filter_max_{window}": fmax
    }, index=series.index)


def calc_sr_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """주어진 기간(window)의 최솟값/최댓값을 구한다."""
    sr_min = series.rolling(window=window, min_periods=window).min()
    sr_max = series.rolling(window=window, min_periods=window).max()
    return pd.DataFrame({
        f"sr_min_{window}": sr_min,
        f"sr_max_{window}": sr_max
    }, index=series.index)


def calc_cb_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """주어진 기간(window)의 최솟값/최댓값을 구한다."""
    ch_min = series.rolling(window=window, min_periods=window).min()
    ch_max = series.rolling(window=window, min_periods=window).max()
    return pd.DataFrame({
        f"ch_min_{window}": ch_min,
        f"ch_max_{window}": ch_max
    }, index=series.index)
