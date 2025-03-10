# gptbitcoin/indicators/basic_indicators.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 모듈은 "기본 지표" 그룹에 속하는 함수들을 담는다.
# (예: SMA, EMA, RSI, OBV, Filter, SR, CB)

import pandas as pd
from typing import Optional


def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    """
    단순 이동평균(SMA)을 계산한다.

    Args:
        series (pd.Series): 기준 시계열 (예: 종가)
        period (int): 이동평균 구간

    Returns:
        pd.Series: SMA 시계열
    """
    return series.rolling(window=period, min_periods=period).mean()


def calc_ema_series(series: pd.Series, period: int) -> pd.Series:
    """
    지수 이동평균(EMA)을 계산한다.

    Args:
        series (pd.Series): 기준 시계열 (예: 종가)
        period (int): EMA 구간

    Returns:
        pd.Series: EMA 시계열
    """
    return series.ewm(span=period, adjust=False).mean()


def calc_rsi_series(close_s: pd.Series, period: int) -> pd.Series:
    """
    RSI 지표를 계산한다. (Wilder 방식을 모방)
    초기 구간은 period만큼 단순 평균값을 사용하고,
    이후에는 지수평활 개념을 적용한다.

    Args:
        close_s (pd.Series): 종가 시리즈
        period (int): RSI 구간 (예: 14)

    Returns:
        pd.Series: RSI 시리즈 (0~100)
    """
    diffs = close_s.diff()
    gains = diffs.where(diffs > 0, 0.0)
    losses = (-diffs).where(diffs < 0, 0.0)

    rsi_vals = [None] * len(close_s)
    if len(close_s) < period:
        return pd.Series(rsi_vals, index=close_s.index)

    # 초기 평균
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

        avg_gain = ((avg_gain * (period - 1)) + cur_gain) / period
        avg_loss = ((avg_loss * (period - 1)) + cur_loss) / period

        if avg_loss == 0:
            rsi_vals[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_vals[i] = 100.0 - (100.0 / (1.0 + rs))

    return pd.Series(rsi_vals, index=close_s.index)


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """
    OBV (On-Balance Volume)를 계산한다.
    첫 번째 봉은 0으로 시작하며,
    종가가 상승하면 +volume, 하락하면 -volume을 누적한다.

    Args:
        close_s (pd.Series): 종가 시리즈
        vol_s (pd.Series): 거래량 시리즈

    Returns:
        pd.Series: OBV 시리즈
    """
    obv_vals = [0.0] * len(close_s)
    for i in range(1, len(close_s)):
        if close_s.iloc[i] > close_s.iloc[i - 1]:
            obv_vals[i] = obv_vals[i - 1] + vol_s.iloc[i]
        elif close_s.iloc[i] < close_s.iloc[i - 1]:
            obv_vals[i] = obv_vals[i - 1] - vol_s.iloc[i]
        else:
            obv_vals[i] = obv_vals[i - 1]
    return pd.Series(obv_vals, index=close_s.index)


def calc_filter_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """
    Filter 룰 적용을 위해, 최근 window 구간의 최솟값/최댓값을 구한다.
    (매수 신호: 가격이 최저가 대비 X% 상승, 매도 신호: 가격이 최고가 대비 Y% 하락 등)

    Args:
        series (pd.Series): 종가(혹은 원하는 기준) 시계열
        window (int): Lookback 기간

    Returns:
        pd.DataFrame:
            filter_min: 롤링 최솟값
            filter_max: 롤링 최댓값
    """
    fmin = series.rolling(window=window, min_periods=window).min()
    fmax = series.rolling(window=window, min_periods=window).max()
    df = pd.DataFrame({
        f"filter_min_{window}": fmin,
        f"filter_max_{window}": fmax
    }, index=series.index)
    return df


def calc_sr_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """
    Support/Resistance (SR) 룰용 min/max (window 기간) 계산.

    Args:
        series (pd.Series): 종가 시리즈
        window (int): SR 계산에 사용할 봉 수

    Returns:
        pd.DataFrame:
            sr_min: 롤링 최솟값
            sr_max: 롤링 최댓값
    """
    sr_min = series.rolling(window=window, min_periods=window).min()
    sr_max = series.rolling(window=window, min_periods=window).max()
    df = pd.DataFrame({
        f"sr_min_{window}": sr_min,
        f"sr_max_{window}": sr_max
    }, index=series.index)
    return df


def calc_cb_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """
    Channel Breakout(CB) 룰용 min/max (window 기간) 계산.

    Args:
        series (pd.Series): 종가 시리즈 (혹은 고가, 저가 사용 가능)
        window (int): 채널 계산에 사용할 봉 수

    Returns:
        pd.DataFrame:
            ch_min: 롤링 최솟값
            ch_max: 롤링 최댓값
    """
    ch_min = series.rolling(window=window, min_periods=window).min()
    ch_max = series.rolling(window=window, min_periods=window).max()
    df = pd.DataFrame({
        f"ch_min_{window}": ch_min,
        f"ch_max_{window}": ch_max
    }, index=series.index)
    return df
