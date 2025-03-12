# gptbitcoin/indicators/basic_indicators.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 모듈은 "기본 지표" 그룹에 속하는 함수들을 담는다.
# (예: SMA, EMA, RSI, OBV, Filter, SR, CB)

import numpy as np
import pandas as pd

def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    """
    단순 이동평균(SMA)을 계산한다. (numpy 벡터화 버전 예시)

    Args:
        series (pd.Series): 기준 시계열 (예: 종가)
        period (int): 이동평균 구간

    Returns:
        pd.Series: SMA 시계열
    """
    arr = series.to_numpy(dtype=float)
    if len(arr) < period:
        return pd.Series([np.nan]*len(arr), index=series.index)

    # 누적합(cumulative sum)을 활용한 벡터화
    csum = np.cumsum(arr)
    # period 전의 누적합을 빼서 rolling 합계 구함
    csum[period:] = csum[period:] - csum[:-period]

    # 결과 배열을 생성하고, period 미만 구간은 NaN
    sma = np.full_like(arr, np.nan)
    sma[period-1:] = csum[period-1:] / period
    return pd.Series(sma, index=series.index)



def calc_rsi_series(close_s: pd.Series, period: int) -> pd.Series:
    """
    RSI 지표를 Wilder 표준 공식을 따라 계산한다.

    Args:
        close_s (pd.Series): 종가 시리즈
        period (int): RSI 기간 (예: 14)

    Returns:
        pd.Series: RSI 시리즈 (0~100 범위)
    """
    if period < 1:
        return pd.Series(np.nan, index=close_s.index)

    arr = close_s.to_numpy(dtype=float)
    n = len(arr)
    if n < period:
        # 데이터 길이가 period보다 짧으면 전부 NaN
        return pd.Series([np.nan]*n, index=close_s.index)

    # 1. 가격 변동분 계산
    diffs = np.diff(arr)
    # 첫 번째 값은 변동이 없으므로 0.0으로 삽입
    diffs = np.insert(diffs, 0, 0.0)

    gains = np.where(diffs > 0, diffs, 0.0)
    losses = np.where(diffs < 0, -diffs, 0.0)

    rsi_vals = np.full(n, np.nan)

    # 2. 초기 Average Gain, Average Loss (첫 period 구간은 단순 평균)
    avg_gain = np.mean(gains[1:period+1])
    avg_loss = np.mean(losses[1:period+1])

    # 3. 초기 RSI
    if avg_loss == 0.0:
        rsi_vals[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_vals[period] = 100.0 - (100.0 / (1.0 + rs))

    # 4. 이후부터 Wilder의 지수평활 공식 적용
    for i in range(period+1, n):
        cur_gain = gains[i]
        cur_loss = losses[i]
        avg_gain = ((avg_gain * (period - 1)) + cur_gain) / period
        avg_loss = ((avg_loss * (period - 1)) + cur_loss) / period

        if avg_loss == 0.0:
            rsi_vals[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_vals[i] = 100.0 - (100.0 / (1.0 + rs))

    return pd.Series(rsi_vals, index=close_s.index)


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """
    OBV(On-Balance Volume)를 계산한다. (numpy where 활용)

    Args:
        close_s (pd.Series): 종가 시리즈
        vol_s (pd.Series): 거래량 시리즈

    Returns:
        pd.Series: OBV 시리즈
    """
    cvals = close_s.to_numpy(dtype=float)
    vvals = vol_s.to_numpy(dtype=float)

    # price 상승/하락 구간 판별
    diffs = np.diff(cvals, prepend=cvals[0])  # 첫 값은 변화없음으로 처리
    up_mask = diffs > 0
    down_mask = diffs < 0

    # OBV 누적합 계산
    obv_array = np.zeros_like(cvals)
    for i in range(1, len(obv_array)):
        if up_mask[i]:
            obv_array[i] = obv_array[i-1] + vvals[i]
        elif down_mask[i]:
            obv_array[i] = obv_array[i-1] - vvals[i]
        else:
            obv_array[i] = obv_array[i-1]

    return pd.Series(obv_array, index=close_s.index)


def calc_filter_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """
    Filter 룰 적용용 min/max (window) 계산.

    Args:
        series (pd.Series): 기준 시리즈
        window (int): Lookback 기간

    Returns:
        pd.DataFrame: filter_min, filter_max
    """
    arr = series.rolling(window=window, min_periods=window)
    fmin = arr.min()
    fmax = arr.max()
    df = pd.DataFrame({
        f"filter_min_{window}": fmin,
        f"filter_max_{window}": fmax
    }, index=series.index)
    return df


def calc_sr_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """
    Support/Resistance (SR) 룰용 min/max 계산.

    Args:
        series (pd.Series): 기준 시리즈
        window (int): Lookback 기간

    Returns:
        pd.DataFrame: sr_min, sr_max
    """
    arr = series.rolling(window=window, min_periods=window)
    sr_min = arr.min()
    sr_max = arr.max()
    df = pd.DataFrame({
        f"sr_min_{window}": sr_min,
        f"sr_max_{window}": sr_max
    }, index=series.index)
    return df


def calc_cb_minmax(series: pd.Series, window: int) -> pd.DataFrame:
    """
    Channel Breakout(CB) 룰용 min/max 계산.

    Args:
        series (pd.Series): 기준 시리즈
        window (int): Lookback 기간

    Returns:
        pd.DataFrame: ch_min, ch_max
    """
    arr = series.rolling(window=window, min_periods=window)
    ch_min = arr.min()
    ch_max = arr.max()
    df = pd.DataFrame({
        f"ch_min_{window}": ch_min,
        f"ch_max_{window}": ch_max
    }, index=series.index)
    return df
