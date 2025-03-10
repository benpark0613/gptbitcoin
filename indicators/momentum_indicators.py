# gptbitcoin/indicators/momentum_indicators.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 모듈은 "모멘텀/추세" 지표 그룹(예: MACD, DMI/ADX, 스토캐스틱 등)을 담는다.
# (프로젝트 요구사항에 따라 실제로 사용할 지표만 남기면 됨)

import pandas as pd
import numpy as np
from typing import Optional, Dict


def calc_macd(
    close_s: pd.Series,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9
) -> pd.DataFrame:
    """
    MACD (Moving Average Convergence / Divergence) 지표를 계산한다.
    - MACD 라인 = EMA(fast_period) - EMA(slow_period)
    - 시그널 라인 = MACD 라인의 EMA(signal_period)
    - 히스토그램 = MACD 라인 - 시그널 라인

    Args:
        close_s (pd.Series): 종가 시리즈
        fast_period (int): 단기 EMA 기간 (기본 12)
        slow_period (int): 장기 EMA 기간 (기본 26)
        signal_period (int): 시그널 EMA 기간 (기본 9)

    Returns:
        pd.DataFrame: 다음 컬럼을 갖는 DataFrame
          - macd_line
          - macd_signal
          - macd_hist
    """
    # 지수 이동평균 함수
    def ema(series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()

    ema_fast = ema(close_s, fast_period)
    ema_slow = ema(close_s, slow_period)
    macd_line = ema_fast - ema_slow

    macd_signal = ema(macd_line, signal_period)
    macd_hist = macd_line - macd_signal

    df_macd = pd.DataFrame({
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist
    }, index=close_s.index)
    return df_macd


def calc_dmi_adx(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    period: int = 14
) -> pd.DataFrame:
    """
    DMI(+DI, -DI) & ADX 계산을 수행한다.
    - +DI, -DI, 그리고 DX -> ADX
    - 기본적으로 14일 구간이 많이 사용됨.

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        close_s (pd.Series): 종가 시리즈
        period (int): DMI/ADX 계산 기간 (기본 14)

    Returns:
        pd.DataFrame:
          - plus_di: +DI 시계열
          - minus_di: -DI 시계열
          - adx: ADX 시계열
    """
    # 1) +DM, -DM 계산
    prev_high = high_s.shift(1)
    prev_low = low_s.shift(1)
    up_move = high_s - prev_high
    down_move = prev_low - low_s

    plus_dm = up_move.where((up_move > 0) & (up_move > down_move), 0.0)
    minus_dm = down_move.where((down_move > 0) & (down_move > up_move), 0.0)

    # 2) TR (True Range)
    prev_close = close_s.shift(1)
    tr1 = (high_s - low_s).abs()
    tr2 = (high_s - prev_close).abs()
    tr3 = (low_s - prev_close).abs()
    true_range = tr1.combine(tr2, max).combine(tr3, max)

    # 3) +DM, -DM, TR 지표를 rolling sum (또는 평활)
    plus_dm_sum = plus_dm.rolling(window=period).sum()
    minus_dm_sum = minus_dm.rolling(window=period).sum()
    tr_sum = true_range.rolling(window=period).sum()

    plus_di = 100.0 * plus_dm_sum / tr_sum.replace(0, np.nan)
    minus_di = 100.0 * minus_dm_sum / tr_sum.replace(0, np.nan)

    # DX
    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) ) * 100.0

    # ADX = DX의 rolling mean( period )
    adx = dx.rolling(window=period).mean()

    df_dmi = pd.DataFrame({
        "plus_di": plus_di,
        "minus_di": minus_di,
        "adx": adx
    }, index=high_s.index)
    return df_dmi


def calc_stochastic(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    k_period: int = 14,
    d_period: int = 3
) -> pd.DataFrame:
    """
    스토캐스틱 오실레이터(Stochastic Oscillator) 계산.
    - %K = (현재 종가 - n일 최저) / (n일 최고 - n일 최저) * 100
    - %D = %K의 d_period 이동평균

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        close_s (pd.Series): 종가 시리즈
        k_period (int): K선 계산용 lookback (기본 14)
        d_period (int): D선 계산용 SMA 기간 (기본 3)

    Returns:
        pd.DataFrame:
          - stoch_k: %K
          - stoch_d: %D
    """
    min_low = low_s.rolling(window=k_period).min()
    max_high = high_s.rolling(window=k_period).max()

    stoch_k = ((close_s - min_low) / (max_high - min_low).replace(0, np.nan)) * 100
    stoch_d = stoch_k.rolling(window=d_period).mean()

    df_stoch = pd.DataFrame({
        "stoch_k": stoch_k,
        "stoch_d": stoch_d
    }, index=close_s.index)
    return df_stoch