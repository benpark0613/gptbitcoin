# gptbitcoin/indicators/trend_channels.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 모듈은 "추세 채널" 계열 지표(볼린저 밴드, 일목균형표, 파라볼릭 SAR, 슈퍼트렌드 등)를 담는다.

import pandas as pd
import numpy as np
from typing import Dict, Optional


def calc_bollinger_bands(
    close_s: pd.Series,
    period: int = 20,
    stddev_mult: float = 2.0
) -> pd.DataFrame:
    """
    볼린저 밴드(Bollinger Bands) 계산.
    Middle = 해당 period 간 이동평균
    Upper = Middle + stddev_mult * 표준편차
    Lower = Middle - stddev_mult * 표준편차

    Args:
        close_s (pd.Series): 종가 시리즈
        period (int): 볼린저 밴드 기간 (기본 20)
        stddev_mult (float): 표준편차 배수 (기본 2.0)

    Returns:
        pd.DataFrame: 아래 컬럼을 갖는 DataFrame
          - boll_mid
          - boll_upper
          - boll_lower
    """
    mid = close_s.rolling(window=period, min_periods=period).mean()
    stddev = close_s.rolling(window=period, min_periods=period).std()
    upper = mid + stddev_mult * stddev
    lower = mid - stddev_mult * stddev

    df_boll = pd.DataFrame({
        "boll_mid": mid,
        "boll_upper": upper,
        "boll_lower": lower
    }, index=close_s.index)
    return df_boll


def calc_ichimoku(
    high_s: pd.Series,
    low_s: pd.Series,
    tenkan_period: int = 9,
    kijun_period: int = 26,
    span_b_period: int = 52
) -> pd.DataFrame:
    """
    일목균형표(Ichimoku Cloud) 계산.
    - 전환선(tenkan): (과거 n일 최고 + n일 최저)/2
    - 기준선(kijun): (과거 m일 최고 + m일 최저)/2
    - 선행스팬 A: (전환선+기준선)/2 를 기준선 기간만큼 앞으로 시프트
    - 선행스팬 B: (과거 p일 최고+최저)/2 를 기준선 기간만큼 앞으로 시프트
    - 후행스팬(chikou): 종가(또는 고가, 저가 등)로부터 기준선 기간만큼 뒤로 시프트

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        tenkan_period (int): 전환선 기간 (기본 9)
        kijun_period (int): 기준선 기간 (기본 26)
        span_b_period (int): 선행스팬 B 기간 (기본 52)

    Returns:
        pd.DataFrame:
          - ichimoku_tenkan
          - ichimoku_kijun
          - ichimoku_span_a
          - ichimoku_span_b
          - ichimoku_chikou
    """
    # 전환선(tenkan)
    conv_line = (
        high_s.rolling(window=tenkan_period).max() +
        low_s.rolling(window=tenkan_period).min()
    ) / 2.0

    # 기준선(kijun)
    base_line = (
        high_s.rolling(window=kijun_period).max() +
        low_s.rolling(window=kijun_period).min()
    ) / 2.0

    # 스팬 A
    span_a = ((conv_line + base_line) / 2.0).shift(kijun_period)

    # 스팬 B
    span_b = (
        (
            high_s.rolling(window=span_b_period).max() +
            low_s.rolling(window=span_b_period).min()
        ) / 2.0
    ).shift(kijun_period)

    # 치코 스팬(후행스팬)
    # 여기서는 간단히 종가가 아니라 고가를 shift, 실제론 종가를 shift(-kijun_period)도 흔함
    # 프로젝트 정책에 따라 다름
    chikou = high_s.shift(-kijun_period)

    df_ich = pd.DataFrame({
        "ichimoku_tenkan": conv_line,
        "ichimoku_kijun": base_line,
        "ichimoku_span_a": span_a,
        "ichimoku_span_b": span_b,
        "ichimoku_chikou": chikou
    }, index=high_s.index)
    return df_ich


def calc_psar(
    high_s: pd.Series,
    low_s: pd.Series,
    acceleration_step: float = 0.02,
    acceleration_max: float = 0.2
) -> pd.Series:
    """
    파라볼릭 SAR(PSAR) 계산. 단순 예시 버전.
    - 초기 추세(롱/숏) 가정, 매 봉마다 SAR 업데이트
    - EP(Extreme Point), AF(가속도인자) 갱신

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        acceleration_step (float): 가속도 스텝 (기본 0.02)
        acceleration_max (float): 가속도 최대치 (기본 0.2)

    Returns:
        pd.Series: psar 값 시리즈
    """
    psar = [0.0] * len(high_s)
    if len(high_s) < 2:
        return pd.Series(psar, index=high_s.index)

    # 초기화(롱 가정)
    psar[0] = low_s.iloc[0]
    ep = high_s.iloc[0]
    af = acceleration_step
    long_position = True

    for i in range(1, len(high_s)):
        prev_psar = psar[i - 1]
        if long_position:
            # 롱 모드
            cur_psar = prev_psar + af * (ep - prev_psar)
            if cur_psar > low_s.iloc[i]:
                # 반전 -> 숏
                long_position = False
                cur_psar = ep
                af = acceleration_step
                ep = low_s.iloc[i]
            else:
                # 유지
                if high_s.iloc[i] > ep:
                    ep = high_s.iloc[i]
                    af = min(af + acceleration_step, acceleration_max)
        else:
            # 숏 모드
            cur_psar = prev_psar - af * (prev_psar - ep)
            if cur_psar < high_s.iloc[i]:
                # 반전 -> 롱
                long_position = True
                cur_psar = ep
                af = acceleration_step
                ep = high_s.iloc[i]
            else:
                # 유지
                if low_s.iloc[i] < ep:
                    ep = low_s.iloc[i]
                    af = min(af + acceleration_step, acceleration_max)

        psar[i] = cur_psar
    return pd.Series(psar, index=high_s.index)


def calc_atr(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    period: int = 14
) -> pd.Series:
    """
    ATR(Average True Range)을 계산한다.
    TR = max(고가-저가, abs(고가-이전종가), abs(저가-이전종가))
    ATR = TR의 period 이동평균

    Args:
        high_s (pd.Series): 고가
        low_s (pd.Series): 저가
        close_s (pd.Series): 종가
        period (int): ATR 기간 (기본 14)

    Returns:
        pd.Series: ATR 시리즈
    """
    prev_close = close_s.shift(1)
    tr1 = (high_s - low_s).abs()
    tr2 = (high_s - prev_close).abs()
    tr3 = (low_s - prev_close).abs()
    true_range = tr1.combine(tr2, max).combine(tr3, max)
    atr = true_range.rolling(window=period).mean()
    return atr


def calc_supertrend(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    atr_period: int = 10,
    multiplier: float = 3.0
) -> pd.Series:
    """
    슈퍼트렌드(SuperTrend) 계산.
    - 기본 구현 예시: ATR 이용한 상단/하단 밴드
    - 실제로는 추세 반전 시, 상단/하단 밴드를 교체

    Args:
        high_s (pd.Series): 고가
        low_s (pd.Series): 저가
        close_s (pd.Series): 종가
        atr_period (int): ATR 계산 기간
        multiplier (float): ATR 배수

    Returns:
        pd.Series: supertrend 시리즈
    """
    atr_s = calc_atr(high_s, low_s, close_s, period=atr_period)
    hl2 = (high_s + low_s) / 2.0
    upper_band = hl2 + (multiplier * atr_s)
    lower_band = hl2 - (multiplier * atr_s)

    st_vals = [0.0] * len(close_s)
    if len(close_s) == 0:
        return pd.Series(st_vals, index=close_s.index)

    # 초기값
    st_vals[0] = lower_band.iloc[0]
    dir_up = True

    for i in range(1, len(close_s)):
        prev_st = st_vals[i - 1]
        if dir_up:
            cur_st = min(upper_band.iloc[i], prev_st)
            if close_s.iloc[i] < cur_st:
                # 반전 -> 숏
                dir_up = False
                cur_st = upper_band.iloc[i]
        else:
            cur_st = max(lower_band.iloc[i], prev_st)
            if close_s.iloc[i] > cur_st:
                # 반전 -> 롱
                dir_up = True
                cur_st = lower_band.iloc[i]

        st_vals[i] = cur_st

    return pd.Series(st_vals, index=close_s.index)
