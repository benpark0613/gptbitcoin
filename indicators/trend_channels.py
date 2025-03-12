# gptbitcoin/indicators/trend_channels.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 모듈은 "추세 채널" 계열 지표(볼린저 밴드, 일목균형표, 파라볼릭 SAR, 슈퍼트렌드 등)를 담는다.

import pandas as pd
import numpy as np


def calc_bollinger_bands(
    close_s: pd.Series,
    period: int = 20,
    stddev_mult: float = 2.0
) -> pd.DataFrame:
    """
    볼린저 밴드(Bollinger Bands) 계산.
    Middle = period 간 이동평균
    Upper = Middle + stddev_mult * 표준편차
    Lower = Middle - stddev_mult * 표준편차

    Args:
        close_s (pd.Series): 종가 시리즈
        period (int): 기간 (기본 20)
        stddev_mult (float): 표준편차 배수 (기본 2.0)

    Returns:
        pd.DataFrame:
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
    close_s: pd.Series,
    tenkan_period: int = 9,
    kijun_period: int = 26,
    span_b_period: int = 52
) -> pd.DataFrame:
    """
    일목균형표(Ichimoku Cloud)를 계산한다.
    - 전환선(tenkan): (과거 tenkan_period일 최고 + 최저) / 2
    - 기준선(kijun): (과거 kijun_period일 최고 + 최저) / 2
    - 선행스팬 A(span_a): (전환선+기준선)/2 을 kijun_period만큼 미래로 시프트
    - 선행스팬 B(span_b): (과거 span_b_period일 최고+최저)/2 를 kijun_period만큼 미래로 시프트
    - 후행스팬(chikou): 현재 종가(close)를 kijun_period만큼 과거로 시프트

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        close_s (pd.Series): 종가 시리즈
        tenkan_period (int): 전환선 기간
        kijun_period (int): 기준선 기간
        span_b_period (int): 선행스팬 B 기간

    Returns:
        pd.DataFrame:
          - ichimoku_tenkan
          - ichimoku_kijun
          - ichimoku_span_a
          - ichimoku_span_b
          - ichimoku_chikou
    """
    if not (len(high_s) == len(low_s) == len(close_s)):
        raise ValueError("high_s, low_s, close_s 길이 불일치.")

    conv_line = (
        high_s.rolling(window=tenkan_period).max() +
        low_s.rolling(window=tenkan_period).min()
    ) / 2.0

    base_line = (
        high_s.rolling(window=kijun_period).max() +
        low_s.rolling(window=kijun_period).min()
    ) / 2.0

    span_a = ((conv_line + base_line) / 2.0).shift(kijun_period)
    span_b = (
        (
            high_s.rolling(window=span_b_period).max() +
            low_s.rolling(window=span_b_period).min()
        ) / 2.0
    ).shift(kijun_period)

    chikou = close_s.shift(-kijun_period)

    df_ich = pd.DataFrame({
        "ichimoku_tenkan": conv_line,
        "ichimoku_kijun": base_line,
        "ichimoku_span_a": span_a,
        "ichimoku_span_b": span_b,
        "ichimoku_chikou": chikou
    }, index=close_s.index)

    return df_ich


def calc_psar(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    acceleration_step: float = 0.02,
    acceleration_max: float = 0.2,
    init_lookback: int = 5
) -> pd.Series:
    """
    파라볼릭 SAR(PSAR)을 Wilder 공식에 가깝게 계산한다.

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        close_s (pd.Series): 종가 시리즈
        acceleration_step (float): AF(가속도인자) 증가량
        acceleration_max (float): AF 최댓값
        init_lookback (int): 초기 추세 판단용 봉 수

    Returns:
        pd.Series: PSAR 시리즈
    """
    n = len(high_s)
    if n < init_lookback:
        return pd.Series([np.nan]*n, index=high_s.index)

    # 결과 보관
    psar = np.full(n, np.nan, dtype=float)

    # 초기 구간 최대/최소
    initial_max = high_s.iloc[:init_lookback].max()
    initial_min = low_s.iloc[:init_lookback].min()

    # 초기 추세 판단
    first_close = close_s.iloc[0]
    last_close = close_s.iloc[init_lookback - 1]
    up_trend = (last_close >= first_close)

    # 초기 SAR 설정
    if up_trend:
        psar[init_lookback-1] = initial_min
        ep = initial_max  # EP(Extreme Point)
    else:
        psar[init_lookback-1] = initial_max
        ep = initial_min

    af = acceleration_step

    # 메인 루프
    for i in range(init_lookback, n):
        prev_psar = psar[i - 1]
        new_sar = prev_psar + af * (ep - prev_psar)

        if up_trend:
            # 최근 2봉의 최저가보다 SAR이 높아지지 않게
            new_sar = min(new_sar, low_s.iloc[i - 1])
            if (i - 2) >= 0:
                new_sar = min(new_sar, low_s.iloc[i - 2])
            # 반전 체크
            if low_s.iloc[i] < new_sar:
                # 반전
                up_trend = False
                new_sar = ep
                af = acceleration_step
                ep = low_s.iloc[i]
            else:
                # 추세 유지
                if high_s.iloc[i] > ep:
                    ep = high_s.iloc[i]
                    af = min(af + acceleration_step, acceleration_max)

        else:
            # 최근 2봉의 최고가보다 SAR이 낮아지지 않게
            new_sar = max(new_sar, high_s.iloc[i - 1])
            if (i - 2) >= 0:
                new_sar = max(new_sar, high_s.iloc[i - 2])
            # 반전 체크
            if high_s.iloc[i] > new_sar:
                # 반전
                up_trend = True
                new_sar = ep
                af = acceleration_step
                ep = high_s.iloc[i]
            else:
                # 추세 유지
                if low_s.iloc[i] < ep:
                    ep = low_s.iloc[i]
                    af = min(af + acceleration_step, acceleration_max)

        psar[i] = new_sar

    return pd.Series(psar, index=high_s.index)


def calc_atr(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    period: int = 14
) -> pd.Series:
    """
    ATR(Average True Range)을 계산한다.
    TR = max(고가-저가, |고가-이전종가|, |저가-이전종가|)
    ATR = TR의 period 이동평균

    Args:
        high_s (pd.Series): 고가
        low_s (pd.Series): 저가
        close_s (pd.Series): 종가
        period (int): 기간 (기본 14)

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
    슈퍼트렌드(SuperTrend)를 표준 방식에 가깝게 계산한다.
    1) basis = (고가 + 저가)/2
    2) 기본 upper/lower 밴드 = basis ± multiplier * ATR
    3) 추세에 따라 상단/하단 밴드 갱신 후 최종 슈퍼트렌드 계산

    Args:
        high_s (pd.Series): 고가
        low_s (pd.Series): 저가
        close_s (pd.Series): 종가
        atr_period (int): ATR 기간
        multiplier (float): ATR 배수

    Returns:
        pd.Series: supertrend 시리즈
    """
    n = len(close_s)
    if n == 0:
        return pd.Series([], dtype=float, index=close_s.index)

    atr_s = calc_atr(high_s, low_s, close_s, period=atr_period)
    basis = (high_s + low_s) / 2.0

    # 초기 upper/lower
    final_upper = basis - (multiplier * atr_s)
    final_lower = basis + (multiplier * atr_s)

    st = np.full(n, np.nan, dtype=float)
    trend_up = True  # 초기 추세 가정 (첫 봉로직 단순화)
    st[0] = final_lower.iloc[0]  # 초기값

    for i in range(1, n):
        # 최종 upper/lower 갱신 (표준 공식)
        cur_upper = basis.iloc[i] - (multiplier * atr_s.iloc[i])
        cur_lower = basis.iloc[i] + (multiplier * atr_s.iloc[i])

        # 기존 final_upper/ final_lower 이어받아 보정
        # Uptrend일 때 upper는 현재값과 이전 upper의 min
        # Downtrend일 때 lower는 현재값과 이전 lower의 max
        if trend_up:
            final_upper.iloc[i] = min(cur_upper, final_upper.iloc[i - 1])
        else:
            final_upper.iloc[i] = cur_upper

        if not trend_up:
            final_lower.iloc[i] = max(cur_lower, final_lower.iloc[i - 1])
        else:
            final_lower.iloc[i] = cur_lower

        # 추세 판단
        if trend_up:
            if close_s.iloc[i] <= final_upper.iloc[i]:
                trend_up = False
                st[i] = final_upper.iloc[i]
            else:
                trend_up = True
                st[i] = final_lower.iloc[i]
        else:
            if close_s.iloc[i] >= final_lower.iloc[i]:
                trend_up = True
                st[i] = final_lower.iloc[i]
            else:
                trend_up = False
                st[i] = final_upper.iloc[i]

    return pd.Series(st, index=close_s.index)
