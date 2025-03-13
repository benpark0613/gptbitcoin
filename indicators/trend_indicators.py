# gptbitcoin/indicators/trend_indicators.py
# 추세 기반 보조지표 계산 모듈 (소문자 컬럼명 사용)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_sma(close_sr: pd.Series, length: int) -> pd.Series:
    """
    이동평균(MA) 지표 계산.

    Args:
        close_sr (pd.Series): 종가 시리즈
        length (int): 이동평균 기간

    Returns:
        pd.Series: 시리즈 이름 예) "ma_{length}"
    """
    sma_sr = ta.sma(close_sr, length=length)
    if sma_sr is None or sma_sr.empty:
        return pd.Series([np.nan] * len(close_sr), index=close_sr.index, name=f"ma_{length}")
    sma_sr.name = f"ma_{length}"
    return sma_sr


def calc_macd(df: pd.DataFrame, fast_period: int, slow_period: int, signal_period: int) -> pd.DataFrame:
    """
    MACD 지표 계산.

    Args:
        df (pd.DataFrame): "close"가 있어야 함
        fast_period (int)
        slow_period (int)
        signal_period (int)

    Returns:
        pd.DataFrame:
            - macd_line_{fast}_{slow}_{signal}
            - macd_signal_{fast}_{slow}_{signal}
            - macd_hist_{fast}_{slow}_{signal}
    """
    macd_df = ta.macd(
        df["close"],
        fast=fast_period,
        slow=slow_period,
        signal=signal_period
    )
    if macd_df is None or macd_df.empty:
        return pd.DataFrame({
            f"macd_line_{fast_period}_{slow_period}_{signal_period}": [np.nan] * len(df),
            f"macd_signal_{fast_period}_{slow_period}_{signal_period}": [np.nan] * len(df),
            f"macd_hist_{fast_period}_{slow_period}_{signal_period}": [np.nan] * len(df)
        }, index=df.index)

    macd_df.columns = [
        f"macd_line_{fast_period}_{slow_period}_{signal_period}",
        f"macd_signal_{fast_period}_{slow_period}_{signal_period}",
        f"macd_hist_{fast_period}_{slow_period}_{signal_period}"
    ]
    return macd_df


def calc_dmi_adx(df: pd.DataFrame, lookback_period: int) -> pd.DataFrame:
    """
    DMI(+DI, -DI)와 ADX 계산.

    Args:
        df (pd.DataFrame): "high","low","close" 필요
        lookback_period (int): 기간

    Returns:
        pd.DataFrame:
            - plus_di_{lookback_period}
            - minus_di_{lookback_period}
            - adx_{lookback_period}
    """
    adx_df = ta.adx(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        length=lookback_period
    )
    if adx_df is None or adx_df.empty:
        return pd.DataFrame({
            f"plus_di_{lookback_period}": [np.nan] * len(df),
            f"minus_di_{lookback_period}": [np.nan] * len(df),
            f"adx_{lookback_period}": [np.nan] * len(df)
        }, index=df.index)

    adx_df.columns = [
        f"plus_di_{lookback_period}",
        f"minus_di_{lookback_period}",
        f"adx_{lookback_period}"
    ]
    return adx_df


def calc_ichimoku(df: pd.DataFrame, tenkan_period: int, kijun_period: int, span_b_period: int) -> pd.DataFrame:
    """
    일목균형표 지표 계산.

    Args:
        df (pd.DataFrame): "high","low","close" 필요
        tenkan_period (int)
        kijun_period (int)
        span_b_period (int)

    Returns:
        pd.DataFrame:
          - ich_{t}_{k}_{s}_tenkan
          - ich_{t}_{k}_{s}_kijun
          - ich_{t}_{k}_{s}_span_a
          - ich_{t}_{k}_{s}_span_b
          - ich_{t}_{k}_{s}_chikou
    """
    result = ta.ichimoku(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        tenkan=tenkan_period,
        kijun=kijun_period,
        senkou=span_b_period
    )
    if result is None:
        return pd.DataFrame({
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_tenkan": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_kijun": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_span_a": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_span_b": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_chikou": [np.nan] * len(df)
        }, index=df.index)

    # (ichimokudf, spandf) 튜플인지 확인
    if isinstance(result, tuple):
        ichimoku_df, _ = result
    else:
        ichimoku_df = result

    if ichimoku_df is None or ichimoku_df.empty:
        return pd.DataFrame({
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_tenkan": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_kijun": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_span_a": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_span_b": [np.nan] * len(df),
            f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_chikou": [np.nan] * len(df)
        }, index=df.index)

    # MultiIndex 컬럼 처리
    if isinstance(ichimoku_df.columns, pd.MultiIndex):
        ichimoku_df.columns = ["_".join(col).strip() for col in ichimoku_df.columns.values]

    rename_map = {}
    for col in ichimoku_df.columns:
        c_up = col.upper()
        if "ITS" in c_up:  # 전환선
            rename_map[col] = f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_tenkan"
        elif "IKS" in c_up:  # 기준선
            rename_map[col] = f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_kijun"
        elif "ISA" in c_up:  # 선행스팬A
            rename_map[col] = f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_span_a"
        elif "ISB" in c_up:  # 선행스팬B
            rename_map[col] = f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_span_b"
        elif "ICS" in c_up or "CHIKOU" in c_up:  # 치코스팬
            rename_map[col] = f"ich_{tenkan_period}_{kijun_period}_{span_b_period}_chikou"
        else:
            # 기타 컬럼이면 그대로
            rename_map[col] = col

    ichimoku_df.rename(columns=rename_map, inplace=True)
    return ichimoku_df


def calc_psar(df: pd.DataFrame, accel_step: float, accel_max: float) -> pd.Series:
    """
    파라볼릭 SAR(PSAR) 계산.

    Args:
        df (pd.DataFrame): "high","low","close" 필요
        accel_step (float)
        accel_max (float)

    Returns:
        pd.Series: 시리즈 이름 예) "psar_{accel_step}_{accel_max}"
    """
    psar_df = ta.psar(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        step=accel_step,
        max=accel_max
    )
    if psar_df is None or psar_df.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"psar_{accel_step}_{accel_max}")

    # psar_df 중 "PSAR_*" 컬럼 추출
    psar_col = [c for c in psar_df.columns if c.upper().startswith("PSAR")]
    if not psar_col:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"psar_{accel_step}_{accel_max}")

    sr_psar = psar_df[psar_col[0]].copy()
    sr_psar.name = f"psar_{accel_step}_{accel_max}"
    return sr_psar


import pandas as pd
import numpy as np
import pandas_ta as ta

def calc_supertrend(df: pd.DataFrame, atr_period: int, multiplier: float) -> pd.DataFrame:
    """
    슈퍼트렌드(Supertrend) 계산.

    Args:
        df (pd.DataFrame): "high","low","close" 필요
        atr_period (int)
        multiplier (float)

    Returns:
        pd.DataFrame:
          - supertrend_{atr_period}_{multiplier}      (메인 라인)
          - supertrendd_{atr_period}_{multiplier}     (방향/모드 컬럼)
          - supertrendl_{atr_period}_{multiplier}     (롱/숏 라인)
          - (만약 pandas‑ta가 추가로 SUPERT… 계열 컬럼을 더 준다면, 자동으로 뒤에 _1, _2 식으로 붙음)
    """
    st_df = ta.supertrend(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        length=atr_period,
        multiplier=multiplier
    )
    # 만약 반환이 비거나 None이면 NaN으로 채운 DF를 반환
    if st_df is None or st_df.empty:
        return pd.DataFrame({
            f"supertrend_{atr_period}_{multiplier}": [np.nan] * len(df),
            f"supertrendd_{atr_period}_{multiplier}": [np.nan] * len(df),
            f"supertrendl_{atr_period}_{multiplier}": [np.nan] * len(df)
        }, index=df.index)

    # rename_map을 만들되, 이미 같은 이름이 있으면 뒤에 _1, _2 식으로 붙여서 중복 방지
    rename_map = {}
    used_names = set()  # 이미 배정된 새 컬럼명 관리
    sup_count = 1       # SUPERT… 중복 시 뒤에 붙일 번호

    for c in st_df.columns:
        c_up = c.upper()

        if "SUPERTD" in c_up:
            new_name = f"supertrendd_{atr_period}_{multiplier}"
        elif "SUPERTL" in c_up:
            new_name = f"supertrendl_{atr_period}_{multiplier}"
        elif "SUPERT" in c_up:
            # 여기 들어오면 여러 SUPERT… 열이 있을 수 있음
            base = f"supertrend_{atr_period}_{multiplier}"
            if base not in used_names:
                new_name = base
            else:
                # 이미 base가 있으면 뒤에 _번호를 붙여서 중복 피하기
                new_name = base + f"_{sup_count}"
                sup_count += 1
        else:
            # SUPERT 아닌 다른 컬럼이면 그대로 사용
            new_name = c

        # 혹시 다른 분기에서 이미 같은 이름을 썼다면 한 번 더 뒤에 번호를 붙임
        while new_name in used_names:
            new_name = new_name + f"_{sup_count}"
            sup_count += 1

        rename_map[c] = new_name
        used_names.add(new_name)

    st_df.rename(columns=rename_map, inplace=True)
    return st_df



def calc_donchian_channel(df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """
    돈채널(Donchian Channel) 계산.

    Args:
        df (pd.DataFrame): "high","low","close" 필요
        lookback (int)

    Returns:
        pd.DataFrame:
          - dcl_{lookback}, dcm_{lookback}, dcu_{lookback}
    """
    dc_df = ta.donchian(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        lower_length=lookback,
        upper_length=lookback
    )
    if dc_df is None or dc_df.empty:
        return pd.DataFrame({
            f"dcl_{lookback}": [np.nan] * len(df),
            f"dcm_{lookback}": [np.nan] * len(df),
            f"dcu_{lookback}": [np.nan] * len(df)
        }, index=df.index)

    dc_df.columns = [
        f"dcl_{lookback}",
        f"dcm_{lookback}",
        f"dcu_{lookback}"
    ]
    return dc_df
