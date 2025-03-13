# gptbitcoin/indicators/trend_indicators.py
# 추세 기반 보조지표 계산 모듈 (소문자 컬럼명 사용: df["close"], df["high"], df["low"], df["volume"])

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_ma(df: pd.DataFrame, short_period: int, long_period: int) -> pd.DataFrame:
    """
    이동평균(MA) 계산 (단기·장기선).
    df["close"]가 필요.
    결과 DataFrame 예시 컬럼:
      - "MA_short_{short_period}"
      - "MA_long_{long_period}"
    """
    short_ma = ta.sma(df["close"], length=short_period)
    if short_ma is None or short_ma.empty:
        short_ma = pd.Series([np.nan] * len(df), index=df.index)

    long_ma = ta.sma(df["close"], length=long_period)
    if long_ma is None or long_ma.empty:
        long_ma = pd.Series([np.nan] * len(df), index=df.index)

    out_df = pd.DataFrame({
        f"MA_short_{short_period}": short_ma,
        f"MA_long_{long_period}": long_ma
    }, index=df.index)
    return out_df


def calc_macd(df: pd.DataFrame, fast_period: int, slow_period: int, signal_period: int) -> pd.DataFrame:
    """
    MACD 지표 계산.
    df["close"]가 필요.
    pandas_ta.macd → ["MACD_*", "MACDh_*", "MACDs_*"] 컬럼 생성
    결과 DataFrame 컬럼명 예시:
      - MACD_{fast_period}_{slow_period}_{signal_period}
      - MACDh_{fast_period}_{slow_period}_{signal_period}
      - MACDs_{fast_period}_{slow_period}_{signal_period}
    """
    macd_df = ta.macd(
        df["close"],
        fast=fast_period,
        slow=slow_period,
        signal=signal_period
    )
    if macd_df is None or macd_df.empty:
        macd_df = pd.DataFrame({
            "MACD_0": np.nan,
            "MACDh_0": np.nan,
            "MACDs_0": np.nan
        }, index=df.index)

    macd_df.columns = [
        f"MACD_{fast_period}_{slow_period}_{signal_period}",
        f"MACDh_{fast_period}_{slow_period}_{signal_period}",
        f"MACDs_{fast_period}_{slow_period}_{signal_period}"
    ]
    return macd_df


def calc_dmi_adx(df: pd.DataFrame, lookback_period: int) -> pd.DataFrame:
    """
    DMI(+DI, -DI)와 ADX 지표 계산.
    df["high"], df["low"], df["close"]가 필요.
    pandas_ta.adx → ["DMP_*", "DMN_*", "ADX_*"] 형태로 이름 변경
    """
    adx_df = ta.adx(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        length=lookback_period
    )
    if adx_df is None or adx_df.empty:
        adx_df = pd.DataFrame({
            "DMP_0": np.nan,
            "DMN_0": np.nan,
            "ADX_0": np.nan
        }, index=df.index)

    adx_df.columns = [
        f"DMP_{lookback_period}",
        f"DMN_{lookback_period}",
        f"ADX_{lookback_period}"
    ]
    return adx_df


def calc_ichimoku(df: pd.DataFrame, tenkan_period: int, kijun_period: int, span_b_period: int) -> pd.DataFrame:
    """
    일목균형표(전환선, 기준선, 선행스팬A,B 등) 계산.
    df["high"], df["low"], df["close"] 필요.
    pandas_ta.ichimoku → 기본 MultiIndex 반환 가능, flatten 처리 후 rename
    결과 예시 컬럼명:
      - ITS_{tenkan_period}_{kijun_period}_{span_b_period}
      - IKS_...
      - ICS_...
      - ISA_...
      - ISB_...
    """
    ichimoku_df = ta.ichimoku(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        tenkan=tenkan_period,
        kijun=kijun_period,
        senkou=span_b_period
    )
    if ichimoku_df is None or ichimoku_df.empty:
        ichimoku_df = pd.DataFrame({
            "ITS_0": np.nan,
            "IKS_0": np.nan,
            "ICS_0": np.nan,
            "ISA_0": np.nan,
            "ISB_0": np.nan
        }, index=df.index)

    if isinstance(ichimoku_df.columns, pd.MultiIndex):
        ichimoku_df.columns = ["_".join(col).strip() for col in ichimoku_df.columns.values]

    new_cols = {}
    for c in ichimoku_df.columns:
        if "ITS" in c:
            new_cols[c] = f"ITS_{tenkan_period}_{kijun_period}_{span_b_period}"
        elif "IKS" in c:
            new_cols[c] = f"IKS_{tenkan_period}_{kijun_period}_{span_b_period}"
        elif "ICS" in c:
            new_cols[c] = f"ICS_{tenkan_period}_{kijun_period}_{span_b_period}"
        elif "ISA" in c:
            new_cols[c] = f"ISA_{tenkan_period}_{kijun_period}_{span_b_period}"
        elif "ISB" in c:
            new_cols[c] = f"ISB_{tenkan_period}_{kijun_period}_{span_b_period}"
        else:
            new_cols[c] = c
    ichimoku_df.rename(columns=new_cols, inplace=True)

    return ichimoku_df


def calc_psar(df: pd.DataFrame, accel_step: float, accel_max: float) -> pd.Series:
    """
    파라볼릭 SAR(PSAR) 지표 계산.
    df["high"], df["low"], df["close"] 필요.
    pandas_ta.psar → PSAR* 컬럼 반환, 여기서는 PSAR 컬럼만 사용
    결과 시리즈명 예) "PSAR_{accel_step}_{accel_max}"
    """
    psar_df = ta.psar(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        step=accel_step,
        max=accel_max
    )
    if psar_df is None or psar_df.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"PSAR_{accel_step}_{accel_max}")

    col_psar = [c for c in psar_df.columns if c.startswith("PSAR_")]
    if not col_psar:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"PSAR_{accel_step}_{accel_max}")

    sr_psar = psar_df[col_psar[0]].copy()
    sr_psar.name = f"PSAR_{accel_step}_{accel_max}"
    return sr_psar


def calc_supertrend(df: pd.DataFrame, atr_period: int, multiplier: float) -> pd.DataFrame:
    """
    슈퍼트렌드(Supertrend) 지표 계산.
    df["high"], df["low"], df["close"] 필요.
    pandas_ta.supertrend → 기본적으로 3개 컬럼 반환 (SUPERT, SUPERTd, SUPERTl 등)
    결과 컬럼명 예) "SUPERT_{atr_period}_{multiplier}", ...
    """
    st_df = ta.supertrend(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        length=atr_period,
        multiplier=multiplier
    )
    if st_df is None or st_df.empty:
        return pd.DataFrame({
            f"SUPERT_{atr_period}_{multiplier}": [np.nan] * len(df),
            f"SUPERTd_{atr_period}_{multiplier}": [np.nan] * len(df),
            f"SUPERTl_{atr_period}_{multiplier}": [np.nan] * len(df)
        }, index=df.index)

    st_cols = list(st_df.columns)
    new_cols = {}
    for c in st_cols:
        c_up = c.upper()
        if "SUPERT" in c_up and "D" not in c_up and "L" not in c_up:
            new_cols[c] = f"SUPERT_{atr_period}_{multiplier}"
        elif "SUPERTD" in c_up:
            new_cols[c] = f"SUPERTd_{atr_period}_{multiplier}"
        elif "SUPERTL" in c_up:
            new_cols[c] = f"SUPERTl_{atr_period}_{multiplier}"
        else:
            new_cols[c] = c
    st_df.rename(columns=new_cols, inplace=True)
    return st_df


def calc_donchian_channel(df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """
    돈채널(Donchian Channel) 지표 계산.
    df["high"], df["low"], df["close"] 필요.
    pandas_ta.donchian → 기본적으로 3개 컬럼 반환 (DCL, DCM, DCU)
    결과 컬럼명 예) "DCL_{lookback}", "DCM_{lookback}", "DCU_{lookback}"
    """
    donchian_df = ta.donchian(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        lower_length=lookback,
        upper_length=lookback
    )
    if donchian_df is None or donchian_df.empty:
        return pd.DataFrame({
            f"DCL_{lookback}": [np.nan] * len(df),
            f"DCM_{lookback}": [np.nan] * len(df),
            f"DCU_{lookback}": [np.nan] * len(df)
        }, index=df.index)

    donchian_df.columns = [
        f"DCL_{lookback}",
        f"DCM_{lookback}",
        f"DCU_{lookback}"
    ]
    return donchian_df
