# gptbitcoin/indicators/momentum_indicators.py
# 모멘텀 기반 보조지표 계산 모듈 (칼럼명을 소문자로 수정: close → close, high → high, 등)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_rsi(df: pd.DataFrame, lookback: int) -> pd.Series:
    """
    RSI 지표 계산.
    - df["close"]가 존재해야 함
    - 반환되는 시리즈의 이름 예) "RSI_{lookback}"
    """
    rsi_sr = ta.rsi(df["close"], length=lookback)
    if rsi_sr is None or rsi_sr.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"RSI_{lookback}")
    rsi_sr.name = f"RSI_{lookback}"
    return rsi_sr


def calc_stoch(df: pd.DataFrame, k_period: int, d_period: int) -> pd.DataFrame:
    """
    스토캐스틱(Stochastic) 지표 계산.
    - df["high"], df["low"], df["close"]가 존재해야 함
    - 기본적으로 STochK, STochD 컬럼(버전에 따라 다름) 반환.
    """
    stoch_df = ta.stoch(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        k=k_period,
        d=d_period
    )
    if stoch_df is None or stoch_df.empty:
        return pd.DataFrame({
            f"STOCHk_{k_period}_{d_period}": [np.nan] * len(df),
            f"STOCHd_{k_period}_{d_period}": [np.nan] * len(df)
        }, index=df.index)

    # pandas_ta에서 반환된 컬럼을 명시적으로 변경
    stoch_cols = list(stoch_df.columns)
    new_cols = {}
    for c in stoch_cols:
        c_up = c.upper()
        if "STOCHK" in c_up:
            new_cols[c] = f"STOCHk_{k_period}_{d_period}"
        elif "STOCHD" in c_up:
            new_cols[c] = f"STOCHd_{k_period}_{d_period}"
        else:
            new_cols[c] = c
    stoch_df.rename(columns=new_cols, inplace=True)

    return stoch_df


def calc_stoch_rsi(df: pd.DataFrame, lookback: int, k_period: int, d_period: int) -> pd.DataFrame:
    """
    스토캐스틱 RSI(Stoch RSI) 지표 계산.
    - df["close"]가 필요
    - pandas_ta.stochrsi 사용
    - 결과 컬럼(S/R)에 "STOCH_RSIk_x_x_x", "STOCH_RSId_x_x_x" 형태로 이름 부여
    """
    stochrsi_df = ta.stochrsi(
        close=df["close"],
        length=lookback,
        rsi_length=lookback,
        k=k_period,
        d=d_period
    )
    if stochrsi_df is None or stochrsi_df.empty:
        return pd.DataFrame({
            f"STOCH_RSIk_{lookback}_{k_period}_{d_period}": [np.nan] * len(df),
            f"STOCH_RSId_{lookback}_{k_period}_{d_period}": [np.nan] * len(df)
        }, index=df.index)

    stochrsi_cols = list(stochrsi_df.columns)
    new_cols = {}
    for c in stochrsi_cols:
        c_up = c.upper()
        if "STOCHRSI_K" in c_up:
            new_cols[c] = f"STOCH_RSIk_{lookback}_{k_period}_{d_period}"
        elif "STOCHRSI_D" in c_up:
            new_cols[c] = f"STOCH_RSId_{lookback}_{k_period}_{d_period}"
        else:
            new_cols[c] = c
    stochrsi_df.rename(columns=new_cols, inplace=True)

    return stochrsi_df


def calc_mfi(df: pd.DataFrame, lookback: int) -> pd.Series:
    """
    MFI(Money Flow Index) 지표 계산.
    - df["high"], df["low"], df["close"], df["volume"]가 필요
    - 반환 시리즈 이름 예) "MFI_{lookback}"
    """
    mfi_sr = ta.mfi(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        volume=df["volume"],
        length=lookback
    )
    if mfi_sr is None or mfi_sr.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"MFI_{lookback}")
    mfi_sr.name = f"MFI_{lookback}"
    return mfi_sr
