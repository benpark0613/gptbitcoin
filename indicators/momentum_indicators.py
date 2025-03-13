# gptbitcoin/indicators/momentum_indicators.py
# 모멘텀 기반 보조지표 계산 모듈 (소문자 컬럼명 사용)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_rsi(df: pd.DataFrame, lookback: int) -> pd.Series:
    """
    RSI 지표 계산.
    - df["close"]가 필요
    - 반환되는 시리즈 이름 예) "rsi_{lookback}"
    """
    rsi_sr = ta.rsi(df["close"], length=lookback)
    if rsi_sr is None or rsi_sr.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"rsi_{lookback}")
    rsi_sr.name = f"rsi_{lookback}"
    return rsi_sr


def calc_stoch(df: pd.DataFrame, k_period: int, d_period: int) -> pd.DataFrame:
    """
    스토캐스틱(Stochastic) 지표.
    - df["high"], df["low"], df["close"]가 필요
    - 반환 컬럼: "stoch_k_{k_period}_{d_period}", "stoch_d_{k_period}_{d_period}"
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
            f"stoch_k_{k_period}_{d_period}": [np.nan] * len(df),
            f"stoch_d_{k_period}_{d_period}": [np.nan] * len(df)
        }, index=df.index)

    # pandas_ta 반환 컬럼 rename
    stoch_cols = list(stoch_df.columns)
    rename_map = {}
    for c in stoch_cols:
        c_up = c.upper()
        if "STOCHK" in c_up:
            rename_map[c] = f"stoch_k_{k_period}_{d_period}"
        elif "STOCHD" in c_up:
            rename_map[c] = f"stoch_d_{k_period}_{d_period}"
        else:
            rename_map[c] = c
    stoch_df.rename(columns=rename_map, inplace=True)

    return stoch_df

def calc_stoch_rsi(
        df: pd.DataFrame,
        rsi_length: int,
        stoch_length: int,
        k_period: int,
        d_period: int
) -> pd.DataFrame:
    """
    Stochastic RSI 지표를 계산하여 DataFrame 반환.

    pandas-ta가 만드는 기본 컬럼명("STOCHRSIk_14_14_3_5" 등)을
    시그널 로직과 일치시키기 위해 소문자+언더바 형태로 rename한다.

    Returns:
        pd.DataFrame:
          - stoch_rsi_k_{rsi_length}_{stoch_length}_{k_period}_{d_period}
          - stoch_rsi_d_{rsi_length}_{stoch_length}_{k_period}_{d_period}
    """
    stochrsi_df = ta.stochrsi(
        close=df["close"],
        rsi_length=rsi_length,
        length=stoch_length,
        k=k_period,
        d=d_period
    )

    # 결과가 None이거나 empty면 NaN 컬럼 생성
    if stochrsi_df is None or stochrsi_df.empty:
        return pd.DataFrame({
            f"stoch_rsi_k_{rsi_length}_{stoch_length}_{k_period}_{d_period}": [np.nan] * len(df),
            f"stoch_rsi_d_{rsi_length}_{stoch_length}_{k_period}_{d_period}": [np.nan] * len(df)
        }, index=df.index)

    # pandas-ta가 생성한 컬럼명 예: ["STOCHRSIk_14_14_3_5", "STOCHRSId_14_14_3_5"]
    # 이 경우 'STOCHRSIK' / 'STOCHRSID'를 체크해야 함.
    rename_map = {}
    for col in stochrsi_df.columns:
        c_up = col.upper()

        # "STOCHRSIk_..."인 경우 K 라인
        if "STOCHRSIK" in c_up:
            rename_map[col] = f"stoch_rsi_k_{rsi_length}_{stoch_length}_{k_period}_{d_period}"
        # "STOCHRSId_..."인 경우 D 라인
        elif "STOCHRSID" in c_up:
            rename_map[col] = f"stoch_rsi_d_{rsi_length}_{stoch_length}_{k_period}_{d_period}"
        else:
            # 혹시 다른 컬럼명이 포함될 수도 있으므로 그대로 둠
            rename_map[col] = col

    stochrsi_df.rename(columns=rename_map, inplace=True)
    return stochrsi_df


def calc_mfi(df: pd.DataFrame, lookback: int) -> pd.Series:
    """
    MFI(Money Flow Index) 지표 계산.
    - df["high"], df["low"], df["close"], df["volume"]가 필요
    - 반환 시리즈 이름 예) "mfi_{lookback}"
    """
    mfi_sr = ta.mfi(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        volume=df["volume"],
        length=lookback
    )
    if mfi_sr is None or mfi_sr.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name=f"mfi_{lookback}")
    mfi_sr.name = f"mfi_{lookback}"
    return mfi_sr
