# gptbitcoin/indicators/volume_indicators.py
# 거래량 기반 보조지표 계산 모듈 (칼럼명을 소문자로 수정: close→close, volume→volume, etc.)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_obv(df: pd.DataFrame) -> pd.Series:
    """
    OBV(On-Balance Volume) 지표를 계산한다.
    - df["close"], df["volume"]가 존재해야 함
    - 반환 시리즈의 이름: "OBV"
    """
    obv_sr = ta.obv(
        close=df["close"],
        volume=df["volume"]
    )
    if obv_sr is None or obv_sr.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name="OBV")

    obv_sr.name = "OBV"
    return obv_sr


def calc_vwap(df: pd.DataFrame) -> pd.Series:
    """
    VWAP(Volume Weighted Average Price) 지표를 계산한다.
    pandas_ta의 vwap 기본 계산을 사용.
    - df["high"], df["low"], df["close"], df["volume"]가 필요
    - 반환 시리즈(또는 단일 컬럼 DataFrame) 이름: "VWAP"
    """
    vwap_df = ta.vwap(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        volume=df["volume"]
    )
    if vwap_df is None or (isinstance(vwap_df, pd.DataFrame) and vwap_df.empty):
        return pd.Series([np.nan] * len(df), index=df.index, name="VWAP")

    # pandas_ta.vwap가 DataFrame 형태일 수도 있으므로 처리
    if isinstance(vwap_df, pd.DataFrame):
        vwap_col = vwap_df.columns[0]  # 단일 컬럼이라 가정
        sr_vwap = vwap_df[vwap_col].copy()
        sr_vwap.name = "VWAP"
        return sr_vwap

    # Series 형태인 경우
    vwap_df.name = "VWAP"
    return vwap_df
