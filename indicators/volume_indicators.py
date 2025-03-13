# gptbitcoin/indicators/volume_indicators.py
# 거래량 기반 보조지표 계산 모듈 (소문자 컬럼명 사용)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_obv(df: pd.DataFrame) -> pd.Series:
    """
    OBV(On-Balance Volume) 지표 계산.
    - df["close"], df["volume"] 필요
    - 반환 시리즈 이름: "obv_raw"
    """
    obv_sr = ta.obv(
        close=df["close"],
        volume=df["volume"]
    )
    if obv_sr is None or obv_sr.empty:
        return pd.Series([np.nan] * len(df), index=df.index, name="obv_raw")

    obv_sr.name = "obv_raw"
    return obv_sr


def calc_vwap(df: pd.DataFrame) -> pd.Series:
    """
    VWAP(Volume Weighted Average Price) 지표 계산.
    - df["high"], df["low"], df["close"], df["volume"] 필요
    - 반환 시리즈(또는 단일 컬럼 DataFrame) 이름: "vwap"
    """
    vwap_data = ta.vwap(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        volume=df["volume"]
    )
    if vwap_data is None:
        return pd.Series([np.nan] * len(df), index=df.index, name="vwap")

    # pandas_ta.vwap가 DataFrame 형태로 반환될 수 있으므로 처리
    if isinstance(vwap_data, pd.DataFrame):
        # 보통 단일 컬럼이므로 그 첫 번째 컬럼을 'vwap'으로 rename
        col = vwap_data.columns[0]
        sr_vwap = vwap_data[col].copy()
        sr_vwap.name = "vwap"
        return sr_vwap

    # Series 형태인 경우
    vwap_data.name = "vwap"
    return vwap_data
