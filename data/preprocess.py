# gptbitcoin/data/preprocess.py
# 구글 스타일, 최소한의 한글 주석
# 원본데이터(OHLCV)에 대한 전처리(숫자 변환, 결측 검사)와 보조지표 계산 담당.
# OBV 누적 계산을 이어서 처리할 수 있도록 incremental 함수 추가.

import pandas as pd
from indicators.indicators import calc_all_indicators


def preprocess_ohlcv_data(df: pd.DataFrame, dropna: bool = False) -> pd.DataFrame:
    """
    원본데이터(OHLCV) 전체에 대해:
      1) 숫자 변환
      2) OHLC, volume 결측 검사
      3) calc_all_indicators(df)로 보조지표 계산
      4) dropna=True면 지표 NaN 행 제거

    일반적인 전체 재계산용. OBV는 첫 봉=0으로 시작.
    """
    if df.empty:
        print("[WARN] preprocess_ohlcv_data: 입력 df가 비어 있음.")
        return df

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"[ERROR] 필수 컬럼 '{col}' 누락.")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df[["open","high","low","close","volume"]].isna().any().any():
        raise ValueError("[ERROR] OHLC 또는 volume 컬럼에 NaN 존재. 데이터 무결성 오류.")

    df = calc_all_indicators(df)

    if dropna:
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df


def preprocess_incremental_ohlcv_data(
    df_new: pd.DataFrame,
    old_obv_final: float,
    compare_prev_close: float,
    dropna_indicators: bool = False
) -> pd.DataFrame:
    """
    과거 구간(예: 2024-12-31)까지 DB에 저장된 OBV 누적값(old_obv_final)을 이어서,
    신규 구간 df_new에 대해 OBV 누적 + MA/RSI 등 지표를 계산한다.

    Args:
        df_new (pd.DataFrame):
            신규 구간의 OHLCV. 최소 'datetime_utc','open','high','low','close','volume' 필요
        old_obv_final (float):
            과거 구간 마지막 봉의 OBV 누적값
        compare_prev_close (float):
            신규 구간 첫 봉 직전 종가. 첫 봉 상승/하락 판단에 필요
        dropna_indicators (bool):
            True면 지표 계산 후 NaN 행 제거

    Returns:
        pd.DataFrame:
            - df_new 구간의 지표가 추가된 결과
            - OBV는 old_obv_final을 이어서 계산
            - 다른 롤링 지표는 df_new 범위만 재계산 (초반부 NaN 가능)
            - dropna_indicators=True 시 NaN 행 제거
    """
    if df_new.empty:
        print("[WARN] preprocess_incremental_ohlcv_data: df_new 비어 있음.")
        return df_new

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df_new.columns:
            raise ValueError(f"[ERROR] 필수 컬럼 '{col}'이 df_new에 없음.")
        df_new[col] = pd.to_numeric(df_new[col], errors="coerce")

    if df_new[["open","high","low","close","volume"]].isna().any().any():
        raise ValueError("[ERROR] OHLC나 volume에 NaN 존재. 데이터 오류.")

    # 우선 MA/RSI/Filter 등 롤링 지표 계산
    df_new = calc_all_indicators(df_new)

    # obv_raw가 생성되었을 것. 첫 봉=0 형태.
    # old_obv_final을 이어서 계산하기 위해 증분 계산
    if "obv_raw" not in df_new.columns:
        raise ValueError("[ERROR] obv_raw가 없어 누적 이어붙이기 불가. OBV 설정 확인.")

    # obv_increment = obv_raw.diff() (첫 봉은 obv_raw[0], 보통 0)
    df_new["obv_increment"] = df_new["obv_raw"].diff().fillna(df_new["obv_raw"])

    obv_list = []
    # 첫 봉 계산
    first_close = df_new.iloc[0]["close"]
    first_vol = df_new.iloc[0]["volume"]
    if first_close > compare_prev_close:
        obv_first = old_obv_final + first_vol
    else:
        obv_first = old_obv_final - first_vol
    obv_list.append(obv_first)

    # 2번째 봉부터
    for i in range(1, len(df_new)):
        prev_obv = obv_list[-1]
        inc = df_new.iloc[i]["obv_increment"]  # + or - volume
        obv_val = prev_obv + inc
        obv_list.append(obv_val)

    df_new["obv"] = pd.Series(obv_list, index=df_new.index).round(2)

    # obv_increment 등 임시 컬럼 제거
    if "obv_increment" in df_new.columns:
        df_new.drop(columns=["obv_increment"], inplace=True)
    # obv_raw를 유지할지 여부는 선택
    # df_new.drop(columns=["obv_raw"], inplace=True)  # 필요하다면

    # dropna_indicators=True 시 지표 NaN 행 제거
    if dropna_indicators:
        df_new.dropna(inplace=True)
        df_new.reset_index(drop=True, inplace=True)

    return df_new
