# gptbitcoin/data/preprocess.py
# 구글 스타일, 최소한의 한글 주석
# 이 모듈은 원본데이터(OHLCV) 전처리와 보조지표 계산만 담당한다.
# DB 관련 로직은 모두 update_data.py 등으로 분리. 여기서는 NaN이 발생하면 즉시 예외를 발생시킨다.

import os
import sys
import pandas as pd
from typing import Optional

from indicators.indicators import calc_all_indicators

def preprocess_ohlcv_data(df: pd.DataFrame, dropna: bool = False) -> pd.DataFrame:
    """
    원본데이터(OHLCV)에 대해:
      1) 숫자 형 변환
      2) OHLC, volume 중 하나라도 NaN이면 예외 발생 후 종료
      3) 보조지표(calc_all_indicators) 계산
      4) (옵션) dropna=True면, 지표 계산 후 발생한 NaN(지표 컬럼)도 제거

    Args:
        df (pd.DataFrame): 최소한 'open','high','low','close','volume' 열을 포함해야 함
        dropna (bool): True 시, 보조지표 계산 후 NaN 존재 행을 제거

    Returns:
        pd.DataFrame: 보조지표가 추가된 전처리 완료 DataFrame
    """
    if df.empty:
        print("[WARN] preprocess_ohlcv_data: 입력 df가 비어 있습니다.")
        return df

    # 1) 숫자 형 변환
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"[ERROR] 필수 컬럼 '{col}'이 DataFrame에 없습니다.")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 2) OHLC, volume 중 하나라도 NaN이면 예외 발생
    if df[["open", "high", "low", "close", "volume"]].isna().any().any():
        raise ValueError("[ERROR] OHLC 또는 volume 컬럼에서 NaN이 발견되었습니다. "
                         "데이터 무결성 오류로 프로세스를 중단합니다.")

    # 3) 보조지표 계산
    df = calc_all_indicators(df)

    # 4) dropna=True인 경우, 지표 계산 후 지표 컬럼에서 발생한 NaN을 제거
    #    (지표 파라미터에 따라 초반 기간이 NaN이 될 수 있음)
    if dropna:
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df