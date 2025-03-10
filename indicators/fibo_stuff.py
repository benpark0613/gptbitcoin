# gptbitcoin/indicators/fibo_stuff.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 모듈은 "피보나치(Fibonacci)" 관련 계산 함수들을 담는다.
# (되돌림(Retracement), 확장(Extension) 등 다양한 방식이 존재하나, 여기서는 예시로 몇 가지를 구현)

import pandas as pd
import numpy as np
from typing import List


def calc_fibonacci_levels(
    high_s: pd.Series,
    low_s: pd.Series,
    levels: List[float],
    mode: str = "rolling"
) -> pd.DataFrame:
    """
    피보나치 레벨을 계산한다.
    여기서는 간단히 "최근까지의 최고가, 최저가"를 구한 뒤
    그 범위(min~max)를 기준으로 특정 비율(levels) 지점을 시계열화한다.

    mode 파라미터에 따라 구현이 다를 수 있다:
      - "rolling": 각 시점까지의 rolling max/min을 이용해 fibo 레벨을 시계열로 계산
      - "cumulative": 시작부터 현재까지의 누적 max/min으로 계산
      - "latest": 가장 최근 구간 max/min만 한 번 계산 (시계열화는 X)

    이 예시는 "rolling" 모드만 실제로 동작하며, 다른 mode는 간단히 예시 처리를 함.

    Args:
        high_s (pd.Series): 고가 시리즈
        low_s (pd.Series): 저가 시리즈
        levels (List[float]): 예) [0.382, 0.5, 0.618]
        mode (str, optional): "rolling", "cumulative", "latest" 등. 기본 "rolling"

    Returns:
        pd.DataFrame: 각 level마다 "fibo_{level}" 컬럼을 가진 DataFrame
                      (ex: fibo_0.382, fibo_0.5, fibo_0.618)
    """
    if len(high_s) != len(low_s):
        raise ValueError("high_s와 low_s의 길이가 다릅니다.")

    # rolling max/min
    if mode == "rolling":
        rolling_max = high_s.cummax()
        rolling_min = low_s.cummin()
    elif mode == "cumulative":
        # 간단 구현: 전체 기간의 global max/min을 사용 -> 모든 행이 동일 값
        global_max = high_s.max()
        global_min = low_s.min()
        rolling_max = pd.Series([global_max] * len(high_s), index=high_s.index)
        rolling_min = pd.Series([global_min] * len(low_s), index=low_s.index)
    else:
        # "latest" 같은 경우, 맨 마지막 시점의 high/low로만 계산 -> 나머지는 NaN
        last_h = high_s.iloc[-1]
        last_l = low_s.iloc[-1]
        # 여기서는 그냥 global or final approach
        # 단순 예: 최근 1개 값만
        rolling_max = pd.Series([np.nan]*(len(high_s)-1) + [last_h], index=high_s.index)
        rolling_min = pd.Series([np.nan]*(len(low_s)-1) + [last_l], index=low_s.index)

    data_dict = {}
    for lv in levels:
        col_name = f"fibo_{lv}"
        # 예: fibo_level = rolling_min + (rolling_max - rolling_min) * lv
        # retracement 개념 시 (1-lv) 쓸 수도 있으나 여긴 예시
        data_dict[col_name] = rolling_min + (rolling_max - rolling_min) * lv

    df_fibo = pd.DataFrame(data_dict, index=high_s.index)
    return df_fibo


def calc_fibonacci_retracement_once(
    swing_high: float,
    swing_low: float,
    levels: List[float]
) -> pd.DataFrame:
    """
    단일 구간에 대한 피보나치 되돌림 레벨을 계산한다.
    종종 "최근 스윙 고점/저점"을 수동으로 찾은 뒤 그에 대한 레벨만 구할 때 사용.

    Args:
        swing_high (float): 최근 스윙 고가
        swing_low (float): 최근 스윙 저가
        levels (List[float]): 예) [0.382, 0.5, 0.618]

    Returns:
        pd.DataFrame: 1행만 존재, 각 컬럼이 fibo_{level} 형태로
    """
    if swing_low > swing_high:
        raise ValueError("swing_low가 swing_high보다 큽니다. 값 확인필요.")

    row_data = {}
    for lv in levels:
        col_name = f"fibo_{lv}"
        row_data[col_name] = swing_low + (swing_high - swing_low) * lv

    df = pd.DataFrame([row_data])
    return df
