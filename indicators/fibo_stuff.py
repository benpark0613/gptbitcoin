# gptbitcoin/indicators/fibo_stuff.py
# 최소한의 한글 주석
# 구글 스타일 Docstring

from typing import List
import numpy as np
import pandas as pd

def calc_fibonacci_levels(
    high_s: pd.Series,
    low_s: pd.Series,
    levels: List[float],
    rolling_window: int = 20
) -> pd.DataFrame:
    """
    피보나치 레벨을 numpy로 rolling 윈도우 계산한다.

    Args:
        high_s (pd.Series): 고가
        low_s (pd.Series): 저가
        levels (List[float]): 예) [0.382, 0.5, 0.618]
        rolling_window (int): 윈도우 크기

    Returns:
        pd.DataFrame: fibo_{lv} 컬럼들을 갖는 DataFrame
    """
    h = high_s.to_numpy(dtype=float)
    l = low_s.to_numpy(dtype=float)
    n = len(h)
    if len(l) != n:
        raise ValueError("길이가 달라서는 안 됨.")

    roll_max = np.full(n, np.nan)
    roll_min = np.full(n, np.nan)
    for i in range(n):
        start = max(0, i - rolling_window + 1)
        roll_max[i] = h[start:i+1].max()
        roll_min[i] = l[start:i+1].min()

    data_dict = {}
    for lv in levels:
        col_name = f"fibo_{lv}"
        data_dict[col_name] = roll_min + (roll_max - roll_min) * lv

    return pd.DataFrame(data_dict, index=high_s.index)
