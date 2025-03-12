# gptbitcoin/indicators/fibo_stuff.py
# 피보나치 핵심 파라미터(피보 레벨, rolling_window)만 사용하여 직접 계산.

import pandas as pd
import numpy as np
import pandas_ta as ta
from typing import List


def calc_fibonacci_levels(
    high_s: pd.Series,
    low_s: pd.Series,
    levels: List[float],
    rolling_window: int = 20
) -> pd.DataFrame:
    """피보나치 레벨 계산."""
    roll_max = high_s.rolling(window=rolling_window, min_periods=rolling_window).max()
    roll_min = low_s.rolling(window=rolling_window, min_periods=rolling_window).min()

    if roll_max is None or roll_min is None:
        roll_max = pd.Series([np.nan] * len(high_s), index=high_s.index)
        roll_min = pd.Series([np.nan] * len(low_s), index=low_s.index)

    data_dict = {}
    for lv in levels:
        data_dict[f"fibo_{lv}"] = roll_min + (roll_max - roll_min) * lv

    return pd.DataFrame(data_dict, index=high_s.index)
