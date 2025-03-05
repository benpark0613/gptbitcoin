# gptbitcoin/strategies/inout_strategy.py

import pandas as pd
import numpy as np


def inout_strategy(signal_series):
    """
    In-Out 전략:
      - signal_series가 +1이면 '롱(1)' 포지션
      - signal_series가 -1 또는 0이면 '현금(0)'

    Parameters
    ----------
    signal_series : pd.Series
        각 캔들(시간)에 대한 지표 기반 신호 (+1, -1, 0).
        예: ma_cross_signal, rsi_signal 등

    Returns
    -------
    pd.Series
        +1(롱), 0(현금)로 구성된 최종 포지션 시리즈.
    """
    # +1 → in(1), 그 외(-1,0) → out(0)
    positions = np.where(signal_series == 1, 1, 0)
    return pd.Series(positions, index=signal_series.index, name="inout_position")
