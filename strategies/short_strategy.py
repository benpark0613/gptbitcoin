# gptbitcoin/strategies/short_strategy.py

import pandas as pd
import numpy as np


def short_strategy(signal_series: pd.Series) -> pd.Series:
    """
    숏 허용 전략:
      - +1 신호면 롱(1)
      - -1 신호면 숏(-1)
      -  0 신호면 현금(0)

    Parameters
    ----------
    signal_series : pd.Series
        +1(매수), -1(매도), 0(중립)으로 구성된 지표 신호 시리즈
        (MA 교차, RSI 등에서 생성된 것).

    Returns
    -------
    pd.Series
        최종 포지션 시리즈:
          +1 → 롱, -1 → 숏, 0 → 현금
        인덱스와 길이는 signal_series와 동일.

    Notes
    -----
    - 이 함수는 ALLOW_SHORT = True 환경에서 -1 신호를 숏 포지션으로 받아들인다.
    - ALLOW_SHORT = False인 상황에서는
      백테스트 엔진에서 -1을 0으로 처리하게 할 수도 있다.
    - 실제 체결(슬리피지, 수수료 등)은 별도의 백테스트 엔진에서 수행한다.
    """

    # signal_series에 나온 신호를 그대로 포지션으로 사용
    # +1 → 롱, -1 → 숏,  0 → 현금
    # numpy 배열로 변환해 포지션 시리즈 생성
    positions = signal_series.values.astype(int)

    return pd.Series(positions, index=signal_series.index, name="short_position")
