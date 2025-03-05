# gptbitcoin/strategies/buy_and_hold_strategy.py

"""
buy_and_hold_strategy.py

Buy & Hold 전략 모듈.
전 기간 동안 포지션을 +1(롱)으로 유지하고,
매도나 숏 전환을 하지 않는 가장 단순한 전략.

사용법:
    from strategies.buy_and_hold_strategy import buy_and_hold_signals
    ...
    position_series = buy_and_hold_signals(df)
    result, trades_info = run_backtest(df, position_series, ...)
    metrics = summarize_metrics(result, trades_info, ...)
"""

import pandas as pd

def buy_and_hold_signals(df: pd.DataFrame, params: dict = None) -> pd.Series:
    """
    Buy & Hold 전략 신호 생성 함수.

    Parameters
    ----------
    df : pd.DataFrame
        'close' 등 시계열 데이터를 포함한 DataFrame.
        인덱스는 시간축(날짜 등) 순서.
    params : dict, optional
        다른 전략들과 인터페이스를 맞추기 위해 포함.
        이 전략은 별도 파라미터를 사용하지 않는다.

    Returns
    -------
    pd.Series
        +1(롱) 값으로 구성된 시리즈. df와 같은 인덱스.
        예) 모든 캔들에서 포지션 = +1
    """
    return pd.Series([1]*len(df), index=df.index, name="buy_and_hold_signal")
