# gptbitcoin/strategies/signal_logic.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 프로젝트에서 "새로 추가된" 지표들(MACD, DMI/ADX, Bollinger Bands, Ichimoku, PSAR, SuperTrend, Stochastic 등)을
# 활용한 시그널 로직을 포함한 완성된 예시 코드.

from typing import List

import pandas as pd


def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,
    long_ma_col: str,
    band_filter: float = 0.0,
    signal_col: str = "ma_sig"
) -> pd.DataFrame:
    """
    이동평균 교차 시그널을 계산한다.
    (band_filter가 이미 0.01이라면 1%를 의미한다)

    Args:
        df (pd.DataFrame): 이동평균 칼럼이 들어있는 DataFrame
        short_ma_col (str): 단기 이동평균 칼럼명
        long_ma_col (str): 장기 이동평균 칼럼명
        band_filter (float): 퍼센트로 해석할 소수값 (예: 0.01 -> 1%)
        signal_col (str): 결과 시그널이 기록될 칼럼명

    Returns:
        pd.DataFrame: signal_col에 +1/-1/0 시그널을 추가한 DataFrame
    """
    df[signal_col] = 0

    # band_filter = 0.01 → 1%, 0.02 → 2%, ...
    up_cond = df[short_ma_col] > df[long_ma_col] * (1.0 + band_filter)
    down_cond = df[short_ma_col] < df[long_ma_col] * (1.0 - band_filter)

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1

    return df



def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,
    lower_bound: float = 30.0,
    upper_bound: float = 70.0,
    signal_col: str = "rsi_sig"
) -> pd.DataFrame:
    """
    RSI 기준 시그널.
    rsi < lower_bound → +1
    rsi > upper_bound → -1
    else 0
    """
    df[signal_col] = 0
    df.loc[df[rsi_col] < lower_bound, signal_col] = 1
    df.loc[df[rsi_col] > upper_bound, signal_col] = -1
    return df


def obv_signal(
    df: pd.DataFrame,
    obv_col: str = "obv_raw",
    threshold: float = 0.0,
    signal_col: str = "obv_sig"
) -> pd.DataFrame:
    """
    OBV 값으로 단순 매수/매도.
    obv > threshold → +1
    obv < -threshold → -1
    else 0
    """
    df[signal_col] = 0
    up_cond = df[obv_col] > threshold
    down_cond = df[obv_col] < -threshold

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def filter_rule_signal(
    df: pd.DataFrame,
    close_col: str,
    window: int,
    x_pct: float,
    y_pct: float,
    signal_col: str = "filter_sig"
) -> pd.DataFrame:
    """
    Filter 룰.
    - close > filter_max_{w}*(1+x_pct) → +1
    - close < filter_min_{w}*(1-y_pct) → -1
    - else 0
    """
    df[signal_col] = 0
    max_col = f"filter_max_{window}"
    min_col = f"filter_min_{window}"

    up_cond = df[close_col] > df[max_col] * (1 + x_pct)
    down_cond = df[close_col] < df[min_col] * (1 - y_pct)

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def support_resistance_signal(
    df: pd.DataFrame,
    rolling_min_col: str,
    rolling_max_col: str,
    price_col: str = "close",
    band_pct: float = 0.0,
    signal_col: str = "sr_sig"
) -> pd.DataFrame:
    """
    SR 룰.
    - price > rolling_max*(1+band_pct) → +1
    - price < rolling_min*(1-band_pct) → -1
    - else 0
    """
    df[signal_col] = 0
    up_cond = df[price_col] > df[rolling_max_col] * (1.0 + band_pct)
    down_cond = df[price_col] < df[rolling_min_col] * (1.0 - band_pct)

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def channel_breakout_signal(
    df: pd.DataFrame,
    rolling_min_col: str,
    rolling_max_col: str,
    price_col: str = "close",
    breakout_pct: float = 0.0,
    signal_col: str = "cb_sig"
) -> pd.DataFrame:
    """
    채널 돌파 룰.
    - price > ch_max*(1+breakout_pct) → +1
    - price < ch_min*(1-breakout_pct) → -1
    - else 0
    """
    df[signal_col] = 0
    up_cond = df[price_col] > df[rolling_max_col] * (1.0 + breakout_pct)
    down_cond = df[price_col] < df[rolling_min_col] * (1.0 - breakout_pct)

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def macd_signal(
    df: pd.DataFrame,
    macd_line_col: str,
    macd_signal_col: str,
    signal_col: str = "macd_sig"
) -> pd.DataFrame:
    """
    MACD 시그널.
    - macd_line > macd_signal → +1
    - macd_line < macd_signal → -1
    - else 0
    """
    df[signal_col] = 0
    up_cond = df[macd_line_col] > df[macd_signal_col]
    down_cond = df[macd_line_col] < df[macd_signal_col]

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def dmi_adx_signal(
    df: pd.DataFrame,
    plus_di_col: str,
    minus_di_col: str,
    adx_col: str,
    adx_threshold: float = 25.0,
    signal_col: str = "dmi_sig"
) -> pd.DataFrame:
    """
    DMI/ADX 시그널:
     - adx > adx_threshold & +DI > -DI → +1
     - adx > adx_threshold & -DI > +DI → -1
     - else 0
    """
    df[signal_col] = 0
    strong_trend_cond = df[adx_col] > adx_threshold
    plus_up = df[plus_di_col] > df[minus_di_col]
    minus_up = df[minus_di_col] > df[plus_di_col]

    df.loc[strong_trend_cond & plus_up, signal_col] = 1
    df.loc[strong_trend_cond & minus_up, signal_col] = -1
    return df


def stochastic_signal(
    df: pd.DataFrame,
    stoch_k_col: str,
    stoch_d_col: str,
    signal_col: str = "stoch_sig",
    k_overbought: float = 80.0,
    k_oversold: float = 20.0,
    cross_logic: bool = False
) -> pd.DataFrame:
    """
    스토캐스틱 시그널.
    1) 단순 오버보트/오버솔드:
       - stoch_k < k_oversold → +1
       - stoch_k > k_overbought → -1
    2) cross_logic=True 라면, K가 D를 상향 돌파하면 +1, 하향 돌파하면 -1

    Args:
        df (pd.DataFrame): stoch_k_col, stoch_d_col이 존재
        stoch_k_col (str): %K
        stoch_d_col (str): %D
        signal_col (str): 결과 시그널
        k_overbought (float): 예 80
        k_oversold (float): 예 20
        cross_logic (bool): 교차 로직 사용 여부

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    if not cross_logic:
        # 단순 오버보트/솔드
        df.loc[df[stoch_k_col] < k_oversold, signal_col] = 1
        df.loc[df[stoch_k_col] > k_overbought, signal_col] = -1
    else:
        # cross: k가 d를 상향 돌파 → +1, 하향 돌파 → -1
        df["prev_k"] = df[stoch_k_col].shift(1)
        df["prev_d"] = df[stoch_d_col].shift(1)

        up_cond = (df["prev_k"] < df["prev_d"]) & (df[stoch_k_col] > df[stoch_d_col])
        down_cond = (df["prev_k"] > df["prev_d"]) & (df[stoch_k_col] < df[stoch_d_col])

        df.loc[up_cond, signal_col] = 1
        df.loc[down_cond, signal_col] = -1
        df.drop(columns=["prev_k", "prev_d"], inplace=True)

    return df


def bollinger_signal(
    df: pd.DataFrame,
    mid_col: str,
    upper_col: str,
    lower_col: str,
    price_col: str = "close",
    signal_col: str = "boll_sig"
) -> pd.DataFrame:
    """
    볼린저 밴드 시그널(단순 예시).
    - price > upper_col → +1
    - price < lower_col → -1
    - else 0
    """
    df[signal_col] = 0
    up_cond = df[price_col] > df[upper_col]
    down_cond = df[price_col] < df[lower_col]

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def ichimoku_signal(
    df: pd.DataFrame,
    tenkan_col: str,
    kijun_col: str,
    span_a_col: str,
    span_b_col: str,
    chikou_col: str,
    price_col: str = "close",
    signal_col: str = "ich_sig"
) -> pd.DataFrame:
    """
    일목균형표 기초 시그널(간단 예시):
      - price > span_a, price > span_b → +1
      - price < span_a, price < span_b → -1
      - else 0
    (프로젝트마다 기준선, 전환선 교차로직을 쓰거나 치코 스팬 확인 등 다양하게 변형 가능)
    """
    df[signal_col] = 0
    up_cond = (df[price_col] > df[span_a_col]) & (df[price_col] > df[span_b_col])
    down_cond = (df[price_col] < df[span_a_col]) & (df[price_col] < df[span_b_col])

    df.loc[up_cond, signal_col] = 1
    df.loc[down_cond, signal_col] = -1
    return df


def psar_signal(
    df: pd.DataFrame,
    psar_col: str,
    price_col: str = "close",
    signal_col: str = "psar_sig"
) -> pd.DataFrame:
    """
    PSAR 시그널(간단):
    - if psar < price → +1
    - else → -1
    """
    df[signal_col] = 0
    up_cond = df[psar_col] < df[price_col]
    df.loc[up_cond, signal_col] = 1
    df.loc[~up_cond, signal_col] = -1
    return df


def supertrend_signal(
    df: pd.DataFrame,
    st_col: str,
    price_col: str = "close",
    signal_col: str = "st_sig"
) -> pd.DataFrame:
    """
    SuperTrend 시그널:
    - price > supertrend → +1
    - price < supertrend → -1
    """
    df[signal_col] = 0
    up_cond = df[price_col] > df[st_col]
    df.loc[up_cond, signal_col] = 1
    df.loc[~up_cond, signal_col] = -1
    return df


def fibo_signal(
    df: pd.DataFrame,
    fibo_cols: List[str],
    price_col: str = "close",
    mode: str = "above_last",
    signal_col: str = "fibo_sig"
) -> pd.DataFrame:
    """
    피보나치 레벨 기반 시그널(단순 예시).
    여러 fibo_cols 중 마지막 레벨을 기준으로 price가 위면 +1, 아래면 -1, etc.

    Args:
        df (pd.DataFrame): fibo_* 컬럼이 이미 계산돼 있어야 함
        fibo_cols (List[str]): 예) ["fibo_0.382_set1", "fibo_0.5_set1", ...]
        price_col (str): 기준 가격
        mode (str): 단순 예시, "above_last" → 마지막 피보 레벨보다 위/아래로 매수/매도
        signal_col (str): 결과 시그널

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    if not fibo_cols:
        return df  # no action

    if mode == "above_last":
        last_col = fibo_cols[-1]
        up_cond = df[price_col] > df[last_col]
        down_cond = df[price_col] < df[last_col]

        df.loc[up_cond, signal_col] = 1
        df.loc[down_cond, signal_col] = -1
    else:
        # 다른 모드: price가 가장 가까운 레벨 위인지 아래인지 등등 구현 가능
        pass
    return df


def combine_signals(
    df: pd.DataFrame,
    signal_cols: List[str],
    out_col: str = "final_signal"
) -> pd.DataFrame:
    """
    여러 시그널 컬럼을 합산하여 최종 시그널을 만든다.
    sum_val > 0 → +1
    sum_val < 0 → -1
    else 0
    """
    df[out_col] = 0
    df["temp_sum"] = 0

    for sc in signal_cols:
        df["temp_sum"] += df[sc]

    df.loc[df["temp_sum"] > 0, out_col] = 1
    df.loc[df["temp_sum"] < 0, out_col] = -1

    df.drop(columns=["temp_sum"], inplace=True)
    return df
