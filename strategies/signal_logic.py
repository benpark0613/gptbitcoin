# gptbitcoin/strategies/signal_logic.py
# 보조지표 기반 매매 시그널 로직
# 컬럼 이름은 현재 지표 계산 로직(calc_all_indicators)에서 만드는 형식에 맞춤
# 시그널은 단순 합산하여 +면 매수(1), -면 매도(-1), 0이면 관망(0)

import numpy as np
import pandas as pd


def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,   # 예: "ma_5", "ma_20"
    long_ma_col: str,    # 예: "ma_50", "ma_200"
    signal_col: str = "signal_ma"
) -> pd.DataFrame:
    """
    단기MA와 장기MA가 골든/데드크로스하는 지점에서 매매 시그널(1, -1, 0)을 계산.

    Args:
        df (pd.DataFrame): 'short_ma_col', 'long_ma_col'이 포함된 데이터프레임
        short_ma_col (str): 단기MA 칼럼명 (ex. "ma_5")
        long_ma_col (str): 장기MA 칼럼명 (ex. "ma_200")
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col 칼럼에 1(매수), -1(매도), 0(관망) 할당
    """
    if short_ma_col not in df.columns or long_ma_col not in df.columns:
        raise ValueError(f"MA 칼럼이 존재하지 않습니다: {short_ma_col}, {long_ma_col}")

    df[signal_col] = 0
    df.loc[df[short_ma_col] > df[long_ma_col], signal_col] = 1   # 골든크로스
    df.loc[df[short_ma_col] < df[long_ma_col], signal_col] = -1  # 데드크로스
    return df


def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,         # 예: "rsi_14", "rsi_30"
    lower_bound: float = 30.0,
    upper_bound: float = 70.0,
    signal_col: str = "signal_rsi"
) -> pd.DataFrame:
    """
    RSI 지표를 이용해 과매도/과매수 구간에서 매수/매도 시그널을 계산.

    Args:
        df (pd.DataFrame): rsi_col이 포함된 DF
        rsi_col (str): RSI 칼럼명 (ex. "rsi_14")
        lower_bound (float, optional): 과매도 기준
        upper_bound (float, optional): 과매수 기준
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame
    """
    if rsi_col not in df.columns:
        raise ValueError(f"RSI 칼럼이 존재하지 않습니다: {rsi_col}")

    df[signal_col] = 0
    df.loc[df[rsi_col] < lower_bound, signal_col] = 1
    df.loc[df[rsi_col] > upper_bound, signal_col] = -1
    return df


def obv_signal(
    df: pd.DataFrame,
    obv_col: str = "obv",   # calc_all_indicators에서 "obv"가 기본
    threshold: float = 0.0,
    signal_col: str = "signal_obv"
) -> pd.DataFrame:
    """
    OBV를 기준으로 threshold 위면 매수, 아래면 매도하는 단순 시그널.

    Args:
        df (pd.DataFrame): obv_col이 포함된 DF
        obv_col (str, optional): OBV 칼럼명 (ex. "obv", "obv_sma_20")
        threshold (float, optional): 기준값
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame
    """
    if obv_col not in df.columns:
        raise ValueError(f"OBV 칼럼이 존재하지 않습니다: {obv_col}")

    df[signal_col] = 0
    df.loc[df[obv_col] > threshold, signal_col] = 1
    df.loc[df[obv_col] < threshold, signal_col] = -1
    return df


def filter_rule_signal(
    df: pd.DataFrame,
    close_col: str = "close",
    window: int = 10,
    x_pct: float = 0.05,
    y_pct: float = 0.05,
    signal_col: str = "signal_filter"
) -> pd.DataFrame:
    """
    필터룰: 최근 window 기간 최저가 대비 x% 이상 상승 -> 매수,
            최근 window 기간 최고가 대비 y% 이상 하락 -> 매도
    내부에서 rolling으로 min/max 계산.

    Args:
        df (pd.DataFrame)
        close_col (str): 종가 칼럼명
        window (int): lookback window
        x_pct (float): 상승률 기준
        y_pct (float): 하락률 기준
        signal_col (str): 결과 신호 칼럼

    Returns:
        pd.DataFrame
    """
    if close_col not in df.columns:
        raise ValueError(f"'{close_col}' 칼럼이 없습니다.")

    df[signal_col] = 0
    rolling_max = df[close_col].rolling(window=window, min_periods=window).max()
    rolling_min = df[close_col].rolling(window=window, min_periods=window).min()

    buy_condition = df[close_col] >= (rolling_min * (1 + x_pct))
    sell_condition = df[close_col] <= (rolling_max * (1 - y_pct))

    df.loc[buy_condition, signal_col] = 1
    df.loc[sell_condition, signal_col] = -1

    return df


def support_resistance_signal(
    df: pd.DataFrame,
    rolling_min_col: str,   # 예: "sr_min_20"
    rolling_max_col: str,   # 예: "sr_max_20"
    price_col: str = "close",
    band_pct: float = 0.0,
    signal_col: str = "signal_sr"
) -> pd.DataFrame:
    """
    Support/Resistance: price가 rolling_max_col*(1+band_pct) 상향 돌파 -> 매수,
                        rolling_min_col*(1-band_pct) 하향 돌파 -> 매도

    Args:
        df (pd.DataFrame)
        rolling_min_col (str): 지지선 컬럼 (ex. "sr_min_20")
        rolling_max_col (str): 저항선 컬럼 (ex. "sr_max_20")
        price_col (str): 가격 칼럼
        band_pct (float): 밴드 여유
        signal_col (str): 결과 신호 칼럼

    Returns:
        pd.DataFrame
    """
    if rolling_min_col not in df.columns or rolling_max_col not in df.columns:
        raise ValueError(f"지지/저항 컬럼이 존재하지 않습니다: {rolling_min_col}, {rolling_max_col}")

    df[signal_col] = 0
    buy_condition = df[price_col] > df[rolling_max_col] * (1 + band_pct)
    sell_condition = df[price_col] < df[rolling_min_col] * (1 - band_pct)

    df.loc[buy_condition, signal_col] = 1
    df.loc[sell_condition, signal_col] = -1
    return df


def channel_breakout_signal(
    df: pd.DataFrame,
    rolling_min_col: str,   # 예: "ch_min_20"
    rolling_max_col: str,   # 예: "ch_max_20"
    price_col: str = "close",
    breakout_pct: float = 0.0,
    signal_col: str = "signal_cb"
) -> pd.DataFrame:
    """
    채널 폭을 (rolling_max_col - rolling_min_col)로 보고,
    breakout_pct 만큼 상단 돌파 시 매수, 하단 돌파 시 매도

    Args:
        df (pd.DataFrame)
        rolling_min_col (str): 채널 하단 (ex. "ch_min_20")
        rolling_max_col (str): 채널 상단 (ex. "ch_max_20")
        price_col (str): 가격 칼럼
        breakout_pct (float): 채널 폭 대비 몇 %를 벗어나면 돌파로 보는지
        signal_col (str): 결과 신호 칼럼

    Returns:
        pd.DataFrame
    """
    if rolling_min_col not in df.columns or rolling_max_col not in df.columns:
        raise ValueError(f"채널 컬럼이 존재하지 않습니다: {rolling_min_col}, {rolling_max_col}")

    df[signal_col] = 0
    width = df[rolling_max_col] - df[rolling_min_col]
    upper_threshold = df[rolling_max_col] + (width * breakout_pct)
    lower_threshold = df[rolling_min_col] - (width * breakout_pct)

    df.loc[df[price_col] > upper_threshold, signal_col] = 1
    df.loc[df[price_col] < lower_threshold, signal_col] = -1
    return df


def combine_signals(
    df: pd.DataFrame,
    signal_cols: list,
    out_col: str = "signal_combined"
) -> pd.DataFrame:
    """
    주어진 여러 시그널 칼럼을 합산하여 최종 매매 시그널을 만든다.
    합이 양수면 1, 음수면 -1, 나머지는 0.

    Args:
        df (pd.DataFrame): 시그널 칼럼들이 들어있는 데이터프레임
        signal_cols (list): 합칠 칼럼명 리스트
        out_col (str, optional): 결과 시그널 컬럼명

    Returns:
        pd.DataFrame
    """
    df[out_col] = 0
    sum_series = df[signal_cols].sum(axis=1)

    df.loc[sum_series > 0, out_col] = 1
    df.loc[sum_series < 0, out_col] = -1
    # sum_series == 0 이면 관망(0)
    return df