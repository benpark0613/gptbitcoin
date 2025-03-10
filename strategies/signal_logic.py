# gptbitcoin/strategies/signal_logic.py
# 최소한의 한글 주석, 구글 스타일 docstring
# time_delays, holding_periods는 engine에서 처리하므로 여기서는
# band_filter 등 지표별 파라미터 위주로 시그널 로직을 완성한다.
# 논문 방식을 따라 MA의 band_filter를 절댓값이 아닌 퍼센트(비율) 차로 적용한다.

from typing import List

import pandas as pd


def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,
    long_ma_col: str,
    band_filter: float = 0.0,
    signal_col: str = "signal_ma"
) -> pd.DataFrame:
    """
    MA 크로스오버 신호를 생성한다.
    논문 방식에 따라, (단기 MA / 장기 MA - 1)의 비율이 ±band_filter를 초과하면 매매 신호가 발생한다.

    Args:
        df (pd.DataFrame): 단기/장기 MA 칼럼을 포함한 DataFrame
        short_ma_col (str): 단기 MA 칼럼명
        long_ma_col (str): 장기 MA 칼럼명
        band_filter (float, optional): MA 비율 차가 이 값보다 커야 매수, -band_filter보다 작아야 매도
                                       예: band_filter=0.01이면, 단기 MA가 장기 MA보다 1% 초과해야 매수
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col에 1/-1/0 신호가 기록된 DataFrame
    """
    if short_ma_col not in df.columns or long_ma_col not in df.columns:
        raise ValueError(f"MA 컬럼이 존재하지 않습니다: {short_ma_col}, {long_ma_col}")

    df[signal_col] = 0

    # 장기 MA가 0이 아닌 구간만 유효비율 계산
    valid_mask = (df[long_ma_col] != 0)
    ratio = (df[short_ma_col] / df[long_ma_col]) - 1.0  # 예: 0.01 => 1% 차이

    # 매수: 비율이 band_filter를 초과
    buy_mask = valid_mask & (ratio > band_filter)
    # 매도: 비율이 -band_filter 미만
    sell_mask = valid_mask & (ratio < -band_filter)

    df.loc[buy_mask, signal_col] = 1
    df.loc[sell_mask, signal_col] = -1

    return df


def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,
    lower_bound: float = 30.0,
    upper_bound: float = 70.0,
    signal_col: str = "signal_rsi"
) -> pd.DataFrame:
    """
    RSI 지표 기반 신호. RSI < lower_bound 시 매수, RSI > upper_bound 시 매도.

    Args:
        df (pd.DataFrame): RSI 칼럼이 포함된 DataFrame
        rsi_col (str): RSI 칼럼명
        lower_bound (float, optional): 과매도 기준
        upper_bound (float, optional): 과매수 기준
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col에 1/-1/0 신호가 기록된 DataFrame
    """
    if rsi_col not in df.columns:
        raise ValueError(f"RSI 칼럼이 존재하지 않습니다: {rsi_col}")

    df[signal_col] = 0
    df.loc[df[rsi_col] < lower_bound, signal_col] = 1
    df.loc[df[rsi_col] > upper_bound, signal_col] = -1

    return df


def obv_signal(
    df: pd.DataFrame,
    obv_col: str = "obv",
    threshold: float = 0.0,
    signal_col: str = "signal_obv"
) -> pd.DataFrame:
    """
    OBV가 threshold를 초과하면 매수, 미만이면 매도하는 단순 신호.

    Args:
        df (pd.DataFrame): OBV(또는 OBV SMA) 칼럼이 포함된 DataFrame
        obv_col (str, optional): OBV 관련 칼럼명 (기본 "obv")
        threshold (float, optional): 기준값 (ex. 0.0)
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col에 1/-1/0 신호가 기록된 DataFrame
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
    필터 룰 신호. 최근 window 최저가 대비 x_pct 상승 시 매수,
    최근 window 최고가 대비 y_pct 하락 시 매도.

    Args:
        df (pd.DataFrame): 종가 칼럼을 포함한 DataFrame
        close_col (str, optional): 종가 칼럼명
        window (int, optional): 롤링 윈도우
        x_pct (float, optional): 매수 기준 퍼센트 (0.05 => 5%)
        y_pct (float, optional): 매도 기준 퍼센트
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col에 1/-1/0 신호가 기록된 DataFrame
    """
    if close_col not in df.columns:
        raise ValueError(f"'{close_col}' 칼럼이 없습니다.")

    df[signal_col] = 0
    rolling_max = df[close_col].rolling(window=window, min_periods=window).max()
    rolling_min = df[close_col].rolling(window=window, min_periods=window).min()

    buy_condition = df[close_col] >= rolling_min * (1 + x_pct)
    sell_condition = df[close_col] <= rolling_max * (1 - y_pct)

    df.loc[buy_condition, signal_col] = 1
    df.loc[sell_condition, signal_col] = -1

    return df


def support_resistance_signal(
    df: pd.DataFrame,
    rolling_min_col: str,
    rolling_max_col: str,
    price_col: str = "close",
    band_pct: float = 0.0,
    signal_col: str = "signal_sr"
) -> pd.DataFrame:
    """
    지지·저항을 돌파할 때 신호 발생.
    price가 최대값*(1+band_pct) 초과 시 매수,
    최소값*(1-band_pct) 미만 시 매도.

    Args:
        df (pd.DataFrame): 지지·저항 칼럼이 포함된 DataFrame
        rolling_min_col (str): 구간 최솟값 칼럼명
        rolling_max_col (str): 구간 최댓값 칼럼명
        price_col (str, optional): 가격 칼럼명
        band_pct (float, optional): 돌파 여유 퍼센트
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col에 1/-1/0 신호가 기록된 DataFrame
    """
    if rolling_min_col not in df.columns or rolling_max_col not in df.columns:
        raise ValueError(f"지지/저항 컬럼이 존재하지 않음: {rolling_min_col}, {rolling_max_col}")

    df[signal_col] = 0
    buy_condition = df[price_col] > df[rolling_max_col] * (1 + band_pct)
    sell_condition = df[price_col] < df[rolling_min_col] * (1 - band_pct)

    df.loc[buy_condition, signal_col] = 1
    df.loc[sell_condition, signal_col] = -1

    return df


def channel_breakout_signal(
    df: pd.DataFrame,
    rolling_min_col: str,
    rolling_max_col: str,
    price_col: str = "close",
    breakout_pct: float = 0.0,
    signal_col: str = "signal_cb"
) -> pd.DataFrame:
    """
    채널 돌파 신호. (최대-최소) 폭에 breakout_pct를 곱해 상단/하단을 확장 후 돌파 시 매수/매도.

    Args:
        df (pd.DataFrame): 채널 최솟값/최댓값 칼럼이 포함된 DataFrame
        rolling_min_col (str): 채널 하단 칼럼
        rolling_max_col (str): 채널 상단 칼럼
        price_col (str, optional): 가격 칼럼명
        breakout_pct (float, optional): 채널 폭에 곱할 퍼센트
        signal_col (str, optional): 결과 신호 칼럼명

    Returns:
        pd.DataFrame: signal_col에 1/-1/0 신호가 기록된 DataFrame
    """
    if rolling_min_col not in df.columns or rolling_max_col not in df.columns:
        raise ValueError(f"채널 컬럼이 존재하지 않음: {rolling_min_col}, {rolling_max_col}")

    df[signal_col] = 0
    width = df[rolling_max_col] - df[rolling_min_col]
    upper_threshold = df[rolling_max_col] + (width * breakout_pct)
    lower_threshold = df[rolling_min_col] - (width * breakout_pct)

    df.loc[df[price_col] > upper_threshold, signal_col] = 1
    df.loc[df[price_col] < lower_threshold, signal_col] = -1

    return df


def combine_signals(
    df: pd.DataFrame,
    signal_cols: List[str],
    out_col: str = "signal_combined"
) -> pd.DataFrame:
    """
    여러 시그널 칼럼을 합산해 최종 신호를 만든다.
    합이 양수면 1, 음수면 -1, 아니면 0.

    Args:
        df (pd.DataFrame): 시그널 칼럼들(signal_cols)이 있는 DataFrame
        signal_cols (List[str]): 합산 대상 시그널 칼럼명 목록
        out_col (str, optional): 최종 신호 칼럼명

    Returns:
        pd.DataFrame: out_col에 1/-1/0 신호가 기록된 DataFrame
    """
    df[out_col] = 0
    sum_series = df[signal_cols].sum(axis=1)
    df.loc[sum_series > 0, out_col] = 1
    df.loc[sum_series < 0, out_col] = -1
    return df
