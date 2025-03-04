# gptbitcoin/strategies/signal_logic.py
# 보조지표 기반 매매 시그널 로직 모듈
# 각 시그널을 단순합하여 +면 매수(1), -면 매도(-1), 그 외(0)는 유지/관망
# 주석은 필수적 최소만 한글로, docstring은 구글 스타일로 작성

import pandas as pd
import numpy as np

def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,
    long_ma_col: str,
    signal_col: str = "signal_ma"
) -> pd.DataFrame:
    """
    단기MA와 장기MA가 골든크로스/데드크로스를 이루는 구간에서
    매매 시그널(1, -1, 0)을 계산한다.

    Args:
        df (pd.DataFrame): MA 칼럼이 포함된 DataFrame
        short_ma_col (str): 단기MA 칼럼명
        long_ma_col (str): 장기MA 칼럼명
        signal_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame: 'signal_col' 칼럼에 1(매수), -1(매도), 0(유지)
    """
    if short_ma_col not in df.columns or long_ma_col not in df.columns:
        raise ValueError(f"MA 칼럼을 찾을 수 없습니다. '{short_ma_col}', '{long_ma_col}' 확인")

    df[signal_col] = 0
    df.loc[df[short_ma_col] > df[long_ma_col], signal_col] = 1   # 골든크로스 -> 매수
    df.loc[df[short_ma_col] < df[long_ma_col], signal_col] = -1  # 데드크로스 -> 매도

    return df


def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,
    lower_bound: float = 30.0,
    upper_bound: float = 70.0,
    signal_col: str = "signal_rsi"
) -> pd.DataFrame:
    """
    RSI 지표를 이용해 과매도/과매수 구간에서 매수/매도 시그널(1, -1, 0)을 계산.

    Args:
        df (pd.DataFrame): RSI 칼럼이 포함된 DataFrame
        rsi_col (str): RSI 값이 들어 있는 칼럼명
        lower_bound (float, optional): 과매도 기준
        upper_bound (float, optional): 과매수 기준
        signal_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame: 'signal_col' 칼럼에 1(매수), -1(매도), 0(유지)
    """
    if rsi_col not in df.columns:
        raise ValueError(f"RSI 칼럼 '{rsi_col}'을 찾을 수 없습니다.")

    df[signal_col] = 0
    df.loc[df[rsi_col] < lower_bound, signal_col] = 1
    df.loc[df[rsi_col] > upper_bound, signal_col] = -1

    return df


def obv_signal(
    df: pd.DataFrame,
    obv_col: str = "OBV",
    threshold: float = 0.0,
    signal_col: str = "signal_obv"
) -> pd.DataFrame:
    """
    OBV(On-Balance Volume)를 이용해 매수/매도 시그널(1, -1, 0)을 계산.
    예: OBV가 threshold보다 크면 매수, 작으면 매도, 그 외는 0.

    Args:
        df (pd.DataFrame): OBV 칼럼이 포함된 DataFrame
        obv_col (str, optional): OBV 칼럼명
        threshold (float, optional): 기준값
        signal_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame: 'signal_col' 칼럼에 1(매수), -1(매도), 0(유지)
    """
    if obv_col not in df.columns:
        raise ValueError(f"OBV 칼럼 '{obv_col}'을 찾을 수 없습니다.")

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
    필터룰(Filter Rule) 예시: 최근 window 기간 중 최저가 대비 x% 이상 상승시 매수,
    최고가 대비 y% 이상 하락시 매도, 그 외 0.

    Args:
        df (pd.DataFrame): OHLCV DataFrame
        close_col (str, optional): 종가 칼럼명
        window (int, optional): lookback window
        x_pct (float, optional): 상승 기준
        y_pct (float, optional): 하락 기준
        signal_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame
    """
    if close_col not in df.columns:
        raise ValueError(f"'{close_col}' 칼럼을 찾을 수 없습니다.")

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
    rolling_min_col: str,
    rolling_max_col: str,
    price_col: str = "close",
    band_pct: float = 0.0,
    signal_col: str = "signal_sr"
) -> pd.DataFrame:
    """
    Support/Resistance: price가 rolling_max_col보다 band_pct만큼 상향 돌파 시 매수,
    rolling_min_col보다 band_pct만큼 하향 돌파 시 매도.

    Args:
        df (pd.DataFrame): rolling_min_col, rolling_max_col이 포함된 DataFrame
        rolling_min_col (str): 저점(지지선) 칼럼명
        rolling_max_col (str): 고점(저항선) 칼럼명
        price_col (str, optional): 기준 가격 칼럼
        band_pct (float, optional): 여유 밴드
        signal_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame
    """
    if rolling_min_col not in df.columns or rolling_max_col not in df.columns:
        raise ValueError("SR용 rolling_min_col, rolling_max_col을 찾을 수 없습니다.")

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
    channel_width_col: str = None,
    breakout_pct: float = 0.0,
    signal_col: str = "signal_cb"
) -> pd.DataFrame:
    """
    채널 브레이크아웃: (max - min)를 채널 폭으로 보고, breakout_pct 비율만큼
    초과/미달 시 매수/매도.

    Args:
        df (pd.DataFrame): 채널 관련 칼럼(rolling_min_col, rolling_max_col 등)이 포함된 DF
        rolling_min_col (str): 채널 저점 칼럼
        rolling_max_col (str): 채널 고점 칼럼
        price_col (str, optional): 기준 가격
        channel_width_col (str, optional): 채널 폭 칼럼(없으면 자동 계산)
        breakout_pct (float, optional): 채널 폭 대비 몇 % 이상 돌파 시 매수/매도
        signal_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame
    """
    if rolling_min_col not in df.columns or rolling_max_col not in df.columns:
        raise ValueError("채널 브레이크아웃용 min/max 칼럼이 없습니다.")

    df[signal_col] = 0
    if channel_width_col and channel_width_col in df.columns:
        width = df[channel_width_col]
    else:
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
    여러 시그널 칼럼을 단순합하여 최종 매매 시그널 결정.
    합이 양수면 1(매수), 음수면 -1(매도), 0이면 0 유지.

    Args:
        df (pd.DataFrame): 여러 시그널 칼럼이 있는 DF
        signal_cols (list): 합산할 시그널 칼럼명 리스트
        out_col (str, optional): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame
    """
    df[out_col] = 0
    signals_sum = df[signal_cols].sum(axis=1)

    df.loc[signals_sum > 0, out_col] = 1
    df.loc[signals_sum < 0, out_col] = -1

    return df


if __name__ == "__main__":
    # 간단 테스트 코드 예시
    data = {
        "close": [100, 102, 101, 105, 110],
        "MA_5": [np.nan, np.nan, np.nan, np.nan, 103],
        "MA_20": [np.nan, np.nan, np.nan, np.nan, 108],
        "RSI_14": [np.nan, 35, 40, 75, 25],
        "OBV": [0, 1, 2, 1, 3],
    }
    df_test = pd.DataFrame(data)

    # MA 시그널
    df_test = ma_crossover_signal(df_test, short_ma_col="MA_5", long_ma_col="MA_20")
    print("MA 시그널:", df_test.get("signal_ma"))

    # RSI 시그널
    df_test = rsi_signal(df_test, rsi_col="RSI_14", lower_bound=30, upper_bound=70)
    print("RSI 시그널:", df_test.get("signal_rsi"))

    # OBV 시그널
    df_test = obv_signal(df_test, obv_col="OBV", threshold=1.0)
    print("OBV 시그널:", df_test.get("signal_obv"))

    # 시그널 합치기 (단순합)
    signal_cols_example = ["signal_ma", "signal_rsi", "signal_obv"]
    df_test = combine_signals(df_test, signal_cols_example, out_col="signal_final")
    print("최종 시그널:", df_test["signal_final"])
