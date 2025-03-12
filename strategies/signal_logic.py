# gptbitcoin/strategies/signal_logic.py
# signal_logic.py
# 정교하게 계산하도록 일부 로직을 변경 (RSI, MACD, DMI/ADX 등)
# 주석은 최소한 한글로, Docstring은 구글 스타일로 작성.

from typing import List
import pandas as pd
import numpy as np

pd.set_option('future.no_silent_downcasting', True)

def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,
    long_ma_col: str,
    band_filter: float = 0.0,
    signal_col: str = "ma_sig"
) -> pd.DataFrame:
    """
    이동평균 교차 시그널. (기존과 동일)
    band_filter=0.05 → 5%로 해석.

    Args:
        df (pd.DataFrame): 이동평균 칼럼이 들어있는 DataFrame
        short_ma_col (str): 단기 이동평균 칼럼명
        long_ma_col (str): 장기 이동평균 칼럼명
        band_filter (float): 퍼센트(0.05→5%)
        signal_col (str): 결과 시그널 칼럼

    Returns:
        pd.DataFrame: signal_col에 +1/-1/0 시그널이 기록된 DataFrame
    """
    df[signal_col] = 0
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
    RSI 시그널(일반적 해석, 연속 상태).
    - RSI < lower_bound → 매수(+1)
    - RSI > upper_bound → 매도(-1)
    - 그 외 → 0

    Args:
        df (pd.DataFrame): 시계열 데이터프레임
        rsi_col (str): RSI 칼럼명
        lower_bound (float): RSI 하단 임계값(예:30)
        upper_bound (float): RSI 상단 임계값(예:70)
        signal_col (str): 결과 시그널 칼럼명

    Returns:
        pd.DataFrame: signal_col에 +1/-1/0 시그널을 기록한 DataFrame
    """
    df[signal_col] = 0
    buy_cond = df[rsi_col] < lower_bound
    sell_cond = df[rsi_col] > upper_bound

    df.loc[buy_cond, signal_col] = 1
    df.loc[sell_cond, signal_col] = -1
    return df


def obv_signal(
    df: pd.DataFrame,
    obv_col: str = "obv_raw",
    threshold: float = 0.0,
    signal_col: str = "obv_sig"
) -> pd.DataFrame:
    """
    OBV 시그널.
    threshold=0.05 → 5%로 해석 시,
    obv > 0.05 → +1, obv < -0.05 → -1, 그 외 0.

    Args:
        df (pd.DataFrame)
        obv_col (str): OBV 칼럼명
        threshold (float): 기준값(퍼센트로 해석)
        signal_col (str): 결과 시그널 칼럼

    Returns:
        pd.DataFrame
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
    Filter 룰 시그널.
    x_pct=0.05 → +5%, y_pct=0.1 → +10%로 해석.

    Args:
        df (pd.DataFrame)
        close_col (str)
        window (int)
        x_pct (float)
        y_pct (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    max_col = f"filter_max_{window}"
    min_col = f"filter_min_{window}"

    up_cond = df[close_col] > df[max_col] * (1.0 + x_pct)
    down_cond = df[close_col] < df[min_col] * (1.0 - y_pct)

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
    지지/저항 시그널.
    band_pct=0.05 → 5%

    Args:
        df (pd.DataFrame)
        rolling_min_col (str): rolling 최소 칼럼
        rolling_max_col (str): rolling 최대 칼럼
        price_col (str): 가격 칼럼
        band_pct (float): 퍼센트 필터
        signal_col (str): 결과 시그널 칼럼

    Returns:
        pd.DataFrame
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
    채널 돌파 시그널.
    breakout_pct=0.03 → 3%

    Args:
        df (pd.DataFrame)
        rolling_min_col (str)
        rolling_max_col (str)
        price_col (str)
        breakout_pct (float)
        signal_col (str)

    Returns:
        pd.DataFrame
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
    MACD 시그널(일반적 해석, 연속 상태).
    - MACD 라인이 Signal 라인보다 크거나 같으면 매수(+1)
    - 작으면 매도(-1)

    Args:
        df (pd.DataFrame): MACD/Signal 라인이 들어있는 DataFrame
        macd_line_col (str): MACD 라인 칼럼
        macd_signal_col (str): Signal 라인 칼럼
        signal_col (str): 결과 시그널 칼럼

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    if macd_line_col not in df.columns or macd_signal_col not in df.columns:
        return df

    buy_cond = df[macd_line_col] >= df[macd_signal_col]
    df.loc[buy_cond, signal_col] = 1
    df.loc[~buy_cond, signal_col] = -1
    return df


def dmi_adx_signal_trend(
    df: pd.DataFrame,
    plus_di_col: str,
    minus_di_col: str,
    adx_col: str,
    adx_threshold: float = 25.0,
    signal_col: str = "dmi_sig"
) -> pd.DataFrame:
    """
    DMI(+DI, -DI) & ADX 시그널(일반적인 추세지표 해석).
    - ADX >= adx_threshold 일 때만 추세가 유효
      +DI >= -DI → 매수(+1)
      +DI <  -DI → 매도(-1)
    - ADX < adx_threshold → 0 (추세 미약)

    Args:
        df (pd.DataFrame)
        plus_di_col (str): +DI 칼럼
        minus_di_col (str): -DI 칼럼
        adx_col (str): ADX 칼럼
        adx_threshold (float): 추세 강도 임계값
        signal_col (str): 결과 시그널 칼럼

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    needed_cols = [plus_di_col, minus_di_col, adx_col]
    for c in needed_cols:
        if c not in df.columns:
            return df

    adx_valid = df[adx_col] >= adx_threshold
    buy_cond = (df[plus_di_col] >= df[minus_di_col]) & adx_valid
    sell_cond = (df[plus_di_col] < df[minus_di_col]) & adx_valid

    df.loc[buy_cond, signal_col] = 1
    df.loc[sell_cond, signal_col] = -1
    # ADX 미만이면 0 유지
    return df


def bollinger_signal(
    df: pd.DataFrame,
    mid_col: str,
    upper_col: str,
    lower_col: str,
    price_col: str = "close",
    signal_col: str = "boll_sig",
    width_threshold: float = 0.05
) -> pd.DataFrame:
    """
    볼린저 밴드 스퀴즈 탈출 기반 추세 시그널.
    (기존 로직 동일)

    Args:
        df (pd.DataFrame)
        mid_col (str)
        upper_col (str)
        lower_col (str)
        price_col (str)
        signal_col (str)
        width_threshold (float): 밴드 폭 스퀴즈 기준

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    bw_col = "_band_width"
    df[bw_col] = (df[upper_col] - df[lower_col]) / df[mid_col].abs().replace(0, np.nan)

    sq_col = "_is_squeeze"
    df[sq_col] = df[bw_col] < width_threshold

    prev_sq_col = "_prev_squeeze"
    df[prev_sq_col] = df[sq_col].shift(1).fillna(False)

    up_break = df[price_col] > df[upper_col]
    down_break = df[price_col] < df[lower_col]

    cond_up = df[prev_sq_col] & up_break
    cond_down = df[prev_sq_col] & down_break

    df.loc[cond_up, signal_col] = 1
    df.loc[cond_down, signal_col] = -1

    df.drop(columns=[bw_col, sq_col, prev_sq_col], inplace=True)
    return df


def ichimoku_signal_trend(
    df: pd.DataFrame,
    tenkan_col: str,
    kijun_col: str,
    span_a_col: str,
    span_b_col: str,
    price_col: str = "close",
    signal_col: str = "ich_sig"
) -> pd.DataFrame:
    """
    일목균형표 시그널(기존 로직 유지).

    Args:
        df (pd.DataFrame)
        tenkan_col (str)
        kijun_col (str)
        span_a_col (str)
        span_b_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    needed = [tenkan_col, kijun_col, span_a_col, span_b_col, price_col]
    if any(c not in df.columns for c in needed):
        df[signal_col] = 0
        return df

    ten_arr = df[tenkan_col].to_numpy(float)
    kij_arr = df[kijun_col].to_numpy(float)
    diff_arr = ten_arr - kij_arr

    span_a_arr = df[span_a_col].to_numpy(float)
    span_b_arr = df[span_b_col].to_numpy(float)
    price_arr = df[price_col].to_numpy(float)

    cloud_top = np.maximum(span_a_arr, span_b_arr)
    cloud_bot = np.minimum(span_a_arr, span_b_arr)

    n = len(df)
    sig_array = np.zeros(n, dtype=int)

    for i in range(1, n):
        prev_diff = diff_arr[i - 1]
        curr_diff = diff_arr[i]
        sig_array[i] = sig_array[i - 1]

        cross_up = (prev_diff <= 0) and (curr_diff > 0)
        cross_down = (prev_diff >= 0) and (curr_diff < 0)

        if cross_up:
            # 가격이 구름 위면 +1
            if price_arr[i] > cloud_top[i]:
                sig_array[i] = 1
        elif cross_down:
            # 가격이 구름 아래면 -1
            if price_arr[i] < cloud_bot[i]:
                sig_array[i] = -1

    df[signal_col] = sig_array
    return df


def psar_signal(
    df: pd.DataFrame,
    psar_col: str,
    price_col: str = "close",
    signal_col: str = "psar_sig"
) -> pd.DataFrame:
    """
    PSAR 시그널(단순판단).
    psar < price 이면 +1, 아니면 -1

    Args:
        df (pd.DataFrame)
        psar_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
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
    SuperTrend 시그널.
    price > supertrend 이면 +1, price < supertrend 이면 -1

    Args:
        df (pd.DataFrame)
        st_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    up_cond = df[price_col] > df[st_col]
    df.loc[up_cond, signal_col] = 1
    df.loc[~up_cond, signal_col] = -1
    return df


def fibo_signal_trend(
    df: pd.DataFrame,
    fibo_cols: List[str],
    price_col: str = "close",
    signal_col: str = "fibo_sig"
) -> pd.DataFrame:
    """
    피보나치 레벨 돌파 시그널(기존 로직 유지).

    Args:
        df (pd.DataFrame)
        fibo_cols (List[str]): 예) ["fibo_0.236", "fibo_0.382", ...]
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    if price_col not in df.columns:
        df[signal_col] = 0
        return df

    valid_fibo_cols = [c for c in fibo_cols if c in df.columns]
    if not valid_fibo_cols:
        df[signal_col] = 0
        return df

    price_arr = df[price_col].to_numpy(dtype=float)
    fibo_arrays = [df[c].to_numpy(dtype=float) for c in valid_fibo_cols]

    n = len(df)
    rung_arr = np.zeros(n, dtype=int)

    # 몇 개 레벨을 돌파했는지 count
    for i in range(n):
        p = price_arr[i]
        count = 0
        for fiboA in fibo_arrays:
            if p >= fiboA[i]:
                count += 1
            else:
                break
        rung_arr[i] = count

    # 올라갔으면 +1, 내려갔으면 -1, 유지면 이전값
    sig_array = np.zeros(n, dtype=int)
    for i in range(1, n):
        prev_rung = rung_arr[i - 1]
        curr_rung = rung_arr[i]

        if curr_rung > prev_rung:
            sig_array[i] = 1
        elif curr_rung < prev_rung:
            sig_array[i] = -1
        else:
            sig_array[i] = sig_array[i - 1]

    df[signal_col] = sig_array
    return df
