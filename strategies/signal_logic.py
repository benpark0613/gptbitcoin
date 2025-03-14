# gptbitcoin/strategies/signal_logic.py
"""
구글 스타일 Docstring, 필요한 최소한의 한글 주석만 추가.
"""

import numpy as np
import pandas as pd


def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,
    long_ma_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    MA 교차 시그널 (즉시모드 추세추종).
    - 단기 MA > 장기 MA이면 +1, 단기 MA < 장기 MA이면 -1, NaN은 0 처리.

    Args:
        df (pd.DataFrame): 시계열 데이터
        short_ma_col (str): 단기 MA 칼럼
        long_ma_col (str): 장기 MA 칼럼
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame
    """
    short_arr = df[short_ma_col].values
    long_arr = df[long_ma_col].values
    n = len(df)

    mask_nan = np.isnan(short_arr) | np.isnan(long_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (short_arr > long_arr)] = 1
    signals[~mask_nan & (short_arr < long_arr)] = -1

    df[signal_col] = signals
    return df


def obv_ma_signal(
    df: pd.DataFrame,
    obv_short_col: str,
    obv_long_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    OBV 단기/장기 이동평균 비교 시그널 (추세추종).

    Args:
        df (pd.DataFrame)
        obv_short_col (str)
        obv_long_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    short_arr = df[obv_short_col].values
    long_arr = df[obv_long_col].values
    n = len(df)

    mask_nan = np.isnan(short_arr) | np.isnan(long_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (short_arr > long_arr)] = 1
    signals[~mask_nan & (short_arr < long_arr)] = -1

    df[signal_col] = signals
    return df


def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,
    lower_bound: float,
    upper_bound: float,
    signal_col: str
) -> pd.DataFrame:
    """
    RSI 시그널 (즉시모드 추세추종).
    - RSI > upper_bound → +1, RSI < lower_bound → -1, 나머지 0.

    Args:
        df (pd.DataFrame)
        rsi_col (str)
        lower_bound (float)
        upper_bound (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    arr = df[rsi_col].values
    n = len(df)

    mask_nan = np.isnan(arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (arr > upper_bound)] = 1
    signals[~mask_nan & (arr < lower_bound)] = -1

    df[signal_col] = signals
    return df


def macd_signal(
    df: pd.DataFrame,
    macd_line_col: str,
    macd_signal_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    MACD 시그널 (즉시모드 추세추종).
    - MACD 라인 > 시그널 라인이면 +1, 작으면 -1, NaN은 0.

    Args:
        df (pd.DataFrame)
        macd_line_col (str)
        macd_signal_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    macd_arr = df[macd_line_col].values
    sig_arr = df[macd_signal_col].values
    n = len(df)

    mask_nan = np.isnan(macd_arr) | np.isnan(sig_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (macd_arr > sig_arr)] = 1
    signals[~mask_nan & (macd_arr < sig_arr)] = -1

    df[signal_col] = signals
    return df


def dmi_adx_signal_trend(
    df: pd.DataFrame,
    plus_di_col: str,
    minus_di_col: str,
    adx_col: str,
    adx_threshold: float,
    signal_col: str
) -> pd.DataFrame:
    """
    DMI(+DI/-DI) & ADX 시그널 (즉시모드 추세추종).
    - ADX >= adx_threshold 시 +DI/-DI 비교 (+1/-1), 그 외 0.

    Args:
        df (pd.DataFrame)
        plus_di_col (str)
        minus_di_col (str)
        adx_col (str)
        adx_threshold (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    plus_arr = df[plus_di_col].values
    minus_arr = df[minus_di_col].values
    adx_arr = df[adx_col].values
    n = len(df)

    mask_nan = (np.isnan(plus_arr) | np.isnan(minus_arr) | np.isnan(adx_arr))
    mask_low_adx = adx_arr < adx_threshold
    signals = np.zeros(n, dtype=int)

    # adx >= threshold
    valid_mask = (~mask_nan) & (~mask_low_adx)
    signals[valid_mask & (plus_arr > minus_arr)] = 1
    signals[valid_mask & (plus_arr < minus_arr)] = -1

    df[signal_col] = signals
    return df


def bollinger_signal(
    df: pd.DataFrame,
    mid_col: str,
    upper_col: str,
    lower_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    볼린저 밴드 시그널 (즉시모드 추세추종).
    - 가격 > 상단밴드 → +1, 가격 < 하단밴드 → -1.

    Args:
        df (pd.DataFrame)
        mid_col (str)
        upper_col (str)
        lower_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    price_arr = df[price_col].values
    up_arr = df[upper_col].values
    lo_arr = df[lower_col].values
    n = len(df)

    mask_nan = np.isnan(price_arr) | np.isnan(up_arr) | np.isnan(lo_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (price_arr > up_arr)] = 1
    signals[~mask_nan & (price_arr < lo_arr)] = -1

    df[signal_col] = signals
    return df


def ichimoku_signal_trend(
    df: pd.DataFrame,
    tenkan_col: str,
    kijun_col: str,
    span_a_col: str,
    span_b_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    일목균형표 (전환선/기준선/구름) 시그널 (즉시추종).
    - 전환>기준 & 종가>구름상단 → +1, 전환<기준 & 종가<구름하단 → -1

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
    ten_arr = df[tenkan_col].values
    kij_arr = df[kijun_col].values
    span_a_arr = df[span_a_col].values
    span_b_arr = df[span_b_col].values
    price_arr = df[price_col].values
    n = len(df)

    mask_nan = np.isnan(ten_arr) | np.isnan(kij_arr) | np.isnan(span_a_arr) \
               | np.isnan(span_b_arr) | np.isnan(price_arr)

    cloud_top = np.maximum(span_a_arr, span_b_arr)
    cloud_bot = np.minimum(span_a_arr, span_b_arr)

    signals = np.zeros(n, dtype=int)

    cond_long = (ten_arr > kij_arr) & (price_arr > cloud_top) & (~mask_nan)
    cond_short = (ten_arr < kij_arr) & (price_arr < cloud_bot) & (~mask_nan)

    signals[cond_long] = 1
    signals[cond_short] = -1

    df[signal_col] = signals
    return df


def psar_signal(
    df: pd.DataFrame,
    psar_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    파라볼릭 SAR 시그널 (즉시모드 추세추종).
    - PSAR < 종가 → +1, PSAR > 종가 → -1

    Args:
        df (pd.DataFrame)
        psar_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    psar_arr = df[psar_col].values
    price_arr = df[price_col].values
    n = len(df)

    mask_nan = np.isnan(psar_arr) | np.isnan(price_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (psar_arr < price_arr)] = 1
    signals[~mask_nan & (psar_arr > price_arr)] = -1

    df[signal_col] = signals
    return df


def supertrend_signal(
    df: pd.DataFrame,
    st_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    슈퍼트렌드 시그널 (즉시모드 추세추종).
    - 가격 > supertrend → +1, 가격 < supertrend → -1

    Args:
        df (pd.DataFrame)
        st_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    st_arr = df[st_col].values
    price_arr = df[price_col].values
    n = len(df)

    mask_nan = np.isnan(st_arr) | np.isnan(price_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (price_arr > st_arr)] = 1
    signals[~mask_nan & (price_arr < st_arr)] = -1

    df[signal_col] = signals
    return df


def donchian_signal(
    df: pd.DataFrame,
    lower_col: str,
    upper_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    돈채널(Donchian) 시그널 (추세추종).
    - 가격 > upper → +1, 가격 < lower → -1

    Args:
        df (pd.DataFrame)
        lower_col (str)
        upper_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    price_arr = df[price_col].values
    low_arr = df[lower_col].values
    up_arr = df[upper_col].values
    n = len(df)

    mask_nan = np.isnan(price_arr) | np.isnan(low_arr) | np.isnan(up_arr)
    signals = np.zeros(n, dtype=int)

    signals[~mask_nan & (price_arr > up_arr)] = 1
    signals[~mask_nan & (price_arr < low_arr)] = -1

    df[signal_col] = signals
    return df


def stoch_signal(
    df: pd.DataFrame,
    stoch_k_col: str,
    stoch_d_col: str,
    lower_threshold: float,
    upper_threshold: float,
    signal_col: str
) -> pd.DataFrame:
    """
    스토캐스틱 시그널 (추세추종).
    - K & D > upper_threshold → +1, K & D < lower_threshold → -1

    Args:
        df (pd.DataFrame)
        stoch_k_col (str)
        stoch_d_col (str)
        lower_threshold (float)
        upper_threshold (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    k_arr = df[stoch_k_col].values
    d_arr = df[stoch_d_col].values
    n = len(df)

    mask_nan = np.isnan(k_arr) | np.isnan(d_arr)
    signals = np.zeros(n, dtype=int)

    cond_long = (k_arr >= upper_threshold) & (d_arr >= upper_threshold) & (~mask_nan)
    cond_short = (k_arr <= lower_threshold) & (d_arr <= lower_threshold) & (~mask_nan)

    signals[cond_long] = 1
    signals[cond_short] = -1

    df[signal_col] = signals
    return df


def stoch_rsi_signal(
    df: pd.DataFrame,
    k_col: str,
    d_col: str,
    lower_threshold: float,
    upper_threshold: float,
    signal_col: str
) -> pd.DataFrame:
    """
    스토캐스틱 RSI 시그널 (추세추종).
    - K & D > upper_threshold → +1, K & D < lower_threshold → -1

    Args:
        df (pd.DataFrame)
        k_col (str)
        d_col (str)
        lower_threshold (float)
        upper_threshold (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    k_arr = df[k_col].values
    d_arr = df[d_col].values
    n = len(df)

    mask_nan = np.isnan(k_arr) | np.isnan(d_arr)
    signals = np.zeros(n, dtype=int)

    cond_long = (k_arr >= upper_threshold) & (d_arr >= upper_threshold) & (~mask_nan)
    cond_short = (k_arr <= lower_threshold) & (d_arr <= lower_threshold) & (~mask_nan)

    signals[cond_long] = 1
    signals[cond_short] = -1

    df[signal_col] = signals
    return df

def vwap_signal(
    df: pd.DataFrame,
    vwap_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    VWAP 시그널 (추세추종).
    - 가격 > VWAP → +1, 그 외 → -1

    Args:
        df (pd.DataFrame)
        vwap_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    vw_arr = df[vwap_col].values
    pr_arr = df[price_col].values
    n = len(df)

    mask_nan = np.isnan(vw_arr) | np.isnan(pr_arr)
    signals = np.full(n, -1, dtype=int)  # 디폴트 -1
    signals[~mask_nan & (pr_arr > vw_arr)] = 1

    df[signal_col] = signals
    return df
