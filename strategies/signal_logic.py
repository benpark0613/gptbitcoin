# gptbitcoin/strategies/signal_logic.py
"""
(추세추종) 시그널 로직을 모아둔 모듈.

구글 스타일 Docstring, 필요한 최소한의 한글 주석만 추가.
"""

import pandas as pd
import numpy as np

pd.set_option('future.no_silent_downcasting', True)


def ma_crossover_signal(
    df: pd.DataFrame,
    short_ma_col: str,
    long_ma_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    이동평균 교차(MA) 시그널 (즉시모드 추세추종).
    - 단기 MA > 장기 MA이면 +1, 단기 MA < 장기 MA이면 -1, 같으면 0.

    Args:
        df (pd.DataFrame): 시계열 데이터
        short_ma_col (str): 단기 MA 칼럼
        long_ma_col (str): 장기 MA 칼럼
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    short_arr = df[short_ma_col].values
    long_arr = df[long_ma_col].values
    n = len(df)

    for i in range(n):
        if pd.isna(short_arr[i]) or pd.isna(long_arr[i]):
            df.loc[df.index[i], signal_col] = 0
        elif short_arr[i] > long_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif short_arr[i] < long_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

    return df


def obv_ma_signal(
    df: pd.DataFrame,
    obv_short_col: str,
    obv_long_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    OBV 단기/장기 이동평균 비교 시그널 (추세추종).
    이미 즉시모드로 작성되어 있으므로 변경 없음.
    """
    df[signal_col] = 0
    short_arr = df[obv_short_col].values
    long_arr = df[obv_long_col].values
    n = len(df)

    for i in range(n):
        if short_arr[i] > long_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif short_arr[i] < long_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

    return df


def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,
    lower_bound: float,
    upper_bound: float,
    signal_col: str
) -> pd.DataFrame:
    """
    RSI 지표 시그널 (즉시모드 추세추종).
    - RSI > upper_bound면 +1, RSI < lower_bound면 -1, 그 외 0.

    Args:
        df (pd.DataFrame)
        rsi_col (str)
        lower_bound (float)
        upper_bound (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    arr = df[rsi_col].values
    n = len(df)

    for i in range(n):
        if pd.isna(arr[i]):
            df.loc[df.index[i], signal_col] = 0
        elif arr[i] > upper_bound:
            df.loc[df.index[i], signal_col] = 1
        elif arr[i] < lower_bound:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

    return df


def macd_signal(
    df: pd.DataFrame,
    macd_line_col: str,
    macd_signal_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    MACD 시그널 (즉시모드 추세추종).
    - MACD 라인 > 시그널 라인이면 +1, 작으면 -1, 같으면 0.

    Args:
        df (pd.DataFrame)
        macd_line_col (str)
        macd_signal_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    if macd_line_col not in df.columns or macd_signal_col not in df.columns:
        return df

    macd_arr = df[macd_line_col].values
    sig_arr = df[macd_signal_col].values
    n = len(df)

    for i in range(n):
        if pd.isna(macd_arr[i]) or pd.isna(sig_arr[i]):
            df.loc[df.index[i], signal_col] = 0
        elif macd_arr[i] > sig_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif macd_arr[i] < sig_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

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
    - ADX >= 임계값이면 +DI와 -DI 비교하여 +1/-1,
      ADX < 임계값이면 0.

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
    df[signal_col] = 0
    for c in [plus_di_col, minus_di_col, adx_col]:
        if c not in df.columns:
            return df

    plus_arr = df[plus_di_col].values
    minus_arr = df[minus_di_col].values
    adx_arr = df[adx_col].values
    n = len(df)

    for i in range(n):
        if (pd.isna(plus_arr[i]) or pd.isna(minus_arr[i]) or pd.isna(adx_arr[i])):
            df.loc[df.index[i], signal_col] = 0
        elif adx_arr[i] < adx_threshold:
            df.loc[df.index[i], signal_col] = 0
        else:
            # adx가 임계값 이상인 경우 +DI / -DI 비교
            if plus_arr[i] > minus_arr[i]:
                df.loc[df.index[i], signal_col] = 1
            elif plus_arr[i] < minus_arr[i]:
                df.loc[df.index[i], signal_col] = -1
            else:
                df.loc[df.index[i], signal_col] = 0

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
    - 가격 > upper 밴드면 +1, 가격 < lower 밴드면 -1, 그 외 0.

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
    df[signal_col] = 0
    price_arr = df[price_col].values
    up_arr = df[upper_col].values
    lo_arr = df[lower_col].values
    n = len(df)

    for i in range(n):
        if pd.isna(price_arr[i]) or pd.isna(up_arr[i]) or pd.isna(lo_arr[i]):
            df.loc[df.index[i], signal_col] = 0
        elif price_arr[i] > up_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif price_arr[i] < lo_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

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
    일목균형표 시그널 (즉시모드 추세추종).
    - 전환선>기준선 & 종가>구름 상단 → +1
      전환선<기준선 & 종가<구름 하단 → -1
      그 외는 0.

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
    for c in [tenkan_col, kijun_col, span_a_col, span_b_col, price_col]:
        if c not in df.columns:
            df[signal_col] = 0
            return df

    df[signal_col] = 0
    ten_arr = df[tenkan_col].values
    kij_arr = df[kijun_col].values
    span_a_arr = df[span_a_col].values
    span_b_arr = df[span_b_col].values
    price_arr = df[price_col].values

    cloud_top = np.maximum(span_a_arr, span_b_arr)
    cloud_bot = np.minimum(span_a_arr, span_b_arr)
    n = len(df)

    for i in range(n):
        if any(pd.isna([ten_arr[i], kij_arr[i], cloud_top[i], cloud_bot[i], price_arr[i]])):
            df.loc[df.index[i], signal_col] = 0
        else:
            if (ten_arr[i] > kij_arr[i]) and (price_arr[i] > cloud_top[i]):
                df.loc[df.index[i], signal_col] = 1
            elif (ten_arr[i] < kij_arr[i]) and (price_arr[i] < cloud_bot[i]):
                df.loc[df.index[i], signal_col] = -1
            else:
                df.loc[df.index[i], signal_col] = 0

    return df


def psar_signal(
    df: pd.DataFrame,
    psar_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    파라볼릭 SAR(PSAR) 시그널 (즉시모드 추세추종).
    - PSAR < 종가면 +1, PSAR > 종가면 -1, 그 외 0.

    Args:
        df (pd.DataFrame)
        psar_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    psar_arr = df[psar_col].values
    price_arr = df[price_col].values
    n = len(df)

    for i in range(n):
        if pd.isna(psar_arr[i]) or pd.isna(price_arr[i]):
            df.loc[df.index[i], signal_col] = 0
        elif psar_arr[i] < price_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif psar_arr[i] > price_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

    return df


def supertrend_signal(
    df: pd.DataFrame,
    st_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    슈퍼트렌드 시그널 (즉시모드 추세추종).
    - 가격 > 슈퍼트렌드면 +1, 가격 < 슈퍼트렌드면 -1, 같으면 0.

    Args:
        df (pd.DataFrame)
        st_col (str)
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    st_arr = df[st_col].values
    price_arr = df[price_col].values
    n = len(df)

    for i in range(n):
        if pd.isna(st_arr[i]) or pd.isna(price_arr[i]):
            df.loc[df.index[i], signal_col] = 0
        elif price_arr[i] > st_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif price_arr[i] < st_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

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
    이미 즉시모드로 작성되어 있으므로 변경 없음.
    """
    df[signal_col] = 0
    price_arr = df[price_col].values
    low_arr = df[lower_col].values
    up_arr = df[upper_col].values
    n = len(df)

    for i in range(n):
        if price_arr[i] > up_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        elif price_arr[i] < low_arr[i]:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

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
    이미 즉시모드로 작성되어 있으므로 변경 없음.
    """
    df[signal_col] = 0
    k_arr = df[stoch_k_col].values
    d_arr = df[stoch_d_col].values
    n = len(df)

    for i in range(n):
        if k_arr[i] >= upper_threshold and d_arr[i] >= upper_threshold:
            df.loc[df.index[i], signal_col] = 1
        elif k_arr[i] <= lower_threshold and d_arr[i] <= lower_threshold:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

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
    이미 즉시모드로 작성되어 있으므로 변경 없음.
    """
    df[signal_col] = 0
    k_arr = df[k_col].values
    d_arr = df[d_col].values
    n = len(df)

    for i in range(n):
        if k_arr[i] >= upper_threshold and d_arr[i] >= upper_threshold:
            df.loc[df.index[i], signal_col] = 1
        elif k_arr[i] <= lower_threshold and d_arr[i] <= lower_threshold:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

    return df


def mfi_signal(
    df: pd.DataFrame,
    mfi_col: str,
    lower_threshold: float,
    upper_threshold: float,
    signal_col: str
) -> pd.DataFrame:
    """
    MFI(Money Flow Index) 시그널 (추세추종).
    이미 즉시모드로 작성되어 있으므로 변경 없음.
    """
    df[signal_col] = 0
    mfi_arr = df[mfi_col].values
    n = len(df)

    for i in range(n):
        if mfi_arr[i] >= upper_threshold:
            df.loc[df.index[i], signal_col] = 1
        elif mfi_arr[i] <= lower_threshold:
            df.loc[df.index[i], signal_col] = -1
        else:
            df.loc[df.index[i], signal_col] = 0

    return df


def vwap_signal(
    df: pd.DataFrame,
    vwap_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    VWAP 시그널 (추세추종).
    이미 즉시모드로 작성되어 있으므로 변경 없음.
    """
    df[signal_col] = 0
    vw_arr = df[vwap_col].values
    pr_arr = df[price_col].values
    n = len(df)

    for i in range(n):
        if pr_arr[i] > vw_arr[i]:
            df.loc[df.index[i], signal_col] = 1
        else:
            df.loc[df.index[i], signal_col] = -1

    return df
