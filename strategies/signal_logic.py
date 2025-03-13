# gptbitcoin/strategies/signal_logic.py
# Docstring은 구글 스타일, 필요한 한글 주석만 추가
# "추세추종" 관점으로 로직 수정

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
    이동평균 교차(MA) 시그널 (추세추종).
    단기선이 장기선을 위로 돌파하면 매수, 아래로 돌파하면 매도.

    Args:
        df (pd.DataFrame): 시계열 데이터
        short_ma_col (str): 단기 MA 칼럼
        long_ma_col (str): 장기 MA 칼럼
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame: 시그널이 추가된 DataFrame
    """
    df[signal_col] = 0
    short_arr = df[short_ma_col].values
    long_arr = df[long_ma_col].values
    n = len(df)

    for i in range(1, n):
        # 이전 신호 유지
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        prev_diff = short_arr[i - 1] - long_arr[i - 1]
        curr_diff = short_arr[i] - long_arr[i]

        # 단기선이 장기선을 아래→위로 교차
        if prev_diff <= 0 and curr_diff > 0:
            df.loc[df.index[i], signal_col] = 1
        # 단기선이 장기선을 위→아래로 교차
        elif prev_diff >= 0 and curr_diff < 0:
            df.loc[df.index[i], signal_col] = -1

    return df


def rsi_signal(
    df: pd.DataFrame,
    rsi_col: str,
    lower_bound: float,
    upper_bound: float,
    signal_col: str
) -> pd.DataFrame:
    """
    RSI 지표 시그널 (추세추종).
    RSI가 상단 임계값(upper_bound)을 돌파하면 매수,
    하단 임계값(lower_bound)을 하향 돌파하면 매도.

    Args:
        df (pd.DataFrame): 시계열 데이터
        rsi_col (str): RSI 칼럼명
        lower_bound (float): 하단 임계값
        upper_bound (float): 상단 임계값
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame: 시그널이 추가된 DataFrame
    """
    df[signal_col] = 0
    arr = df[rsi_col].values
    n = len(df)

    for i in range(1, n):
        # 이전 신호 유지
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        # RSI가 upper_bound를 위로 돌파(추세 상방)
        if arr[i - 1] <= upper_bound and arr[i] > upper_bound:
            df.loc[df.index[i], signal_col] = 1
        # RSI가 lower_bound를 아래로 돌파(추세 하방)
        elif arr[i - 1] >= lower_bound and arr[i] < lower_bound:
            df.loc[df.index[i], signal_col] = -1

    return df


def obv_signal(
    df: pd.DataFrame,
    obv_col: str,
    threshold: float,
    signal_col: str
) -> pd.DataFrame:
    """
    OBV 지표 시그널 (추세추종 가정).
    OBV가 threshold 이상이면 매수, threshold 이하로 내려가면 매도.
    (threshold 양/음성 구간을 다르게 설정할 수도 있음)

    Args:
        df (pd.DataFrame): 시계열 데이터
        obv_col (str): OBV 칼럼
        threshold (float): 절대 임계값
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame: 시그널이 추가된 DataFrame
    """
    df[signal_col] = 0
    obv_arr = df[obv_col].values
    n = len(df)

    for i in range(n):
        if obv_arr[i] >= threshold:
            df.loc[df.index[i], signal_col] = 1
        else:
            # 굳이 아래쪽 별도 threshold를 두려면 추가 로직 작성
            df.loc[df.index[i], signal_col] = -1

    return df


def macd_signal(
    df: pd.DataFrame,
    macd_line_col: str,
    macd_signal_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    MACD 시그널 (추세추종).
    MACD 라인이 시그널 라인 위로 교차 시 매수, 아래로 교차 시 매도.

    Args:
        df (pd.DataFrame)
        macd_line_col (str): MACD 라인 칼럼
        macd_signal_col (str): 시그널 라인 칼럼
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    if macd_line_col not in df.columns or macd_signal_col not in df.columns:
        return df

    macd_arr = df[macd_line_col].values
    sig_arr = df[macd_signal_col].values
    n = len(df)

    for i in range(1, n):
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        prev_diff = macd_arr[i - 1] - sig_arr[i - 1]
        curr_diff = macd_arr[i] - sig_arr[i]

        if prev_diff <= 0 and curr_diff > 0:
            df.loc[df.index[i], signal_col] = 1
        elif prev_diff >= 0 and curr_diff < 0:
            df.loc[df.index[i], signal_col] = -1

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
    DMI(+DI/-DI) & ADX 시그널 (추세추종).
    ADX가 임계값 이상일 때 +DI가 -DI 위로 교차 시 매수, 아래로 교차 시 매도.

    Args:
        df (pd.DataFrame)
        plus_di_col (str): +DI 칼럼
        minus_di_col (str): -DI 칼럼
        adx_col (str): ADX 칼럼
        adx_threshold (float): ADX 최소 임계값
        signal_col (str): 결과 시그널 컬럼

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

    for i in range(1, n):
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        if adx_arr[i] < adx_threshold:
            df.loc[df.index[i], signal_col] = 0
            continue

        prev_diff = plus_arr[i - 1] - minus_arr[i - 1]
        curr_diff = plus_arr[i] - minus_arr[i]

        # +DI 위로 교차
        if prev_diff <= 0 and curr_diff > 0:
            df.loc[df.index[i], signal_col] = 1
        # -DI 위로 교차
        elif prev_diff >= 0 and curr_diff < 0:
            df.loc[df.index[i], signal_col] = -1

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
    볼린저 밴드 시그널 (추세추종).
    - 가격이 상단 밴드를 아래→위로 돌파하면 매수
    - 가격이 하단 밴드를 위→아래로 돌파하면 매도

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

    for i in range(1, n):
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        prev_price = price_arr[i - 1]
        curr_price = price_arr[i]
        prev_up = up_arr[i - 1]
        curr_up = up_arr[i]
        prev_lo = lo_arr[i - 1]
        curr_lo = lo_arr[i]

        # 상단 밴드를 돌파
        if prev_price <= prev_up and curr_price > curr_up:
            df.loc[df.index[i], signal_col] = 1
        # 하단 밴드를 이탈
        elif prev_price >= prev_lo and curr_price < curr_lo:
            df.loc[df.index[i], signal_col] = -1

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
    일목균형표 시그널 (추세추종).
    전환선이 기준선을 아래→위로 교차 & 가격이 구름대 위면 매수,
    반대면 매도.

    Args:
        df (pd.DataFrame)
        tenkan_col (str): 전환선
        kijun_col (str): 기준선
        span_a_col (str): 선행스팬A
        span_b_col (str): 선행스팬B
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
    for i in range(1, n):
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        prev_diff = ten_arr[i - 1] - kij_arr[i - 1]
        curr_diff = ten_arr[i] - kij_arr[i]
        cross_up = (prev_diff <= 0 and curr_diff > 0)
        cross_down = (prev_diff >= 0 and curr_diff < 0)

        if cross_up and (price_arr[i] > cloud_top[i]):
            df.loc[df.index[i], signal_col] = 1
        elif cross_down and (price_arr[i] < cloud_bot[i]):
            df.loc[df.index[i], signal_col] = -1

    return df


def psar_signal(
    df: pd.DataFrame,
    psar_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    파라볼릭 SAR(PSAR) 시그널 (추세추종).
    PSAR이 가격 아래에서 위로 교차 시 매수, 위에서 아래로 교차 시 매도.

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

    for i in range(1, n):
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        prev_diff = psar_arr[i - 1] - price_arr[i - 1]
        curr_diff = psar_arr[i] - price_arr[i]

        if prev_diff >= 0 and curr_diff < 0:
            df.loc[df.index[i], signal_col] = 1
        elif prev_diff <= 0 and curr_diff > 0:
            df.loc[df.index[i], signal_col] = -1

    return df


def supertrend_signal(
    df: pd.DataFrame,
    st_col: str,
    price_col: str,
    signal_col: str
) -> pd.DataFrame:
    """
    슈퍼트렌드 시그널 (추세추종).
    가격이 지표 위로 돌파 시 매수, 아래로 돌파 시 매도.

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

    for i in range(1, n):
        df.loc[df.index[i], signal_col] = df.loc[df.index[i - 1], signal_col]

        prev_diff = st_arr[i - 1] - price_arr[i - 1]
        curr_diff = st_arr[i] - price_arr[i]

        # 지표 위로 돌파
        if prev_diff >= 0 and curr_diff < 0:
            df.loc[df.index[i], signal_col] = 1
        # 지표 아래로 돌파
        elif prev_diff <= 0 and curr_diff > 0:
            df.loc[df.index[i], signal_col] = -1

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
    가격이 상단선 위면 매수, 하단선 아래면 매도, 그 외 0

    Args:
        df (pd.DataFrame)
        lower_col (str): DCL_x
        upper_col (str): DCU_x
        price_col (str)
        signal_col (str)

    Returns:
        pd.DataFrame
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
    K, D가 상단 임계값 이상이면 매수, 하단 임계값 이하이면 매도.

    Args:
        df (pd.DataFrame)
        stoch_k_col (str): %K 칼럼
        stoch_d_col (str): %D 칼럼
        lower_threshold (float)
        upper_threshold (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    k_arr = df[stoch_k_col].values
    d_arr = df[stoch_d_col].values
    n = len(df)

    for i in range(n):
        # K, D가 모두 upper_threshold 이상
        if k_arr[i] >= upper_threshold and d_arr[i] >= upper_threshold:
            df.loc[df.index[i], signal_col] = 1
        # K, D가 모두 lower_threshold 이하
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
    StochRSI K,D가 upper_threshold 이상이면 매수, lower_threshold 이하면 매도.

    Args:
        df (pd.DataFrame)
        k_col (str): StochRSI K 칼럼
        d_col (str): StochRSI D 칼럼
        lower_threshold (float)
        upper_threshold (float)
        signal_col (str)

    Returns:
        pd.DataFrame
    """
    df[signal_col] = 0
    k_arr = df[k_col].values
    d_arr = df[d_col].values
    n = len(df)

    for i in range(n):
        # K, D 모두 상단 임계값 이상
        if k_arr[i] >= upper_threshold and d_arr[i] >= upper_threshold:
            df.loc[df.index[i], signal_col] = 1
        # K, D 모두 하단 임계값 이하
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
    MFI가 upper_threshold를 넘으면 매수, lower_threshold보다 작으면 매도.

    Args:
        df (pd.DataFrame)
        mfi_col (str)
        lower_threshold (float)
        upper_threshold (float)
        signal_col (str)

    Returns:
        pd.DataFrame
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
    가격이 VWAP보다 위이면 매수, 아래이면 매도.

    Args:
        df (pd.DataFrame)
        vwap_col (str): VWAP 칼럼
        price_col (str): 종가 등
        signal_col (str): 결과 시그널 컬럼

    Returns:
        pd.DataFrame
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
