# gptbitcoin/indicators/trend_channels.py
# Bollinger Bands, Ichimoku, PSAR, Supertrend 등 핵심 파라미터만 사용해 계산.
import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_bollinger_bands(
    close_s: pd.Series,
    period: int,
    stddev_mult: float
) -> pd.DataFrame:
    """볼린저 밴드 계산 (기존과 동일)."""
    bb_df = ta.bbands(close_s, length=period, std=stddev_mult)
    if bb_df is None or bb_df.empty:
        na = [np.nan] * len(close_s)
        return pd.DataFrame({
            "boll_mid": na,
            "boll_upper": na,
            "boll_lower": na
        }, index=close_s.index)

    cols = list(bb_df.columns)
    lc = next((c for c in cols if "BBL" in c), None)
    mc = next((c for c in cols if "BBM" in c), None)
    uc = next((c for c in cols if "BBU" in c), None)

    return pd.DataFrame({
        "boll_mid": bb_df[mc] if mc else np.nan,
        "boll_upper": bb_df[uc] if uc else np.nan,
        "boll_lower": bb_df[lc] if lc else np.nan
    }, index=close_s.index)


def calc_ichimoku(
        high_s: pd.Series,
        low_s: pd.Series,
        close_s: pd.Series,
        tenkan_period: int,
        kijun_period: int,
        span_b_period: int
) -> pd.DataFrame:
    """
    일목균형표(Ichimoku) 지표를 직접 계산한다.

    일반 공식(파라미터는 보통 9,26,52):
      - Tenkan-sen (Conversion Line) = (지난 X일 고가 최댓값 + 지난 X일 저가 최솟값)/2  (기본 X=9)
      - Kijun-sen  (Base Line)       = (지난 Y일 고가 최댓값 + 지난 Y일 저가 최솟값)/2  (기본 Y=26)
      - Senkou Span A = (Tenkan + Kijun)/2 를 Y일 만큼 앞으로 shift
      - Senkou Span B = (지난 Z일 고가 최댓값 + 지난 Z일 저가 최솟값)/2 를 Y일 만큼 앞으로 shift  (기본 Z=52)
      - Chikou Span   = 종가를 Y일 만큼 뒤로 shift (과거로 이동)

    Args:
        high_s (pd.Series): 고가 시계열
        low_s (pd.Series): 저가 시계열
        close_s (pd.Series): 종가 시계열
        tenkan_period (int): Tenkan-sen 기간(기본 9)
        kijun_period (int): Kijun-sen 기간(기본 26)
        span_b_period (int): Span B 기간(기본 52)

    Returns:
        pd.DataFrame: [
            "ichimoku_tenkan",
            "ichimoku_kijun",
            "ichimoku_span_a",
            "ichimoku_span_b",
            "ichimoku_chikou"
        ]
        각 컬럼에 해당 지표 값이 들어있음.
    """
    # Tenkan-sen (Conversion Line)
    tenkan = (high_s.rolling(tenkan_period).max() + low_s.rolling(tenkan_period).min()) / 2.0

    # Kijun-sen (Base Line)
    kijun = (high_s.rolling(kijun_period).max() + low_s.rolling(kijun_period).min()) / 2.0

    span_a = ((tenkan + kijun) / 2.0).shift(kijun_period)

    # Span B = (지난 span_b_period일 고가/저가) 평균을 kijun_period만큼 앞으로 shift
    span_b = (
            (high_s.rolling(span_b_period).max() + low_s.rolling(span_b_period).min()) / 2.0
    ).shift(kijun_period)

    # Chikou Span = 종가를 kijun_period만큼 뒤로 shift => .shift(-kijun_period)
    chikou = close_s.shift(-kijun_period)

    # 결과 합치기
    df_ich = pd.DataFrame({
        "ichimoku_tenkan": tenkan,
        "ichimoku_kijun": kijun,
        "ichimoku_span_a": span_a,
        "ichimoku_span_b": span_b,
        "ichimoku_chikou": chikou
    }, index=close_s.index)

    return df_ich


def calc_psar(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    acceleration_step: float,
    acceleration_max: float
) -> pd.Series:
    """
    PSAR 계산: pandas-ta는 psar를 여러 컬럼(PSARl_, PSARs_ 등)에 나눠서 줌.
    적절히 병합하거나 한쪽(롱/숏)만 선택해서 대표값을 만든다.
    """
    psar_df = ta.psar(
        high=high_s,
        low=low_s,
        close=close_s,
        af=acceleration_step,
        max_af=acceleration_max
    )
    if psar_df is None or psar_df.empty:
        return pd.Series([np.nan] * len(close_s), index=close_s.index)

    # 보통 ["PSARl_xxx", "PSARs_xxx", "PSARaf_xxx", "PSARr_xxx"] 등이 생성
    cols = psar_df.columns
    psar_long_col = next((c for c in cols if c.startswith("PSARl_")), None)
    psar_short_col= next((c for c in cols if c.startswith("PSARs_")), None)

    # 여기서는 'long' / 'short' 위치에 따라 값이 나뉘므로,
    # 일반적으로 "둘 중 하나가 NaN이 아니면 그 값을 취한다" 등으로 단일 시리즈를 만든다.
    # 아래는 단순 예시:
    out_psar = np.where(
        psar_df[psar_long_col].notna(),
        psar_df[psar_long_col],
        psar_df[psar_short_col]
    )
    return pd.Series(out_psar, index=psar_df.index, name="psar_value")


def calc_supertrend(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    atr_period: int,
    multiplier: float
) -> pd.Series:
    """슈퍼트렌드 계산 (기존과 동일)."""
    st_df = ta.supertrend(
        high=high_s,
        low=low_s,
        close=close_s,
        length=atr_period,
        multiplier=multiplier
    )
    if st_df is None or st_df.empty:
        return pd.Series([np.nan] * len(close_s), index=close_s.index)

    st_col = [c for c in st_df.columns if c.startswith("SUPERT_")]
    if not st_col:
        return pd.Series([np.nan] * len(close_s), index=close_s.index)

    return st_df[st_col[0]]
