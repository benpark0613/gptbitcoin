# gptbitcoin/indicators/indicators.py
"""
보조지표를 계산하는 모듈.
구글 스타일 docstring과 최소한의 한글 주석을 사용한다.
이 모듈은 OHLCV 데이터에 대해 SMA, RSI, OBV, Filter, SR(Support/Resistance), CB(Channel Breakout) 지표를 계산한다.
"""

import pandas as pd
from typing import Optional

# 보조지표 설정(INDICATOR_CONFIG)은 config/indicator_config.py에 정의되어 있음
from config.indicator_config import INDICATOR_CONFIG


def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    """
    단순 이동평균(SMA)을 계산한다.

    Args:
        series (pd.Series): 가격 시계열 (예: 종가)
        period (int): 이동평균 구간

    Returns:
        pd.Series: SMA 시계열
    """
    return series.rolling(window=period, min_periods=period).mean()


def calc_rsi_series(close_s: pd.Series, period: int) -> pd.Series:
    """
    RSI 지표를 계산한다. (Wilder 방식을 모방)
    초기 구간은 period만큼 단순 평균값을 사용하고 이후는 지수평활 개념을 적용한다.

    Args:
        close_s (pd.Series): 종가 시리즈
        period (int): RSI 구간

    Returns:
        pd.Series: RSI 시리즈 (0~100)
    """
    diffs = close_s.diff()
    gains = diffs.where(diffs > 0, 0.0)
    losses = (-diffs).where(diffs < 0, 0.0)

    rsi_vals = [None] * len(close_s)
    if len(close_s) < period:
        return pd.Series(rsi_vals, index=close_s.index)

    avg_gain = gains.iloc[1:period + 1].mean()
    avg_loss = losses.iloc[1:period + 1].mean()

    if avg_loss == 0:
        rsi_vals[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_vals[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, len(close_s)):
        cur_gain = gains.iloc[i] if gains.iloc[i] > 0 else 0.0
        cur_loss = losses.iloc[i] if losses.iloc[i] > 0 else 0.0

        avg_gain = ((avg_gain * (period - 1)) + cur_gain) / period
        avg_loss = ((avg_loss * (period - 1)) + cur_loss) / period

        if avg_loss == 0:
            rsi_vals[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_vals[i] = 100.0 - (100.0 / (1.0 + rs))

    return pd.Series(rsi_vals, index=close_s.index)


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """
    OBV (On-Balance Volume)를 계산한다.
    첫 봉은 0으로 시작하며, 종가가 상승하면 +volume, 하락하면 -volume을 누적한다.

    Args:
        close_s (pd.Series): 종가 시리즈
        vol_s (pd.Series): 거래량 시리즈

    Returns:
        pd.Series: OBV 시리즈
    """
    obv_vals = [0] * len(close_s)
    for i in range(1, len(close_s)):
        if close_s.iloc[i] > close_s.iloc[i - 1]:
            obv_vals[i] = obv_vals[i - 1] + vol_s.iloc[i]
        else:
            obv_vals[i] = obv_vals[i - 1] - vol_s.iloc[i]

    return pd.Series(obv_vals, index=close_s.index)


def rolling_min_series(series: pd.Series, window: int) -> pd.Series:
    """
    window 구간의 최솟값을 구한다.

    Args:
        series (pd.Series): 대상 시계열
        window (int): 롤링 구간

    Returns:
        pd.Series: 롤링 최솟값 시계열
    """
    return series.rolling(window=window, min_periods=window).min()


def rolling_max_series(series: pd.Series, window: int) -> pd.Series:
    """
    window 구간의 최댓값을 구한다.

    Args:
        series (pd.Series): 대상 시계열
        window (int): 롤링 구간

    Returns:
        pd.Series: 롤링 최댓값 시계열
    """
    return series.rolling(window=window, min_periods=window).max()


def calc_all_indicators(df: pd.DataFrame, cfg: Optional[dict] = None) -> pd.DataFrame:
    """
    config/indicator_config.py의 INDICATOR_CONFIG를 참고하여
    MA, RSI, OBV, Filter, SR, CB 지표 컬럼을 DataFrame에 추가한다.

    Args:
        df (pd.DataFrame): OHLCV 데이터프레임 (open_time, open, high, low, close, volume 등 필수)
        cfg (dict, optional): 지표 설정. None이면 INDICATOR_CONFIG 사용.

    Returns:
        pd.DataFrame: 지표 컬럼이 추가된 DataFrame
    """
    if cfg is None:
        cfg = INDICATOR_CONFIG

    if "close" not in df.columns or "volume" not in df.columns:
        raise ValueError("DataFrame에 'close'와 'volume' 컬럼이 필요합니다.")

    # === MA 계산 ===
    if "MA" in cfg:
        sp_list = cfg["MA"].get("short_ma_periods", [])
        lp_list = cfg["MA"].get("long_ma_periods", [])
        # MA에서 실제로는 band_filter/time_delay 등은 여기서 계산 안 함
        for sp in sp_list:
            col_sp = f"ma_{sp}"
            df[col_sp] = calc_sma_series(df["close"], sp)
        for lp in lp_list:
            col_lp = f"ma_{lp}"
            df[col_lp] = calc_sma_series(df["close"], lp)

    # === RSI 계산 ===
    if "RSI" in cfg:
        rsi_lookbacks = cfg["RSI"].get("lookback_periods", [])
        for lb in rsi_lookbacks:
            col_rsi = f"rsi_{lb}"
            df[col_rsi] = calc_rsi_series(df["close"], lb)

    # === OBV 계산 ===
    if "OBV" in cfg:
        # obv_raw 계산
        if "obv_raw" not in df.columns:
            df["obv_raw"] = calc_obv_series(df["close"], df["volume"])
        # obv_sma
        sp_list = cfg["OBV"].get("short_ma_periods", [])
        lp_list = cfg["OBV"].get("long_ma_periods", [])
        for p in set(sp_list + lp_list):
            col_obv_sma = f"obv_sma_{p}"
            df[col_obv_sma] = df["obv_raw"].rolling(window=p, min_periods=p).mean()

    # === Filter (필터 룰) ===
    # Filter 룰에서 roll min/max 시 사용될 lookback_periods
    if "Filter" in cfg:
        flb_list = cfg["Filter"].get("lookback_periods", [])
        for w in flb_list:
            df[f"filter_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"filter_max_{w}"] = rolling_max_series(df["close"], w)

    # === SR (Support/Resistance) ===
    if "SR" in cfg:
        sr_list = cfg["SR"].get("lookback_periods", [])
        for w in sr_list:
            df[f"sr_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"sr_max_{w}"] = rolling_max_series(df["close"], w)

    # === CB (Channel Breakout) ===
    if "CB" in cfg:
        cb_list = cfg["CB"].get("lookback_periods", [])
        for w in cb_list:
            df[f"ch_min_{w}"] = rolling_min_series(df["close"], w)
            df[f"ch_max_{w}"] = rolling_max_series(df["close"], w)

    return df
