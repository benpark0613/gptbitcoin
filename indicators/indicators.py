# gptbitcoin/indicators/indicators.py
# 구글 스타일, 최소한의 한글 주석
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import pandas_ta as ta
from config.config import INDICATOR_CONFIG


def calc_sma_series(series: pd.Series, period: int) -> pd.Series:
    """
    단순 이동평균. min_periods=period로 설정하여
    초기 period 일 이전에는 NaN 처리.
    """
    return series.rolling(window=period, min_periods=period).mean()


def calc_rsi_series(close_s: pd.Series, period: int) -> pd.Series:
    """
    재귀 평활(Wilder) 방식으로 RSI 계산.
    (추가 SMA 평활은 하지 않는다)
    Args:
        close_s: 종가 시리즈
        period: RSI 기간 (예: 14)
    Returns:
        pd.Series: RSI 결과(소수점 둘째 자리)
    """
    diffs = close_s.diff()
    gains = diffs.where(diffs > 0, 0.0)
    losses = (-diffs).where(diffs < 0, 0.0)
    rsi_vals = [None] * len(close_s)

    if len(close_s) < period:
        return pd.Series(rsi_vals, index=close_s.index)

    # 초기(14) 구간
    avg_gain = gains.iloc[1:period + 1].mean()
    avg_loss = losses.iloc[1:period + 1].mean()
    if avg_loss == 0:
        rsi_vals[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_vals[period] = 100.0 - (100.0 / (1.0 + rs))

    # 이후 재귀식으로 평활
    for i in range(period + 1, len(close_s)):
        cur_gain = gains.iloc[i] if gains.iloc[i] > 0 else 0.0
        cur_loss = losses.iloc[i] if losses.iloc[i] > 0 else 0.0

        avg_gain = (avg_gain * (period - 1) + cur_gain) / period
        avg_loss = (avg_loss * (period - 1) + cur_loss) / period

        if avg_loss == 0:
            rsi_vals[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_vals[i] = 100.0 - (100.0 / (1.0 + rs))

    return pd.Series(rsi_vals, index=close_s.index).round(2)


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """
    OBV 계산.
    첫 봉 OBV=0, 종가가 전일 대비 상승이면 OBV += volume, 하락이면 OBV -= volume
    """
    obv_vals = [0] * len(close_s)
    for i in range(1, len(close_s)):
        if close_s.iloc[i] > close_s.iloc[i - 1]:
            obv_vals[i] = obv_vals[i - 1] + vol_s.iloc[i]
        else:
            obv_vals[i] = obv_vals[i - 1] - vol_s.iloc[i]
    return pd.Series(obv_vals, index=close_s.index)


def rolling_min_series(series: pd.Series, window: int) -> pd.Series:
    """최소값을 window 길이만큼 만족해야 계산."""
    return series.rolling(window=window, min_periods=window).min()


def rolling_max_series(series: pd.Series, window: int) -> pd.Series:
    """최대값을 window 길이만큼 만족해야 계산."""
    return series.rolling(window=window, min_periods=window).max()


def calc_all_indicators(df: pd.DataFrame, cfg: dict = None) -> pd.DataFrame:
    """
    config.config의 INDICATOR_CONFIG를 사용하여
    다음 컬럼들을 계산/추가한다.

    - MA: ma_5, ma_10, ma_20, ma_50, ma_100, ma_200
    - RSI: rsi_14, rsi_21, rsi_30
    - Filter: filter_min_10, filter_max_10, filter_min_20, filter_max_20
    - Support_Resistance: sr_min_10, sr_max_10, sr_min_20, sr_max_20
    - Channel_Breakout: ch_min_14, ch_max_14, ch_min_20, ch_max_20
    - OBV: obv
      + obv_sma_5, obv_sma_10, obv_sma_30, obv_sma_50, obv_sma_100
    """
    # cfg 미지정시 기본 INDICATOR_CONFIG 사용
    if cfg is None:
        cfg = INDICATOR_CONFIG

    # 필수 컬럼 확인
    if "close" not in df.columns or "volume" not in df.columns:
        raise ValueError("데이터프레임에 'close', 'volume'가 필요합니다.")

    # 1) MA
    if "MA" in cfg:
        sp_list = cfg["MA"].get("short_periods", [])
        lp_list = cfg["MA"].get("long_periods", [])
        # ma_5, ma_10, ma_20
        for sp in sp_list:
            df[f"ma_{sp}"] = calc_sma_series(df["close"], sp).round(2)
        # ma_50, ma_100, ma_200
        for lp in lp_list:
            df[f"ma_{lp}"] = calc_sma_series(df["close"], lp).round(2)

    # 2) RSI
    if "RSI" in cfg:
        length_list = cfg["RSI"].get("lengths", [])
        # rsi_14, rsi_21, rsi_30
        for length in length_list:
            df[f"rsi_{length}"] = calc_rsi_series(df["close"], length)

    # 3) OBV
    if "OBV" in cfg:
        # 1) 원본 OBV를 obv_raw 컬럼에 계산 (반올림 전 값)
        if "obv_raw" not in df.columns:
            df["obv_raw"] = calc_obv_series(df["close"], df["volume"])

        # 2) obv_raw를 반올림하여 obv 컬럼에 저장
        #    ("OBV 구해서 반올림해서 저장" 요구사항)
        df["obv"] = df["obv_raw"].apply(round_abs_decimal)

        # 3) obv_sma_* 계산할 때는
        #    "반올림하지 않은 원본 obv_raw"로 rolling.mean() 수행
        sp_list = cfg["OBV"].get("short_periods", [])
        lp_list = cfg["OBV"].get("long_periods", [])
        all_obv_periods = sp_list + lp_list  # 예: [5,10,30,50,100]

        for p in all_obv_periods:
            col_name = f"obv_sma_{p}"

            # ★ 원본(obv_raw) 기반으로 SMA 계산
            s = df["obv_raw"].rolling(window=p, min_periods=p).mean()

            # 필요하다면 SMA도 반올림해 저장할 수 있음
            df[col_name] = s.apply(round_abs_decimal)

    # 4) Filter
    if "Filter" in cfg:
        windows = cfg["Filter"].get("windows", [])
        for w in windows:
            df[f"filter_min_{w}"] = rolling_min_series(df["close"], w).round(2)
            df[f"filter_max_{w}"] = rolling_max_series(df["close"], w).round(2)

    # 5) Support_Resistance
    if "Support_Resistance" in cfg:
        sr_windows = cfg["Support_Resistance"].get("windows", [])
        for w in sr_windows:
            df[f"sr_min_{w}"] = rolling_min_series(df["close"], w).round(2)
            df[f"sr_max_{w}"] = rolling_max_series(df["close"], w).round(2)

    # 6) Channel_Breakout
    if "Channel_Breakout" in cfg:
        ch_windows = cfg["Channel_Breakout"].get("windows", [])
        for w in ch_windows:
            df[f"ch_min_{w}"] = rolling_min_series(df["close"], w).round(2)
            df[f"ch_max_{w}"] = rolling_max_series(df["close"], w).round(2)

    return df


def round_abs_decimal(x):
    """
    1) NaN/None이면 그냥 None 반환
    2) 음수면 절댓값을 취함
    3) 소수점 첫째 자리(= 정수 단위)에서 반올림(ROUND_HALF_UP)
    4) 음수였으면 다시 부호를 씌움
    """
    if x is None or pd.isna(x):
        return None

    sign = -1 if x < 0 else 1
    d = Decimal(str(abs(x)))
    d_rounded = d.quantize(Decimal('1'), rounding=ROUND_HALF_UP)  # 일의 자리에서 반올림
    return float(sign * d_rounded)


def calc_obv_series(close_s: pd.Series, vol_s: pd.Series) -> pd.Series:
    """
    예시: 기본 OBV 계산
    (실제 구현은 기존 코드와 동일)
    """
    obv_vals = [0] * len(close_s)
    for i in range(1, len(close_s)):
        if close_s.iloc[i] > close_s.iloc[i - 1]:
            obv_vals[i] = obv_vals[i - 1] + vol_s.iloc[i]
        else:
            obv_vals[i] = obv_vals[i - 1] - vol_s.iloc[i]
    return pd.Series(obv_vals, index=close_s.index)