# gptbitcoin/indicators/momentum_indicators.py
# 최소한의 한글 주석
# 구글 스타일 Docstring
# 모멘텀/추세 지표 (예: MACD, DMI/ADX 등)

import numpy as np
import pandas as pd

def calc_macd(
    close_s: pd.Series,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9
) -> pd.DataFrame:
    """
    MACD를 numpy 방식으로 직접 계산한다.

    Args:
        close_s (pd.Series): 종가
        fast_period (int): 단기 EMA 기간
        slow_period (int): 장기 EMA 기간
        signal_period (int): 시그널 EMA 기간

    Returns:
        pd.DataFrame: macd_line, macd_signal, macd_hist
    """
    arr = close_s.to_numpy(dtype=float)

    def calc_ema_np(data: np.ndarray, period: int) -> np.ndarray:
        """numpy 배열에 대한 EMA"""
        if period < 1 or len(data) == 0:
            return np.full_like(data, np.nan)
        alpha = 2.0 / (period + 1.0)
        ema_arr = np.zeros_like(data)
        ema_arr[0] = data[0]
        for i in range(1, len(data)):
            ema_arr[i] = alpha * data[i] + (1 - alpha) * ema_arr[i - 1]
        return ema_arr

    ema_fast = calc_ema_np(arr, fast_period)
    ema_slow = calc_ema_np(arr, slow_period)
    macd_line = ema_fast - ema_slow
    signal_line = calc_ema_np(macd_line, signal_period)
    hist_line = macd_line - signal_line

    df_macd = pd.DataFrame({
        "macd_line": macd_line,
        "macd_signal": signal_line,
        "macd_hist": hist_line
    }, index=close_s.index)
    return df_macd


def calc_dmi_adx(
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
    period: int = 14
) -> pd.DataFrame:
    """
    DMI(+DI, -DI) 및 ADX를 numpy 기반으로 계산한다.

    Args:
        high_s (pd.Series): 고가
        low_s (pd.Series): 저가
        close_s (pd.Series): 종가
        period (int): 계산 기간

    Returns:
        pd.DataFrame: plus_di, minus_di, adx
    """
    h = high_s.to_numpy(dtype=float)
    l = low_s.to_numpy(dtype=float)
    c = close_s.to_numpy(dtype=float)

    ph = np.roll(h, 1)
    pl = np.roll(l, 1)
    pc = np.roll(c, 1)

    ph[0], pl[0], pc[0] = h[0], l[0], c[0]

    up_move = h - ph
    down_move = pl - l

    plus_dm = np.where((up_move > 0) & (up_move > down_move), up_move, 0.0)
    minus_dm = np.where((down_move > 0) & (down_move > up_move), down_move, 0.0)

    tr1 = np.abs(h - l)
    tr2 = np.abs(h - pc)
    tr3 = np.abs(l - pc)
    true_range = np.maximum(tr1, np.maximum(tr2, tr3))

    def rolling_sum_np(arr: np.ndarray, w: int) -> np.ndarray:
        """numpy 누적합으로 rolling sum"""
        if w < 1 or len(arr) == 0:
            return np.full_like(arr, np.nan)
        csum = np.cumsum(arr)
        out = np.full_like(arr, np.nan)
        for i in range(w - 1, len(arr)):
            if i - w < 0:
                out[i] = csum[i]
            else:
                out[i] = csum[i] - csum[i - w]
        return out

    pdm_sum = rolling_sum_np(plus_dm, period)
    mdm_sum = rolling_sum_np(minus_dm, period)
    tr_sum = rolling_sum_np(true_range, period)

    # 0 방지
    tr_sum = np.where(tr_sum == 0, np.nan, tr_sum)
    plus_di = 100.0 * (pdm_sum / tr_sum)
    minus_di = 100.0 * (mdm_sum / tr_sum)

    diff_di = np.abs(plus_di - minus_di)
    sum_di = plus_di + minus_di
    sum_di[sum_di == 0] = np.nan
    dx = 100.0 * (diff_di / sum_di)

    adx_arr = np.full_like(dx, np.nan)
    # 처음 period 이후부터 DX의 평균
    if len(dx) >= period:
        # 초기값: period 구간 평균
        adx_arr[period-1] = np.nanmean(dx[:period])
        # 이후 EMA 유사방식(또는 단순평균 갱신)
        for i in range(period, len(dx)):
            adx_arr[i] = ((adx_arr[i-1] * (period - 1)) + dx[i]) / period

    df_dmi = pd.DataFrame({
        "plus_di": plus_di,
        "minus_di": minus_di,
        "adx": adx_arr
    }, index=high_s.index)
    return df_dmi
