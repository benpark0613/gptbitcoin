# gptbitcoin/indicators/aggregator.py
# config 설정에 맞춰 모든 지표를 계산하고 DataFrame에 추가한다.
# OHLC 컬럼을 소문자로 ("open", "high", "low", "close", "volume") 사용하도록 수정

from typing import Optional, Dict
import pandas as pd

# 새로 작성한 지표 모듈 임포트
from .trend_indicators import (
    calc_ma, calc_macd, calc_dmi_adx, calc_ichimoku,
    calc_psar, calc_supertrend, calc_donchian_channel
)
from .momentum_indicators import (
    calc_rsi, calc_stoch, calc_stoch_rsi, calc_mfi
)
from .volatility_indicators import calc_boll
from .volume_indicators import (
    calc_obv, calc_vwap
)

# 기본 config
from config.indicator_config import INDICATOR_CONFIG


def calc_all_indicators(df: pd.DataFrame, cfg: Optional[Dict] = None) -> pd.DataFrame:
    """
    config에 정의된 보조지표 설정에 따라,
    trend_indicators.py, momentum_indicators.py,
    volatility_indicators.py, volume_indicators.py 등을 이용해
    df에 컬럼으로 보조지표들을 추가한다.

    - df에는 'open', 'high', 'low', 'close', 'volume' 등이 있어야 함 (소문자)
    - cfg가 None이면 기본 INDICATOR_CONFIG 사용
    - 결과적으로 df + 새 컬럼 형태로 반환
    """
    if cfg is None:
        cfg = INDICATOR_CONFIG

    # 새 지표 컬럼 임시 저장
    new_cols = pd.DataFrame(index=df.index)

    # =============== Trend Indicators ===============
    if "MA" in cfg:
        short_list = cfg["MA"].get("short_ma_periods", [])
        long_list = cfg["MA"].get("long_ma_periods", [])
        for s_period in short_list:
            for l_period in long_list:
                if s_period >= l_period:
                    continue
                ma_df = calc_ma(df, s_period, l_period)
                new_cols = pd.concat([new_cols, ma_df], axis=1)

    if "MACD" in cfg:
        fasts = cfg["MACD"].get("fast_periods", [])
        slows = cfg["MACD"].get("slow_periods", [])
        signals = cfg["MACD"].get("signal_periods", [])
        for f in fasts:
            for s in slows:
                if f >= s:
                    continue
                for sig in signals:
                    macd_df = calc_macd(df, f, s, sig)
                    new_cols = pd.concat([new_cols, macd_df], axis=1)

    if "DMI_ADX" in cfg:
        lookbacks = cfg["DMI_ADX"].get("lookback_periods", [])
        for lb in lookbacks:
            adx_df = calc_dmi_adx(df, lb)
            new_cols = pd.concat([new_cols, adx_df], axis=1)

    if "ICHIMOKU" in cfg:
        tenkans = cfg["ICHIMOKU"].get("tenkan_period", [])
        kijuns = cfg["ICHIMOKU"].get("kijun_period", [])
        spans = cfg["ICHIMOKU"].get("senkou_span_b_period", [])
        for t in tenkans:
            for k in kijuns:
                for sp in spans:
                    ich_df = calc_ichimoku(df, t, k, sp)
                    new_cols = pd.concat([new_cols, ich_df], axis=1)

    if "PSAR" in cfg:
        steps = cfg["PSAR"].get("acceleration_step", [])
        maxes = cfg["PSAR"].get("acceleration_max", [])
        for stp in steps:
            for mx in maxes:
                psar_sr = calc_psar(df, stp, mx)
                new_cols[psar_sr.name] = psar_sr

    if "SUPERTREND" in cfg:
        atr_list = cfg["SUPERTREND"].get("atr_period", [])
        mul_list = cfg["SUPERTREND"].get("multiplier", [])
        for ap in atr_list:
            for mt in mul_list:
                st_df = calc_supertrend(df, ap, mt)
                new_cols = pd.concat([new_cols, st_df], axis=1)

    if "DONCHIAN_CHANNEL" in cfg:
        dc_list = cfg["DONCHIAN_CHANNEL"].get("lookback_periods", [])
        for lb in dc_list:
            dc_df = calc_donchian_channel(df, lb)
            new_cols = pd.concat([new_cols, dc_df], axis=1)

    # =============== Momentum Indicators ===============
    if "RSI" in cfg:
        lbs = cfg["RSI"].get("lookback_periods", [])
        for lb in lbs:
            rsi_sr = calc_rsi(df, lb)
            new_cols[rsi_sr.name] = rsi_sr

    if "STOCH" in cfg:
        k_list = cfg["STOCH"].get("k_period", [])
        d_list = cfg["STOCH"].get("d_period", [])
        for k_per in k_list:
            for d_per in d_list:
                stoch_df = calc_stoch(df, k_per, d_per)
                new_cols = pd.concat([new_cols, stoch_df], axis=1)

    if "STOCH_RSI" in cfg:
        lb_list = cfg["STOCH_RSI"].get("lookback_periods", [])
        k_list = cfg["STOCH_RSI"].get("k_period", [])
        d_list = cfg["STOCH_RSI"].get("d_period", [])
        for lb in lb_list:
            for k_ in k_list:
                for d_ in d_list:
                    stochrsi_df = calc_stoch_rsi(df, lb, k_, d_)
                    new_cols = pd.concat([new_cols, stochrsi_df], axis=1)

    if "MFI" in cfg:
        lb_list = cfg["MFI"].get("lookback_periods", [])
        for lb in lb_list:
            mfi_sr = calc_mfi(df, lb)
            new_cols[mfi_sr.name] = mfi_sr

    # =============== Volatility Indicators ===============
    if "BOLL" in cfg:
        lbs = cfg["BOLL"].get("lookback_periods", [])
        stds = cfg["BOLL"].get("stddev_multipliers", [])
        for lb in lbs:
            for sd in stds:
                boll_df = calc_boll(df, lb, sd)
                new_cols = pd.concat([new_cols, boll_df], axis=1)

    # =============== Volume Indicators ===============
    if "OBV" in cfg:
        obv_sr = calc_obv(df)
        new_cols[obv_sr.name] = obv_sr
        # config["OBV"] 내 threshold 등은 신호 판단용

    if "VWAP" in cfg:
        vwap_sr = calc_vwap(df)
        new_cols[vwap_sr.name] = vwap_sr

    # ============== 결과 합치기 ==============
    if not new_cols.empty:
        df = pd.concat([df, new_cols], axis=1)

    return df
