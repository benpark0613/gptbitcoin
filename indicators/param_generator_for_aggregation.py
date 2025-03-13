# gptbitcoin/indicators/param_generator_for_aggregation.py
# config에 정의된 모든 보조지표를 한 번에 계산하여 DataFrame에 칼럼으로 추가한다.
# combo_generator_for_backtest.py는 (단기/장기) 쌍 등 백테스트 파라미터 자동 생성용.
# 여기서는 "보조지표 계산"만 단일 함수로 전부 수행한다.

"""
한 번에 보조지표를 전부 계산하기 위한 모듈.

이 모듈은 config/indicator_config.py의 설정값(기간, 파라미터 등)에 따라
모든 보조지표를 한 번에 DataFrame에 추가한다. 예를 들어:
- MA(short_ma_periods, long_ma_periods)
- RSI(lookback_periods)
- OBV(단일 raw + short/long 이동평균)
- MACD(fast, slow, signal)
- DMI_ADX
- BOLL
- ICHIMOKU
- PSAR
- SUPERTREND
- DONCHIAN_CHANNEL
- STOCH
- STOCH_RSI
- MFI
- VWAP

사용 예시:
    from indicators.param_generator_for_aggregation import calc_all_indicators_for_aggregation
    from config.indicator_config import INDICATOR_CONFIG

    df_with_inds = calc_all_indicators_for_aggregation(df, INDICATOR_CONFIG)
    # df_with_inds에 모든 지표 컬럼이 추가됨
"""

from typing import Dict
import pandas as pd

# 필요한 지표 계산 함수들 (이미 프로젝트 내 존재)
from indicators.momentum_indicators import (
    calc_rsi, calc_stoch, calc_stoch_rsi, calc_mfi
)
from indicators.trend_indicators import (
    calc_sma, calc_macd, calc_dmi_adx, calc_ichimoku,
    calc_psar, calc_supertrend, calc_donchian_channel
)
from indicators.volatility_indicators import calc_boll
from indicators.volume_indicators import calc_obv, calc_vwap


def calc_all_indicators_for_aggregation(df: pd.DataFrame,
                                        cfg: Dict[str, Dict]) -> pd.DataFrame:
    """
    config/indicator_config.py에 정의된 모든 보조지표를
    한 번에 계산하여 df에 새로운 컬럼으로 추가한다.

    Args:
        df (pd.DataFrame): OHLCV DataFrame
          - 반드시 ["close"] 등 필요한 컬럼이 포함되어 있어야 함.
        cfg (Dict[str, Dict]): indicator_config.py 내용
          - 예: {
              "MA": {
                  "short_ma_periods": [...],
                  "long_ma_periods": [...]
              },
              "RSI": {
                  "lookback_periods": [...],
                  "thresholds": [...]
              },
              ...
            }

    Returns:
        pd.DataFrame: 지표 컬럼이 추가된 DataFrame (원본 df를 복사하지 않고 직접 변경).
    """
    # df를 바로 수정하고 싶지 않다면 copy() 사용 가능
    # 여기서는 df에 바로 붙인다고 가정
    new_cols = pd.DataFrame(index=df.index)

    # -----------------------------------------------------------------
    # 1) MA: short + long 각각의 이동평균 컬럼 생성
    # -----------------------------------------------------------------
    if "MA" in cfg:
        ma_cfg = cfg["MA"]
        sp_list = ma_cfg.get("short_ma_periods", [])
        lp_list = ma_cfg.get("long_ma_periods", [])
        # short, long 각각 반복하여 ma_5, ma_10, ... ma_200 등 생성
        for sp in sp_list:
            col_name = f"ma_{sp}"
            if col_name not in df.columns:
                ma_sr = calc_sma(df["close"], sp)
                new_cols[col_name] = ma_sr
        for lp in lp_list:
            col_name = f"ma_{lp}"
            if col_name not in df.columns:
                ma_sr = calc_sma(df["close"], lp)
                new_cols[col_name] = ma_sr

    # -----------------------------------------------------------------
    # 2) RSI
    # -----------------------------------------------------------------
    if "RSI" in cfg:
        rsi_cfg = cfg["RSI"]
        lb_list = rsi_cfg.get("lookback_periods", [])
        for lb in lb_list:
            col_name = f"rsi_{lb}"
            if col_name not in df.columns:
                rsi_sr = calc_rsi(df, lb)  # momentum_indicators.calc_rsi
                new_cols[col_name] = rsi_sr

    # -----------------------------------------------------------------
    # 3) OBV + 그 이동평균 (short, long)
    # -----------------------------------------------------------------
    if "OBV" in cfg:
        obv_cfg = cfg["OBV"]
        sp_list = obv_cfg.get("short_ma_periods", [])
        lp_list = obv_cfg.get("long_ma_periods", [])

        # OBV raw
        raw_col = "obv_raw"
        if raw_col not in df.columns:
            obv_sr = calc_obv(df)  # volume_indicators.calc_obv
            new_cols[raw_col] = obv_sr

        # OBV SMA
        # obv_sma_5, obv_sma_10, ...
        for sp in sp_list:
            col_name = f"obv_sma_{sp}"
            if col_name not in df.columns:
                if raw_col not in df.columns and raw_col not in new_cols.columns:
                    # 혹시 obv_raw 안 만들어졌으면?
                    obv_sr = calc_obv(df)
                    new_cols[raw_col] = obv_sr
                obv_ma_sr = calc_sma(df[raw_col] if raw_col in df.columns else new_cols[raw_col], sp)
                new_cols[col_name] = obv_ma_sr

        for lp in lp_list:
            col_name = f"obv_sma_{lp}"
            if col_name not in df.columns:
                obv_ma_sr = calc_sma(df[raw_col] if raw_col in df.columns else new_cols[raw_col], lp)
                new_cols[col_name] = obv_ma_sr

    # -----------------------------------------------------------------
    # 4) MACD
    # -----------------------------------------------------------------
    if "MACD" in cfg:
        macd_cfg = cfg["MACD"]
        fasts = macd_cfg.get("fast_periods", [])
        slows = macd_cfg.get("slow_periods", [])
        signals = macd_cfg.get("signal_periods", [])
        for f_ in fasts:
            for s_ in slows:
                if f_ >= s_:
                    continue
                for sig in signals:
                    macd_df = calc_macd(df, f_, s_, sig)
                    new_cols = pd.concat([new_cols, macd_df], axis=1)

    # -----------------------------------------------------------------
    # 5) DMI_ADX
    # -----------------------------------------------------------------
    if "DMI_ADX" in cfg:
        dmi_cfg = cfg["DMI_ADX"]
        lb_list = dmi_cfg.get("lookback_periods", [])
        for lb in lb_list:
            adx_df = calc_dmi_adx(df, lb)
            new_cols = pd.concat([new_cols, adx_df], axis=1)

    # -----------------------------------------------------------------
    # 6) BOLL (볼린저 밴드)
    # -----------------------------------------------------------------
    if "BOLL" in cfg:
        boll_cfg = cfg["BOLL"]
        lb_list = boll_cfg.get("lookback_periods", [])
        std_list = boll_cfg.get("stddev_multipliers", [])
        for lb in lb_list:
            for sd in std_list:
                boll_df = calc_boll(df, lb, sd)
                new_cols = pd.concat([new_cols, boll_df], axis=1)

    # -----------------------------------------------------------------
    # 7) ICHIMOKU
    # -----------------------------------------------------------------
    if "ICHIMOKU" in cfg:
        ich_cfg = cfg["ICHIMOKU"]
        tenkans = ich_cfg.get("tenkan_period", [])
        kijuns = ich_cfg.get("kijun_period", [])
        spans = ich_cfg.get("senkou_span_b_period", [])
        for t_ in tenkans:
            for k_ in kijuns:
                # if t_ >= k_: pass # 필요 시 제외
                for s_ in spans:
                    ich_df = calc_ichimoku(df, t_, k_, s_)
                    new_cols = pd.concat([new_cols, ich_df], axis=1)

    # -----------------------------------------------------------------
    # 8) PSAR
    # -----------------------------------------------------------------
    if "PSAR" in cfg:
        psar_cfg = cfg["PSAR"]
        steps = psar_cfg.get("acceleration_step", [])
        maxes = psar_cfg.get("acceleration_max", [])
        for st_ in steps:
            for mx_ in maxes:
                if st_ > mx_:
                    continue
                psar_sr = calc_psar(df, st_, mx_)
                new_cols[psar_sr.name] = psar_sr

    # -----------------------------------------------------------------
    # 9) SUPERTREND
    # -----------------------------------------------------------------
    if "SUPERTREND" in cfg:
        st_cfg = cfg["SUPERTREND"]
        atrs = st_cfg.get("atr_period", [])
        mults = st_cfg.get("multiplier", [])
        for a_ in atrs:
            for m_ in mults:
                st_df = calc_supertrend(df, a_, m_)
                new_cols = pd.concat([new_cols, st_df], axis=1)

    # -----------------------------------------------------------------
    # 10) DONCHIAN_CHANNEL
    # -----------------------------------------------------------------
    if "DONCHIAN_CHANNEL" in cfg:
        dc_cfg = cfg["DONCHIAN_CHANNEL"]
        lb_list = dc_cfg.get("lookback_periods", [])
        for lb in lb_list:
            dc_df = calc_donchian_channel(df, lb)
            new_cols = pd.concat([new_cols, dc_df], axis=1)

    # -----------------------------------------------------------------
    # 11) STOCH
    # -----------------------------------------------------------------
    if "STOCH" in cfg:
        stoch_cfg = cfg["STOCH"]
        k_list = stoch_cfg.get("k_period", [])
        d_list = stoch_cfg.get("d_period", [])
        # threshold는 시그널용이므로 계산엔 필요없음
        for k_ in k_list:
            for d_ in d_list:
                stoch_df = calc_stoch(df, k_, d_)
                new_cols = pd.concat([new_cols, stoch_df], axis=1)

    # -----------------------------------------------------------------
    # 12) STOCH_RSI
    # -----------------------------------------------------------------
    if "STOCH_RSI" in cfg:
        srsi_cfg = cfg["STOCH_RSI"]
        rsi_list = srsi_cfg.get("rsi_periods", [])
        stoch_list = srsi_cfg.get("stoch_periods", [])
        k_list = srsi_cfg.get("k_period", [])
        d_list = srsi_cfg.get("d_period", [])
        for r_ in rsi_list:
            for st_ in stoch_list:
                for k_ in k_list:
                    for d_ in d_list:
                        srsi_df = calc_stoch_rsi(
                            df=df,
                            rsi_length=r_,
                            stoch_length=st_,
                            k_period=k_,
                            d_period=d_
                        )
                        new_cols = pd.concat([new_cols, srsi_df], axis=1)

    # -----------------------------------------------------------------
    # 13) MFI
    # -----------------------------------------------------------------
    if "MFI" in cfg:
        mfi_cfg = cfg["MFI"]
        lb_list = mfi_cfg.get("lookback_periods", [])
        # threshold는 시그널용
        for lb in lb_list:
            mfi_sr = calc_mfi(df, lb)
            new_cols[mfi_sr.name] = mfi_sr

    # -----------------------------------------------------------------
    # 14) VWAP
    # -----------------------------------------------------------------
    if "VWAP" in cfg:
        # 별도 파라미터 없음
        vwap_sr = calc_vwap(df)
        if vwap_sr.name not in df.columns:
            new_cols[vwap_sr.name] = vwap_sr

    # df에 new_cols 합치기
    if not new_cols.empty:
        df = pd.concat([df, new_cols], axis=1)

    return df
