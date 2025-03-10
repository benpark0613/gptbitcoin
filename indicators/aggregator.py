# gptbitcoin/indicators/aggregator.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 프로젝트에서 "기본 + 모멘텀 + 트렌드 + 피보" 지표 함수를 한곳에서 통합 호출하는 모듈.
# config/indicator_config.py를 자동으로 불러와 df에 각 지표 컬럼을 일괄 생성할 수 있도록 하되,
# 컬럼을 매번 하나씩 추가하지 않고, dict에 모아 두었다가 pd.concat으로 한 번에 추가하여
# DataFrame 조각화(Fragmentation) 경고를 줄인다.

import pandas as pd
from typing import Optional, Dict, List

# config
from config.indicator_config import INDICATOR_CONFIG

# 분할된 지표 계산 모듈들
from .basic_indicators import (
    calc_sma_series,
    calc_ema_series,
    calc_rsi_series,
    calc_obv_series,
    calc_filter_minmax,
    calc_sr_minmax,
    calc_cb_minmax
)
from .momentum_indicators import (
    calc_macd,
    calc_dmi_adx,
    calc_stochastic
)
from .trend_channels import (
    calc_bollinger_bands,
    calc_ichimoku,
    calc_psar,
    calc_supertrend
)
from .fibo_stuff import (
    calc_fibonacci_levels,
    calc_fibonacci_retracement_once
)


def calc_all_indicators(
    df: pd.DataFrame,
    cfg: Optional[Dict] = None
) -> pd.DataFrame:
    """
    df에 다양한 지표 컬럼을 일괄 추가한다.
    cfg가 None이면 config/indicator_config.py의 INDICATOR_CONFIG를 기본 사용.

    단, 매번 df[new_col] = ... 하지 않고,
    dict에 모았다가 pd.concat(axis=1)으로 한 번에 추가하여
    'DataFrame is highly fragmented' 경고를 줄인다.

    Args:
        df (pd.DataFrame): OHLCV 데이터프레임 (open, high, low, close, volume 등 필요)
        cfg (Dict, optional): 각 지표별 파라미터 설정 (None이면 INDICATOR_CONFIG 사용)

    Returns:
        pd.DataFrame: df에 지표 컬럼이 추가된 결과
    """
    if cfg is None:
        cfg = INDICATOR_CONFIG

    required_cols = {"close", "volume"}
    if not required_cols.issubset(df.columns):
        raise ValueError("DataFrame에 'close'와 'volume' 칼럼이 필요합니다.")

    # 결과를 담을 dict
    # ex) new_cols_dict["ma_5"] = 시리즈, ...
    # 마지막에 pd.concat([df, pd.DataFrame(new_cols_dict, ...)], axis=1)
    new_cols_dict = {}

    # ----------------------------------------------------------------
    # 1) Basic Indicators (MA, RSI, OBV, Filter, SR, CB)
    # ----------------------------------------------------------------
    if "MA" in cfg:
        ma_cfg = cfg["MA"]
        sp_list = ma_cfg.get("short_ma_periods", [])
        lp_list = ma_cfg.get("long_ma_periods", [])
        for sp in sp_list:
            col_sp = f"ma_{sp}"
            if col_sp not in df.columns:
                new_cols_dict[col_sp] = calc_sma_series(df["close"], sp)
        for lp in lp_list:
            col_lp = f"ma_{lp}"
            if col_lp not in df.columns:
                new_cols_dict[col_lp] = calc_sma_series(df["close"], lp)

    if "RSI" in cfg:
        rsi_cfg = cfg["RSI"]
        rsi_list = rsi_cfg.get("lookback_periods", [])
        for lb in rsi_list:
            col_rsi = f"rsi_{lb}"
            if col_rsi not in df.columns:
                new_cols_dict[col_rsi] = calc_rsi_series(df["close"], lb)

    if "OBV" in cfg:
        obv_cfg = cfg["OBV"]
        if "obv_raw" not in df.columns:
            # obv_raw 자체도 df에 없으면 추가
            new_cols_dict["obv_raw"] = calc_obv_series(df["close"], df["volume"])
        sp_list = obv_cfg.get("short_ma_periods", [])
        lp_list = obv_cfg.get("long_ma_periods", [])
        # obv_sma
        for p in set(sp_list + lp_list):
            col_obv_sma = f"obv_sma_{p}"
            if col_obv_sma not in df.columns:
                # 기반은 obv_raw (이미 df에 없으면 위에서 new_cols_dict에 추가됨)
                # concat 이후에 df에 생기므로, obv_raw를 df에 있거나 new_cols_dict에 있거나...
                # 일단 safe: 원본이나 new_cols_dict에서 가져온다.
                base_series = df["obv_raw"] if "obv_raw" in df.columns else new_cols_dict["obv_raw"]
                new_cols_dict[col_obv_sma] = base_series.rolling(window=p, min_periods=p).mean()

    if "Filter" in cfg:
        flb_list = cfg["Filter"].get("lookback_periods", [])
        for w in flb_list:
            min_c = f"filter_min_{w}"
            max_c = f"filter_max_{w}"
            # 이미 df나 new_cols_dict에 없으면 생성
            if min_c not in df.columns and min_c not in new_cols_dict:
                df_flt = calc_filter_minmax(df["close"], w)
                for ccol in df_flt.columns:
                    if (ccol not in df.columns) and (ccol not in new_cols_dict):
                        new_cols_dict[ccol] = df_flt[ccol]

    if "SR" in cfg:
        sr_list = cfg["SR"].get("lookback_periods", [])
        for w in sr_list:
            min_c = f"sr_min_{w}"
            max_c = f"sr_max_{w}"
            if (min_c not in df.columns) and (min_c not in new_cols_dict):
                df_sr = calc_sr_minmax(df["close"], w)
                for ccol in df_sr.columns:
                    if (ccol not in df.columns) and (ccol not in new_cols_dict):
                        new_cols_dict[ccol] = df_sr[ccol]

    if "CB" in cfg:
        cb_list = cfg["CB"].get("lookback_periods", [])
        for w in cb_list:
            min_c = f"ch_min_{w}"
            max_c = f"ch_max_{w}"
            if (min_c not in df.columns) and (min_c not in new_cols_dict):
                df_cb = calc_cb_minmax(df["close"], w)
                for ccol in df_cb.columns:
                    if (ccol not in df.columns) and (ccol not in new_cols_dict):
                        new_cols_dict[ccol] = df_cb[ccol]

    # ----------------------------------------------------------------
    # 2) Momentum Indicators (MACD, DMI_ADX, Stochastic)
    # ----------------------------------------------------------------
    if "MACD" in cfg:
        macd_cfg = cfg["MACD"]
        fast_list = macd_cfg.get("fast_periods", [])
        slow_list = macd_cfg.get("slow_periods", [])
        sig_list = macd_cfg.get("signal_periods", [])
        for f in fast_list:
            for s in slow_list:
                if f >= s:
                    continue
                for sg in sig_list:
                    col_line = f"macd_line_{f}_{s}_{sg}"
                    col_sig = f"macd_signal_{f}_{s}_{sg}"
                    col_hist = f"macd_hist_{f}_{s}_{sg}"
                    if col_line not in df.columns and col_line not in new_cols_dict:
                        macd_df = calc_macd(df["close"], f, s, sg)
                        new_cols_dict[col_line] = macd_df["macd_line"]
                        new_cols_dict[col_sig] = macd_df["macd_signal"]
                        new_cols_dict[col_hist] = macd_df["macd_hist"]

    if "DMI_ADX" in cfg:
        dmi_cfg = cfg["DMI_ADX"]
        dmi_ps = dmi_cfg.get("dmi_periods", [])
        for dp in dmi_ps:
            plus_c = f"plus_di_{dp}"
            minus_c = f"minus_di_{dp}"
            adx_c = f"adx_{dp}"
            if (plus_c not in df.columns) and (plus_c not in new_cols_dict):
                dmi_df = calc_dmi_adx(df["high"], df["low"], df["close"], dp)
                new_cols_dict[plus_c] = dmi_df["plus_di"]
                new_cols_dict[minus_c] = dmi_df["minus_di"]
                new_cols_dict[adx_c] = dmi_df["adx"]

    if "STOCH" in cfg:
        st_cfg = cfg["STOCH"]
        k_ps = st_cfg.get("k_periods", [])
        d_ps = st_cfg.get("d_periods", [])
        for k_p in k_ps:
            for d_p in d_ps:
                k_col = f"stoch_k_{k_p}_{d_p}"
                d_col = f"stoch_d_{k_p}_{d_p}"
                if (k_col not in df.columns) and (k_col not in new_cols_dict):
                    st_df = calc_stochastic(df["high"], df["low"], df["close"], k_p, d_p)
                    new_cols_dict[k_col] = st_df["stoch_k"]
                    new_cols_dict[d_col] = st_df["stoch_d"]

    # ----------------------------------------------------------------
    # 3) Trend Channels (Bollinger, Ichimoku, PSAR, SuperTrend)
    # ----------------------------------------------------------------
    if "BOLL" in cfg:
        boll_cfg = cfg["BOLL"]
        lb_list = boll_cfg.get("lookback_periods", [])
        std_list = boll_cfg.get("stddev_multipliers", [])
        for lb in lb_list:
            for sd in std_list:
                mid_c = f"boll_mid_{lb}_{sd}"
                up_c = f"boll_upper_{lb}_{sd}"
                lo_c = f"boll_lower_{lb}_{sd}"
                if (mid_c not in df.columns) and (mid_c not in new_cols_dict):
                    bdf = calc_bollinger_bands(df["close"], lb, sd)
                    new_cols_dict[mid_c] = bdf["boll_mid"]
                    new_cols_dict[up_c] = bdf["boll_upper"]
                    new_cols_dict[lo_c] = bdf["boll_lower"]

    if "ICHIMOKU" in cfg:
        ich_cfg = cfg["ICHIMOKU"]
        tenkans = ich_cfg.get("tenkan_period", [])
        kijuns = ich_cfg.get("kijun_period", [])
        spans = ich_cfg.get("senkou_span_b_period", [])
        for t in tenkans:
            for k in kijuns:
                for sp in spans:
                    prefix = f"ich_{t}_{k}_{sp}"
                    ten_col = f"{prefix}_tenkan"
                    kij_col = f"{prefix}_kijun"
                    spa_col = f"{prefix}_span_a"
                    spb_col = f"{prefix}_span_b"
                    chi_col = f"{prefix}_chikou"
                    if (ten_col not in df.columns) and (ten_col not in new_cols_dict):
                        ich_df = calc_ichimoku(df["high"], df["low"], t, k, sp)
                        new_cols_dict[ten_col] = ich_df["ichimoku_tenkan"]
                        new_cols_dict[kij_col] = ich_df["ichimoku_kijun"]
                        new_cols_dict[spa_col] = ich_df["ichimoku_span_a"]
                        new_cols_dict[spb_col] = ich_df["ichimoku_span_b"]
                        new_cols_dict[chi_col] = ich_df["ichimoku_chikou"]

    if "PSAR" in cfg:
        psar_cfg = cfg["PSAR"]
        steps = psar_cfg.get("acceleration_step", [])
        maxes = psar_cfg.get("acceleration_max", [])
        for stp in steps:
            for mx in maxes:
                psar_col = f"psar_{stp}_{mx}"
                if (psar_col not in df.columns) and (psar_col not in new_cols_dict):
                    new_cols_dict[psar_col] = calc_psar(df["high"], df["low"], stp, mx)

    if "SUPERTREND" in cfg:
        st_cfg = cfg["SUPERTREND"]
        atr_ps = st_cfg.get("atr_period", [])
        mults = st_cfg.get("multiplier", [])
        for ap in atr_ps:
            for mt in mults:
                st_col = f"supertrend_{ap}_{mt}"
                if (st_col not in df.columns) and (st_col not in new_cols_dict):
                    new_cols_dict[st_col] = calc_supertrend(df["high"], df["low"], df["close"], ap, mt)

    # ----------------------------------------------------------------
    # 4) Fibonacci Stuff
    # ----------------------------------------------------------------
    if "FIBO" in cfg:
        fibo_cfg = cfg["FIBO"]
        level_sets = fibo_cfg.get("levels", [])
        mode = fibo_cfg.get("mode", "rolling")  # 예: "rolling"/"cumulative"/"latest"
        for i, lvset in enumerate(level_sets):
            fib_df = calc_fibonacci_levels(df["high"], df["low"], lvset, mode=mode)
            for col in fib_df.columns:
                new_col = f"{col}_set{i+1}"
                if (new_col not in df.columns) and (new_col not in new_cols_dict):
                    new_cols_dict[new_col] = fib_df[col]

    # 이제 모아둔 new_cols_dict를 한 번에 df에 concat
    if new_cols_dict:
        new_df = pd.DataFrame(new_cols_dict, index=df.index)
        df = pd.concat([df, new_df], axis=1)

    return df
