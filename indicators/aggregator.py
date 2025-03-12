# gptbitcoin/indicators/aggregator.py
# config 설정에 맞춰 모든 지표를 계산하고 DataFrame에 추가한다.

from typing import Optional, Dict
import pandas as pd

from config.indicator_config import INDICATOR_CONFIG
from .basic_indicators import (
    calc_sma_series, calc_rsi_series, calc_obv_series,
    calc_filter_minmax, calc_sr_minmax, calc_cb_minmax
)
from .fibo_stuff import calc_fibonacci_levels
from .momentum_indicators import calc_macd, calc_dmi_adx
from .trend_channels import (
    calc_bollinger_bands, calc_ichimoku, calc_psar, calc_supertrend
)


def calc_all_indicators(df: pd.DataFrame, cfg: Optional[Dict] = None) -> pd.DataFrame:
    """config에 정의된 지표별 설정으로 각종 보조지표를 계산해 df에 컬럼으로 추가한다."""
    if cfg is None:
        cfg = INDICATOR_CONFIG

    new_cols = {}

    # MA
    if "MA" in cfg:
        sp_list = cfg["MA"].get("short_ma_periods", [])
        lp_list = cfg["MA"].get("long_ma_periods", [])
        for sp in sp_list:
            csp = f"ma_{sp}"
            if csp not in df.columns and csp not in new_cols:
                new_cols[csp] = calc_sma_series(df["close"], sp)
        for lp in lp_list:
            clp = f"ma_{lp}"
            if clp not in df.columns and clp not in new_cols:
                new_cols[clp] = calc_sma_series(df["close"], lp)

    # RSI
    if "RSI" in cfg:
        rsi_list = cfg["RSI"].get("lookback_periods", [])
        for rsi_p in rsi_list:
            col_rsi = f"rsi_{rsi_p}"
            if col_rsi not in df.columns and col_rsi not in new_cols:
                new_cols[col_rsi] = calc_rsi_series(df["close"], rsi_p)

    # OBV
    if "OBV" in cfg:
        if "obv_raw" not in df.columns and "obv_raw" not in new_cols:
            new_cols["obv_raw"] = calc_obv_series(df["close"], df["volume"])
        sp_list = cfg["OBV"].get("short_ma_periods", [])
        lp_list = cfg["OBV"].get("long_ma_periods", [])
        for p in set(sp_list + lp_list):
            col_obv_sma = f"obv_sma_{p}"
            if col_obv_sma not in df.columns and col_obv_sma not in new_cols:
                base = df["obv_raw"] if "obv_raw" in df.columns else new_cols["obv_raw"]
                new_cols[col_obv_sma] = base.rolling(window=p, min_periods=p).mean()

    # Filter
    if "Filter" in cfg:
        flb = cfg["Filter"].get("lookback_periods", [])
        for w in flb:
            minc = f"filter_min_{w}"
            maxc = f"filter_max_{w}"
            if minc not in df.columns and minc not in new_cols:
                fdf = calc_filter_minmax(df["close"], w)
                for ccol in fdf.columns:
                    if ccol not in df.columns and ccol not in new_cols:
                        new_cols[ccol] = fdf[ccol]

    # SR
    if "SR" in cfg:
        sr_list = cfg["SR"].get("lookback_periods", [])
        for w in sr_list:
            mn = f"sr_min_{w}"
            mx = f"sr_max_{w}"
            if mn not in df.columns and mn not in new_cols:
                sdf = calc_sr_minmax(df["close"], w)
                for ccol in sdf.columns:
                    if ccol not in df.columns and ccol not in new_cols:
                        new_cols[ccol] = sdf[ccol]

    # CB
    if "CB" in cfg:
        cb_list = cfg["CB"].get("lookback_periods", [])
        for w in cb_list:
            mn = f"ch_min_{w}"
            mx = f"ch_max_{w}"
            if mn not in df.columns and mn not in new_cols:
                cdf = calc_cb_minmax(df["close"], w)
                for ccol in cdf.columns:
                    if ccol not in df.columns and ccol not in new_cols:
                        new_cols[ccol] = cdf[ccol]

    # MACD
    if "MACD" in cfg:
        fasts = cfg["MACD"].get("fast_periods", [])
        slows = cfg["MACD"].get("slow_periods", [])
        sigs = cfg["MACD"].get("signal_periods", [])
        for f in fasts:
            for s in slows:
                if f >= s:
                    continue
                for sg in sigs:
                    ln = f"macd_line_{f}_{s}_{sg}"
                    sn = f"macd_signal_{f}_{s}_{sg}"
                    hn = f"macd_hist_{f}_{s}_{sg}"
                    if ln not in df.columns and ln not in new_cols:
                        mdf = calc_macd(df["close"], f, s, sg)
                        new_cols[ln] = mdf["macd_line"]
                        new_cols[sn] = mdf["macd_signal"]
                        new_cols[hn] = mdf["macd_hist"]

    # DMI_ADX
    if "DMI_ADX" in cfg:
        dps = cfg["DMI_ADX"].get("dmi_periods", [])
        for dp in dps:
            pdp = f"plus_di_{dp}"
            mdp = f"minus_di_{dp}"
            adp = f"adx_{dp}"
            if pdp not in df.columns and pdp not in new_cols:
                ddf = calc_dmi_adx(df["high"], df["low"], df["close"], dp)
                new_cols[pdp] = ddf["plus_di"]
                new_cols[mdp] = ddf["minus_di"]
                new_cols[adp] = ddf["adx"]

    # BOLL
    if "BOLL" in cfg:
        lb_list = cfg["BOLL"].get("lookback_periods", [])
        std_list = cfg["BOLL"].get("stddev_multipliers", [])
        for lb in lb_list:
            for sd in std_list:
                midc = f"boll_mid_{lb}_{sd}"
                upc = f"boll_upper_{lb}_{sd}"
                loc = f"boll_lower_{lb}_{sd}"
                if midc not in df.columns and midc not in new_cols:
                    bdf = calc_bollinger_bands(df["close"], lb, sd)
                    new_cols[midc] = bdf["boll_mid"]
                    new_cols[upc] = bdf["boll_upper"]
                    new_cols[loc] = bdf["boll_lower"]

    # ICHIMOKU
    if "ICHIMOKU" in cfg:
        tenkans = cfg["ICHIMOKU"].get("tenkan_period", [])
        kijuns = cfg["ICHIMOKU"].get("kijun_period", [])
        spans = cfg["ICHIMOKU"].get("senkou_span_b_period", [])
        for t in tenkans:
            for k in kijuns:
                for sp in spans:
                    prefix = f"ich_{t}_{k}_{sp}"
                    ten_col = f"{prefix}_tenkan"
                    kij_col = f"{prefix}_kijun"
                    spa_col = f"{prefix}_span_a"
                    spb_col = f"{prefix}_span_b"
                    chi_col = f"{prefix}_chikou"
                    if ten_col not in df.columns and ten_col not in new_cols:
                        ich_df = calc_ichimoku(df["high"], df["low"], df["close"], t, k, sp)
                        new_cols[ten_col] = ich_df["ichimoku_tenkan"]
                        new_cols[kij_col] = ich_df["ichimoku_kijun"]
                        new_cols[spa_col] = ich_df["ichimoku_span_a"]
                        new_cols[spb_col] = ich_df["ichimoku_span_b"]
                        new_cols[chi_col] = ich_df["ichimoku_chikou"]

    # PSAR
    if "PSAR" in cfg:
        steps = cfg["PSAR"].get("acceleration_step", [])
        maxes = cfg["PSAR"].get("acceleration_max", [])
        for stp in steps:
            for mx in maxes:
                pcol = f"psar_{stp}_{mx}"
                if pcol not in df.columns and pcol not in new_cols:
                    new_cols[pcol] = calc_psar(
                        df["high"], df["low"], df["close"], stp, mx
                    )

    # SUPERTREND
    if "SUPERTREND" in cfg:
        aps = cfg["SUPERTREND"].get("atr_period", [])
        mts = cfg["SUPERTREND"].get("multiplier", [])
        for ap in aps:
            for mt in mts:
                stcol = f"supertrend_{ap}_{mt}"
                if stcol not in df.columns and stcol not in new_cols:
                    new_cols[stcol] = calc_supertrend(df["high"], df["low"], df["close"], ap, mt)

    # FIBO
    if "FIBO" in cfg:
        fib_cfg = cfg["FIBO"]
        level_sets = fib_cfg.get("levels", [])
        roll_win = fib_cfg.get("rolling_window", 20)
        for i, lvset in enumerate(level_sets):
            fib_df = calc_fibonacci_levels(
                df["high"], df["low"], lvset, rolling_window=roll_win
            )
            for ccol in fib_df.columns:
                new_col = f"{ccol}_set{i+1}"
                if new_col not in df.columns and new_col not in new_cols:
                    new_cols[new_col] = fib_df[ccol]

    if new_cols:
        df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

    return df
