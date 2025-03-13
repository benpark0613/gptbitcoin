# gptbitcoin/strategies/signal_factory.py
"""
여러 지표 파라미터를 조합하여 시그널을 합산 후 최종 시그널을 생성하는 모듈.
(추세추종 로직 기반, 즉시모드 신호함수 사용)

구글 스타일 Docstring, 필요한 최소한의 한글 주석만 추가.
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any

# 즉시모드로 수정된 signal_logic.py 임포트
from .signal_logic import (
    ma_crossover_signal,
    obv_ma_signal,
    rsi_signal,
    macd_signal,
    dmi_adx_signal_trend,
    bollinger_signal,
    ichimoku_signal_trend,
    psar_signal,
    supertrend_signal,
    donchian_signal,
    stoch_signal,
    stoch_rsi_signal,
    mfi_signal,
    vwap_signal
)


def create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]],
    out_col: str
) -> pd.DataFrame:
    """
    여러 지표 파라미터(combo_params)를 받아, 각 시점별 매수/매도/관망 시그널(+1/-1/0)을 생성한다.
    지표별 시그널을 모두 합산하여 최종 시그널을 만든 뒤 df[out_col]에 저장한다.
    (즉시모드로 수정된 signal_logic.py의 함수를 이용)

    Args:
        df (pd.DataFrame): 지표 칼럼이 포함된 DataFrame
        combo_params (List[Dict[str, Any]]): 지표 파라미터(dict)들의 리스트
        out_col (str): 최종 시그널을 저장할 컬럼명

    Returns:
        pd.DataFrame: 최종 시그널 컬럼 out_col이 추가된 DataFrame
    """
    n = len(df)
    if n == 0:
        df[out_col] = []
        return df

    partial_signals_list = []

    for param in combo_params:
        ttype = str(param["type"]).upper()
        signals_arr = np.zeros(n, dtype=int)

        # 임시 시그널 컬럼명
        temp_signal_col = f"_{ttype}_temp_sig"

        if ttype == "MA":
            short_p = param["short_period"]
            long_p = param["long_period"]
            short_col = f"ma_{short_p}"
            long_col = f"ma_{long_p}"

            df = ma_crossover_signal(
                df=df,
                short_ma_col=short_col,
                long_ma_col=long_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "OBV":
            sp = param["short_period"]
            lp = param["long_period"]
            obv_short_col = f"obv_sma_{sp}"
            obv_long_col = f"obv_sma_{lp}"

            df = obv_ma_signal(
                df=df,
                obv_short_col=obv_short_col,
                obv_long_col=obv_long_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "RSI":
            lb = param["lookback"]
            overbought = param["overbought"]
            oversold = param["oversold"]
            rsi_col = f"rsi_{lb}"

            df = rsi_signal(
                df=df,
                rsi_col=rsi_col,
                lower_bound=oversold,
                upper_bound=overbought,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "MACD":
            f_per = param["fast_period"]
            s_per = param["slow_period"]
            sig_per = param["signal_period"]
            macd_line_col = f"macd_line_{f_per}_{s_per}_{sig_per}"
            macd_signal_col = f"macd_signal_{f_per}_{s_per}_{sig_per}"

            df = macd_signal(
                df=df,
                macd_line_col=macd_line_col,
                macd_signal_col=macd_signal_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "DMI_ADX":
            dmi_period = param["lookback"]
            adx_th = param["adx_threshold"]
            plus_col = f"plus_di_{dmi_period}"
            minus_col = f"minus_di_{dmi_period}"
            adx_col = f"adx_{dmi_period}"

            df = dmi_adx_signal_trend(
                df=df,
                plus_di_col=plus_col,
                minus_di_col=minus_col,
                adx_col=adx_col,
                adx_threshold=adx_th,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "BOLL":
            lb = param["lookback"]
            sd = param["stddev_mult"]
            price_c = param.get("price_col", "close")

            mid_col = f"boll_mid_{lb}_{sd}"
            upper_col = f"boll_upper_{lb}_{sd}"
            lower_col = f"boll_lower_{lb}_{sd}"

            df = bollinger_signal(
                df=df,
                mid_col=mid_col,
                upper_col=upper_col,
                lower_col=lower_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "ICHIMOKU":
            t = param["tenkan_period"]
            k = param["kijun_period"]
            sp = param["senkou_span_b_period"]
            price_c = param.get("price_col", "close")

            tenkan_col = f"ich_{t}_{k}_{sp}_tenkan"
            kijun_col = f"ich_{t}_{k}_{sp}_kijun"
            span_a_col = f"ich_{t}_{k}_{sp}_span_a"
            span_b_col = f"ich_{t}_{k}_{sp}_span_b"

            df = ichimoku_signal_trend(
                df=df,
                tenkan_col=tenkan_col,
                kijun_col=kijun_col,
                span_a_col=span_a_col,
                span_b_col=span_b_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "PSAR":
            stp = param["acceleration_step"]
            mx = param["acceleration_max"]
            price_c = param.get("price_col", "close")
            psar_col = f"psar_{stp}_{mx}"

            df = psar_signal(
                df=df,
                psar_col=psar_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "SUPERTREND":
            ap = param["atr_period"]
            mt = param["multiplier"]
            price_c = param.get("price_col", "close")
            st_col = f"supertrend_{ap}_{mt}"

            df = supertrend_signal(
                df=df,
                st_col=st_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "DONCHIAN_CHANNEL":
            lb = param["lookback"]
            price_c = param.get("price_col", "close")
            lower_col = f"dcl_{lb}"
            upper_col = f"dcu_{lb}"

            df = donchian_signal(
                df=df,
                lower_col=lower_col,
                upper_col=upper_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "STOCH":
            k_per = param["k_period"]
            d_per = param["d_period"]
            thr_low = param["oversold"]
            thr_high = param["overbought"]

            k_col = f"stoch_k_{k_per}_{d_per}"
            d_col = f"stoch_d_{k_per}_{d_per}"

            df = stoch_signal(
                df=df,
                stoch_k_col=k_col,
                stoch_d_col=d_col,
                lower_threshold=thr_low,
                upper_threshold=thr_high,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "STOCH_RSI":
            rsi_len = param["rsi_length"]
            st_len = param["stoch_length"]
            k_ = param["k_period"]
            d_ = param["d_period"]
            thr_low = param["oversold"]
            thr_high = param["overbought"]

            k_col = f"stoch_rsi_k_{rsi_len}_{st_len}_{k_}_{d_}"
            d_col = f"stoch_rsi_d_{rsi_len}_{st_len}_{k_}_{d_}"

            df = stoch_rsi_signal(
                df=df,
                k_col=k_col,
                d_col=d_col,
                lower_threshold=thr_low,
                upper_threshold=thr_high,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "MFI":
            lb = param["lookback"]
            thr_low = param["oversold"]
            thr_high = param["overbought"]
            mfi_col = f"mfi_{lb}"

            df = mfi_signal(
                df=df,
                mfi_col=mfi_col,
                lower_threshold=thr_low,
                upper_threshold=thr_high,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "VWAP":
            vwap_c = "vwap"
            price_c = param.get("price_col", "close")

            df = vwap_signal(
                df=df,
                vwap_col=vwap_c,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        # 임시 시그널 배열을 저장해두고, 임시 컬럼 제거
        partial_signals_list.append(signals_arr)
        if temp_signal_col in df.columns:
            df.drop(columns=[temp_signal_col], inplace=True)

    # 여러 지표의 부분 시그널들을 합산
    sum_signals = np.zeros(n, dtype=int)
    for arr in partial_signals_list:
        sum_signals += arr

    # 합산값이 양수면 +1, 음수면 -1, 그 외 0
    final_signals = np.where(sum_signals > 0, 1, np.where(sum_signals < 0, -1, 0))
    df[out_col] = final_signals

    return df
