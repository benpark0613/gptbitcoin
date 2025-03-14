# strategies/signal_factory.py
"""
여러 지표 파라미터(combo_params)를 합쳐 최종 시그널을 만드는 모듈.
(추세추종, 즉시모드)

config/indicator_config.py 에서 SIGNAL_COMBINE_METHOD 값에 따라
- "sum":  모든 지표 시그널을 합산 후, 양수면 +1, 음수면 -1, 그 외 0
- "and":  모든 지표 시그널이 +1이어야 최종 +1, 모두 -1이면 최종 -1, 그 외 0
로직을 적용한다.
"""

import numpy as np
import pandas as pd
import uuid
from typing import List, Dict, Any

# 기존: from config.config import SIGNAL_COMBINE_METHOD
# -> indicator_config.py에서 설정한다고 했으므로 아래와 같이 변경
from config.indicator_config import SIGNAL_COMBINE_METHOD

# 개선된 벡터화 시그널 로직 (각 지표별 +1/-1/0 생성)
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
    vwap_signal
)


def create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]],
    out_col: str
) -> pd.DataFrame:
    """
    여러 지표 파라미터(combo_params)를 받아, 각 시점별로 시그널(+1/-1/0)을 생성 후
    config.indicator_config.py의 SIGNAL_COMBINE_METHOD에 맞춰 최종 시그널을 만든다.

    Args:
        df (pd.DataFrame): 이미 지표 칼럼이 계산된 DataFrame
        combo_params (List[Dict[str, Any]]): 개별 지표 파라미터들의 리스트
        out_col (str): 최종 시그널을 저장할 컬럼명

    Returns:
        pd.DataFrame: 최종 시그널 컬럼 out_col이 추가된 DataFrame
                      (+1: 매수, -1: 매도, 0: 관망)
    """
    n = len(df)
    if n == 0:
        df[out_col] = []
        return df

    partial_signals_list = []

    # 1) 개별 지표 시그널을 임시 컬럼으로 생성
    for idx, param in enumerate(combo_params):
        ttype = str(param["type"]).upper()
        # 임시 시그널 컬럼명 (UUID 일부 사용)
        temp_signal_col = f"_temp_sig_{ttype}_{uuid.uuid4().hex[:8]}"

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

        elif ttype == "VWAP":
            vwap_c = "vwap"
            price_c = param.get("price_col", "close")
            df = vwap_signal(
                df=df,
                vwap_col=vwap_c,
                price_col=price_c,
                signal_col=temp_signal_col
            )

        # 완성된 임시 시그널 배열 추출 후 컬럼 삭제
        signals_arr = df[temp_signal_col].values
        partial_signals_list.append(signals_arr)
        df.drop(columns=[temp_signal_col], inplace=True, errors="ignore")

    # 2) 여러 지표 시그널 결합 로직
    #    indicator_config.py 내 SIGNAL_COMBINE_METHOD 값에 따라 달라짐
    signals_2d = np.array(partial_signals_list)  # shape = (지표 개수, n)
    final_signals = np.zeros(n, dtype=int)

    # 2-1) "sum" 방식: 합산 후 양수 = +1, 음수 = -1, 그 외 0
    if SIGNAL_COMBINE_METHOD.lower() == "sum":
        sum_signals = np.sum(signals_2d, axis=0)  # (n,)
        final_signals[sum_signals > 0] = 1
        final_signals[sum_signals < 0] = -1

    # 2-2) "and" 방식: 모든 지표가 +1이어야 최종 +1, 모두 -1이어야 최종 -1, 그 외 0
    elif SIGNAL_COMBINE_METHOD.lower() == "and":
        mask_all_buy = np.all(signals_2d == 1, axis=0)
        mask_all_sell = np.all(signals_2d == -1, axis=0)

        final_signals[mask_all_buy] = 1
        final_signals[mask_all_sell] = -1

    # 3) 최종 시그널을 DataFrame에 추가
    df[out_col] = final_signals
    return df
