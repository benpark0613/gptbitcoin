# gptbitcoin/strategies/signal_factory.py
# 주석은 한글로 작성 (필요 최소한만), 구글 스타일 Docstring
# 퍼센트(%) 값을 0.05라면 5%로 해석한다는 점을 전제로,
# band_filter, buy_filter, c_channel 등의 파라미터를 그대로 사용한다.
# (예: band_filter=0.05 → 5%, band_filter=0.0 → 0%)
# fibo_signal_trend에서 mode 파라미터는 사용하지 않는다(삭제).

import numpy as np
import pandas as pd
from typing import List, Dict, Any

# signal_logic.py의 함수를 import
from .signal_logic import (
    ma_crossover_signal,
    rsi_signal,
    obv_signal,
    filter_rule_signal,
    support_resistance_signal,
    channel_breakout_signal,
    macd_signal,
    dmi_adx_signal_trend,
    bollinger_signal,
    ichimoku_signal_trend,
    psar_signal,
    supertrend_signal,
    fibo_signal_trend
)


def create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]],
    out_col: str = "signal_final"
) -> pd.DataFrame:
    """
    여러 지표 파라미터(combo_params)에 기반하여, 각 시점별 매수/매도/관망 시그널(+1/-1/0)을 생성한다.
    각 콤보별로 부분 시그널을 만든 뒤 합산하여 최종 시그널을 도출한다.

    Args:
        df (pd.DataFrame): 'open_time' 및 필요한 지표 칼럼이 들어있는 DF
        combo_params (List[Dict[str, Any]]): 지표별 파라미터 딕셔너리들의 리스트
        out_col (str): 최종 시그널을 기록할 칼럼명

    Returns:
        pd.DataFrame: df에 out_col 칼럼을 추가(또는 갱신)하여 반환
                      각 행은 +1(매수), -1(매도), 0(관망) 시그널
    """
    n = len(df)
    if n == 0:
        df[out_col] = []
        return df

    # 부분 시그널(+1/-1/0)들을 모아둘 배열 리스트
    partial_signals_list = []

    # 콤보에 포함된 각 지표 파라미터에 대해 시그널 생성
    for param in combo_params:
        signals_arr = np.zeros(n, dtype=int)  # 기본 0 시그널
        ttype = str(param.get("type", "")).upper()

        # 임시 시그널 컬럼명
        temp_signal_col = f"_{ttype}_temp_sig"

        # 지표 타입별로 signal_logic 함수 호출
        if ttype == "MA":
            sp = param["short_period"]
            lp = param["long_period"]
            bf = param.get("band_filter", 0.0)
            short_col = f"ma_{sp}"
            long_col = f"ma_{lp}"
            df = ma_crossover_signal(
                df=df,
                short_ma_col=short_col,
                long_ma_col=long_col,
                band_filter=bf,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "RSI":
            lb = param["lookback"]
            overbought = param.get("overbought", 70.0)
            oversold = param.get("oversold", 30.0)
            rsi_col = f"rsi_{lb}"
            df = rsi_signal(
                df=df,
                rsi_col=rsi_col,
                lower_bound=oversold,
                upper_bound=overbought,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "OBV":
            obv_col = param.get("obv_col", "obv_raw")
            threshold = param.get("band_filter", 0.0)
            df = obv_signal(
                df=df,
                obv_col=obv_col,
                threshold=threshold,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "FILTER":
            w = param["lookback"]
            x_pct = param["buy_filter"]
            y_pct = param["sell_filter"]
            close_col = param.get("close_col", "close")
            df = filter_rule_signal(
                df=df,
                close_col=close_col,
                window=w,
                x_pct=x_pct,
                y_pct=y_pct,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "SR":
            lb = param["lookback"]
            band_pct = param.get("band_filter", 0.0)
            min_col = f"sr_min_{lb}"
            max_col = f"sr_max_{lb}"
            price_col = param.get("price_col", "close")
            df = support_resistance_signal(
                df=df,
                rolling_min_col=min_col,
                rolling_max_col=max_col,
                price_col=price_col,
                band_pct=band_pct,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "CB":
            lb = param["lookback"]
            breakout_pct = param["c_channel"]
            min_col = f"ch_min_{lb}"
            max_col = f"ch_max_{lb}"
            price_col = param.get("price_col", "close")
            df = channel_breakout_signal(
                df=df,
                rolling_min_col=min_col,
                rolling_max_col=max_col,
                price_col=price_col,
                breakout_pct=breakout_pct,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "MACD":
            f_per = param["fast_period"]
            s_per = param["slow_period"]
            sig_per = param["signal_period"]
            macd_line_col = f"macd_line_{f_per}_{s_per}_{sig_per}"
            macd_sig_col = f"macd_signal_{f_per}_{s_per}_{sig_per}"
            df = macd_signal(
                df=df,
                macd_line_col=macd_line_col,
                macd_signal_col=macd_sig_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "DMI_ADX":
            dmi_period = param["dmi_period"]
            adx_th = param.get("adx_threshold", 25.0)
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
            mid_col = f"boll_mid_{lb}_{sd}"
            up_col = f"boll_upper_{lb}_{sd}"
            lo_col = f"boll_lower_{lb}_{sd}"
            price_c = param.get("price_col", "close")
            df = bollinger_signal(
                df=df,
                mid_col=mid_col,
                upper_col=up_col,
                lower_col=lo_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "ICHIMOKU":
            t = param["tenkan_period"]
            k = param["kijun_period"]
            s = param["senkou_span_b_period"]
            prefix = f"ich_{t}_{k}_{s}"
            ten_col = f"{prefix}_tenkan"
            kij_col = f"{prefix}_kijun"
            spa_col = f"{prefix}_span_a"
            spb_col = f"{prefix}_span_b"
            price_col = param.get("price_col", "close")
            df = ichimoku_signal_trend(
                df=df,
                tenkan_col=ten_col,
                kijun_col=kij_col,
                span_a_col=spa_col,
                span_b_col=spb_col,
                price_col=price_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "PSAR":
            stp = param["acc_step"]
            mx = param["acc_max"]
            psar_col = f"psar_{stp}_{mx}"
            price_col = param.get("price_col", "close")
            df = psar_signal(
                df=df,
                psar_col=psar_col,
                price_col=price_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "SUPERTREND":
            ap = param["atr_period"]
            mt = param["multiplier"]
            st_col = f"supertrend_{ap}_{mt}"
            price_c = param.get("price_col", "close")
            df = supertrend_signal(
                df=df,
                st_col=st_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "FIBO":
            fibo_cols = param.get("fibo_cols", [])
            price_col = param.get("price_col", "close")
            df = fibo_signal_trend(
                df=df,
                fibo_cols=fibo_cols,
                price_col=price_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        # 시그널 배열 모음에 추가
        partial_signals_list.append(signals_arr)

        # 임시 시그널 컬럼 제거
        df.drop(columns=[temp_signal_col], inplace=True)

    # 여러 부분 시그널을 합산해 최종 시그널 산출
    sum_signals = np.zeros(n, dtype=int)
    for arr in partial_signals_list:
        sum_signals += arr

    # 합산 결과가 양수면 +1, 음수면 -1, 아니면 0
    final_signals = np.where(sum_signals > 0, 1, np.where(sum_signals < 0, -1, 0))
    df[out_col] = final_signals
    return df
