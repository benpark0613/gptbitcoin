# gptbitcoin/strategies/signal_factory.py
# 구글 스타일 Docstring, 주석은 최소한의 한글
# indicator_config.py에서 정의한 지표 타입만 처리한다 (MA, RSI, OBV, MACD, DMI_ADX, BOLL,
# ICHIMOKU, PSAR, SUPERTREND, DONCHIAN_CHANNEL, STOCH, STOCH_RSI, MFI, VWAP)

import numpy as np
import pandas as pd
from typing import List, Dict, Any

# 수정된 signal_logic.py에서 필요한 함수 임포트
from .signal_logic import (
    ma_crossover_signal,
    rsi_signal,
    obv_signal,
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
    여러 지표 파라미터(combo_params)에 기반해 각 시점별 매수/매도/관망 시그널(+1/-1/0)을 생성한다.
    모든 부분 시그널을 합산해 최종 시그널을 만든 후 df[out_col]에 저장.

    Args:
        df (pd.DataFrame): 필요한 지표 칼럼이 있는 데이터프레임
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

        # 임시 시그널 컬럼 (함수 내에서 사용)
        temp_signal_col = f"_{ttype}_temp_sig"

        if ttype == "MA":
            # 이동평균 교차 시그널
            short_p = param["short_period"]
            long_p = param["long_period"]
            short_col = f"MA_short_{short_p}"
            long_col = f"MA_long_{long_p}"

            df = ma_crossover_signal(
                df=df,
                short_ma_col=short_col,
                long_ma_col=long_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "RSI":
            # RSI 시그널
            lb = param["lookback"]
            overbought = param["overbought"]
            oversold = param["oversold"]
            rsi_col = f"RSI_{lb}"

            df = rsi_signal(
                df=df,
                rsi_col=rsi_col,
                lower_bound=oversold,
                upper_bound=overbought,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "OBV":
            # OBV 시그널
            # 예: OBV 절대 임계값( threshold ) 활용
            obv_col = "OBV"
            threshold = param["threshold_percentile_value"]  # 가정: 백분위 등으로 계산된 임계값
            df = obv_signal(
                df=df,
                obv_col=obv_col,
                threshold=threshold,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "MACD":
            # MACD 시그널
            f_per = param["fast_period"]
            s_per = param["slow_period"]
            sig_per = param["signal_period"]
            macd_line_col = f"MACD_{f_per}_{s_per}_{sig_per}"
            macd_signal_col = f"MACDs_{f_per}_{s_per}_{sig_per}"

            df = macd_signal(
                df=df,
                macd_line_col=macd_line_col,
                macd_signal_col=macd_signal_col,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "DMI_ADX":
            # DMI_ADX 시그널
            dmi_period = param["lookback"]
            adx_th = param["adx_threshold"]
            plus_col = f"DMP_{dmi_period}"
            minus_col = f"DMN_{dmi_period}"
            adx_col = f"ADX_{dmi_period}"

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
            # 볼린저 밴드 시그널
            lb = param["lookback"]
            sd = param["stddev_mult"]
            price_c = param["price_col"]

            lower_col = f"BOLL_L_{lb}_{sd}"
            mid_col = f"BOLL_M_{lb}_{sd}"
            upper_col = f"BOLL_U_{lb}_{sd}"

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
            # 일목균형표 시그널
            tenkan = param["tenkan_period"]
            kijun = param["kijun_period"]
            span_b = param["senkou_span_b_period"]
            price_c = param["price_col"]

            # aggregator에서 만든 칼럼명 예시
            its_col = f"ITS_{tenkan}_{kijun}_{span_b}"
            iks_col = f"IKS_{tenkan}_{kijun}_{span_b}"
            isa_col = f"ISA_{tenkan}_{kijun}_{span_b}"
            isb_col = f"ISB_{tenkan}_{kijun}_{span_b}"

            df = ichimoku_signal_trend(
                df=df,
                tenkan_col=its_col,
                kijun_col=iks_col,
                span_a_col=isa_col,
                span_b_col=isb_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "PSAR":
            # PSAR 시그널
            stp = param["acceleration_step"]
            mx = param["acceleration_max"]
            price_c = param["price_col"]

            psar_col = f"PSAR_{stp}_{mx}"

            df = psar_signal(
                df=df,
                psar_col=psar_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "SUPERTREND":
            # 슈퍼트렌드
            ap = param["atr_period"]
            mt = param["multiplier"]
            price_c = param["price_col"]

            st_col = f"SUPERT_{ap}_{mt}"

            df = supertrend_signal(
                df=df,
                st_col=st_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "DONCHIAN_CHANNEL":
            # 돈채널 시그널
            lb = param["lookback"]
            price_c = param["price_col"]

            lower_col = f"DCL_{lb}"
            upper_col = f"DCU_{lb}"

            df = donchian_signal(
                df=df,
                lower_col=lower_col,
                upper_col=upper_col,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "STOCH":
            # 스토캐스틱 시그널
            k_per = param["k_period"]
            d_per = param["d_period"]
            thr_low = param["oversold"]
            thr_high = param["overbought"]

            k_col = f"STOCHk_{k_per}_{d_per}"
            d_col = f"STOCHd_{k_per}_{d_per}"

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
            # 스토캐스틱 RSI 시그널
            lb = param["lookback"]
            k_ = param["k_period"]
            d_ = param["d_period"]
            thr_low = param["oversold"]
            thr_high = param["overbought"]

            k_col = f"STOCH_RSIk_{lb}_{k_}_{d_}"
            d_col = f"STOCH_RSId_{lb}_{k_}_{d_}"

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
            # MFI 시그널
            lb = param["lookback"]
            thr_low = param["oversold"]
            thr_high = param["overbought"]
            mfi_col = f"MFI_{lb}"

            df = mfi_signal(
                df=df,
                mfi_col=mfi_col,
                lower_threshold=thr_low,
                upper_threshold=thr_high,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        elif ttype == "VWAP":
            # VWAP 시그널
            price_c = param["price_col"]
            vwap_c = "VWAP"

            df = vwap_signal(
                df=df,
                vwap_col=vwap_c,
                price_col=price_c,
                signal_col=temp_signal_col
            )
            signals_arr = df[temp_signal_col].values

        # 임시 시그널 컬럼 제거 후 리스트에 추가
        partial_signals_list.append(signals_arr)
        if temp_signal_col in df.columns:
            df.drop(columns=[temp_signal_col], inplace=True)

    # 부분 시그널 합산
    sum_signals = np.zeros(n, dtype=int)
    for arr in partial_signals_list:
        sum_signals += arr

    # 최종 시그널: 합이 양수면 +1, 음수면 -1, 그 외 0
    final_signals = np.where(sum_signals > 0, 1, np.where(sum_signals < 0, -1, 0))
    df[out_col] = final_signals

    return df
