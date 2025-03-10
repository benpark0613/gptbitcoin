# gptbitcoin/strategies/signal_factory.py
# 최소한의 한글 주석, 구글 스타일 docstring
# numpy 배열을 사용해 시그널(매수/매도/관망)을 계산하는 모듈.
# 데이터프레임 칼럼(df["ma_10"] 등)에서 numpy array를 추출해,
# 콤보(지표 파라미터)별 로직을 벡터 연산으로 수행하여 시간을 절약한다.

import numpy as np
import pandas as pd
from typing import List, Dict, Any


def create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]],
    out_col: str = "signal_final"
) -> pd.DataFrame:
    """
    여러 지표 파라미터(콤보)를 받아 각 지표별로 시그널(+1/-1/0)을 numpy 배열로 계산하고,
    이를 합산(결합)하여 최종 시그널을 out_col에 기록한다.

    지표(예: ma_10, rsi_14, macd_line_12_26_9 등)는 이미 aggregator에서 df에 칼럼으로 준비됐다고 가정.
    여기서는 pandas 할당 대신, numpy 배열 연산을 활용해 속도를 높인다.

    Args:
        df (pd.DataFrame): 'open_time' 및 각종 지표 칼럼이 이미 포함된 DF
                           (예: "ma_10", "ma_20", "rsi_14" 등).
        combo_params (List[Dict[str, Any]]): 지표 파라미터 dict의 리스트.
            예시:
            [
              {
                "type": "MA",
                "short_period": 5,
                "long_period": 20,
                "band_filter": 0.01
              },
              {
                "type": "RSI",
                "lookback": 14,
                "overbought": 70,
                "oversold": 30
              },
              ...
            ]
        out_col (str): 최종 시그널을 기록할 칼럼명 (기본값: "signal_final")

    Returns:
        pd.DataFrame: df에 out_col 칼럼을 추가(또는 갱신)하여 반환.
                      각 행이 +1(매수), -1(매도), 0(관망) 시그널.
    """
    n = len(df)
    if n == 0:
        df[out_col] = []
        return df

    # 최종 시그널: 여러 파라미터의 시그널을 합산 후 +1/-1/0으로 결합
    # (partial_signals_list는 n x (#combo_params) 형태)
    partial_signals_list = []

    for i, param in enumerate(combo_params):
        # 콤보별 시그널 배열 (초기값 0)
        signals_arr = np.zeros(n, dtype=int)
        ttype = str(param.get("type", "")).upper()

        if ttype == "MA":
            # 이동평균 교차
            sp = param["short_period"]
            lp = param["long_period"]
            bf = param.get("band_filter", 0.0)  # 예: 0.01이면 1%
            short_col = f"ma_{sp}"
            long_col = f"ma_{lp}"

            if short_col not in df.columns or long_col not in df.columns:
                # 지표 칼럼이 없으면 스킵(또는 전부 0)
                partial_signals_list.append(signals_arr)
                continue

            short_arr = df[short_col].values
            long_arr = df[long_col].values

            # up_cond: short > long*(1+bf)
            up_mask = short_arr > long_arr * (1.0 + bf)
            # down_cond: short < long*(1-bf)
            down_mask = short_arr < long_arr * (1.0 - bf)

            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        elif ttype == "RSI":
            # RSI 기준 시그널
            lb = param["lookback"]
            overbought = param.get("overbought", 70.0)
            oversold = param.get("oversold", 30.0)
            rsi_col = f"rsi_{lb}"

            if rsi_col not in df.columns:
                partial_signals_list.append(signals_arr)
                continue

            rsi_arr = df[rsi_col].values

            # rsi < oversold → +1, rsi > overbought → -1
            signals_arr[rsi_arr < oversold] = 1
            signals_arr[rsi_arr > overbought] = -1

        elif ttype == "OBV":
            # OBV 시그널
            obv_col = param.get("obv_col", "obv_raw")
            threshold = param.get("band_filter", 0.0)  # 예: 0.01→1%
            if obv_col not in df.columns:
                partial_signals_list.append(signals_arr)
                continue

            obv_arr = df[obv_col].values
            signals_arr[obv_arr > threshold] = 1
            signals_arr[obv_arr < -threshold] = -1

        elif ttype == "FILTER":
            # Filter 룰 (예: close > filter_max*(1+x_pct) → +1)
            w = param["lookback"]
            x_pct = param["buy_filter"] / 100.0
            y_pct = param["sell_filter"] / 100.0
            close_col = param.get("close_col", "close")
            fmax_col = f"filter_max_{w}"
            fmin_col = f"filter_min_{w}"

            if (fmax_col not in df.columns) or (fmin_col not in df.columns) or (close_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            close_arr = df[close_col].values
            fmax_arr = df[fmax_col].values
            fmin_arr = df[fmin_col].values

            up_mask = close_arr > fmax_arr * (1.0 + x_pct)
            down_mask = close_arr < fmin_arr * (1.0 - y_pct)
            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        elif ttype == "SR":
            # 지지/저항
            lb = param["lookback"]
            band_pct = param.get("band_filter", 0.0) / 100.0
            min_col = f"sr_min_{lb}"
            max_col = f"sr_max_{lb}"
            price_col = param.get("price_col", "close")

            if (min_col not in df.columns) or (max_col not in df.columns) or (price_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_col].values
            smin_arr = df[min_col].values
            smax_arr = df[max_col].values

            up_mask = price_arr > smax_arr * (1.0 + band_pct)
            down_mask = price_arr < smin_arr * (1.0 - band_pct)
            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        elif ttype == "CB":
            # 채널 돌파
            lb = param["lookback"]
            cch = param["c_channel"]
            breakout_pct = cch / 100.0
            min_col = f"ch_min_{lb}"
            max_col = f"ch_max_{lb}"
            price_col = param.get("price_col", "close")

            if (min_col not in df.columns) or (max_col not in df.columns) or (price_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_col].values
            cmin_arr = df[min_col].values
            cmax_arr = df[max_col].values

            up_mask = price_arr > cmax_arr * (1.0 + breakout_pct)
            down_mask = price_arr < cmin_arr * (1.0 - breakout_pct)
            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        elif ttype == "MACD":
            # MACD
            f_per = param["fast_period"]
            s_per = param["slow_period"]
            sig_per = param["signal_period"]
            macd_line_col = f"macd_line_{f_per}_{s_per}_{sig_per}"
            macd_sig_col = f"macd_signal_{f_per}_{s_per}_{sig_per}"

            if (macd_line_col not in df.columns) or (macd_sig_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            macd_line_arr = df[macd_line_col].values
            macd_sig_arr = df[macd_sig_col].values

            up_mask = macd_line_arr > macd_sig_arr
            down_mask = macd_line_arr < macd_sig_arr
            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        elif ttype == "DMI_ADX":
            # DMI+ADX
            dmi_period = param["dmi_period"]
            adx_th = param.get("adx_threshold", 25.0)
            plus_col = f"plus_di_{dmi_period}"
            minus_col = f"minus_di_{dmi_period}"
            adx_col = f"adx_{dmi_period}"

            if (plus_col not in df.columns) or (minus_col not in df.columns) or (adx_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            plus_arr = df[plus_col].values
            minus_arr = df[minus_col].values
            adx_arr = df[adx_col].values

            strong_mask = adx_arr > adx_th
            plus_up = plus_arr > minus_arr
            minus_up = minus_arr > plus_arr

            # 강한 트렌드이면서 +DI가 크면 +1, -DI가 크면 -1
            signals_arr[np.logical_and(strong_mask, plus_up)] = 1
            signals_arr[np.logical_and(strong_mask, minus_up)] = -1

        elif ttype == "STOCH":
            # 스토캐스틱
            k_p = param["k_period"]
            d_p = param["d_period"]
            stoch_k_col = f"stoch_k_{k_p}_{d_p}"
            stoch_d_col = f"stoch_d_{k_p}_{d_p}"
            cross_logic = param.get("cross_logic", False)
            oversold = param.get("oversold", 20.0)
            overbought = param.get("overbought", 80.0)

            if (stoch_k_col not in df.columns) or (stoch_d_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            k_arr = df[stoch_k_col].values
            d_arr = df[stoch_d_col].values

            if not cross_logic:
                # 단순 오버보트/오버솔드
                signals_arr[k_arr < oversold] = 1
                signals_arr[k_arr > overbought] = -1
            else:
                # 교차
                # prev_k, prev_d 사용할 수도 있지만 예시는 간단화
                k_shift = np.r_[np.nan, k_arr[:-1]]  # 전 시점 K
                d_shift = np.r_[np.nan, d_arr[:-1]]  # 전 시점 D
                up_mask = (k_shift < d_shift) & (k_arr > d_arr)
                down_mask = (k_shift > d_shift) & (k_arr < d_arr)
                valid_mask = ~np.isnan(k_shift)  # 첫 행 제외
                signals_arr[up_mask & valid_mask] = 1
                signals_arr[down_mask & valid_mask] = -1

        elif ttype == "BOLL":
            # 볼린저 밴드
            lb = param["lookback"]
            sd = param["stddev_mult"]
            mid_col = f"boll_mid_{lb}_{sd}"
            up_col = f"boll_upper_{lb}_{sd}"
            lo_col = f"boll_lower_{lb}_{sd}"
            price_c = param.get("price_col", "close")

            if (mid_col not in df.columns) or (up_col not in df.columns) or (lo_col not in df.columns) or (price_c not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_c].values
            up_arr = df[up_col].values
            lo_arr = df[lo_col].values

            # price > upper → +1, price < lower → -1
            signals_arr[price_arr > up_arr] = 1
            signals_arr[price_arr < lo_arr] = -1

        elif ttype == "ICHIMOKU":
            # 일목균형표
            t_p = param["tenkan_period"]
            k_p = param["kijun_period"]
            s_p = param["senkou_span_b_period"]
            prefix = f"ich_{t_p}_{k_p}_{s_p}"
            ten_col = f"{prefix}_tenkan"
            kij_col = f"{prefix}_kijun"
            spa_col = f"{prefix}_span_a"
            spb_col = f"{prefix}_span_b"
            chi_col = f"{prefix}_chikou"  # 쓰지 않을 수도 있음
            price_col = param.get("price_col", "close")

            if (ten_col not in df.columns) or (kij_col not in df.columns) \
               or (spa_col not in df.columns) or (spb_col not in df.columns) \
               or (price_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_col].values
            span_a_arr = df[spa_col].values
            span_b_arr = df[spb_col].values
            # 간단히 price가 스팬 A/B 위냐 아래냐만 판별
            up_mask = (price_arr > span_a_arr) & (price_arr > span_b_arr)
            down_mask = (price_arr < span_a_arr) & (price_arr < span_b_arr)
            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        elif ttype == "PSAR":
            # 파라볼릭 SAR
            stp = param["acc_step"]
            mx = param["acc_max"]
            psar_col = f"psar_{stp}_{mx}"
            price_col = param.get("price_col", "close")

            if (psar_col not in df.columns) or (price_col not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_col].values
            psar_arr = df[psar_col].values

            up_mask = psar_arr < price_arr
            signals_arr[up_mask] = 1
            signals_arr[~up_mask] = -1

        elif ttype == "SUPERTREND":
            # 슈퍼트렌드
            ap = param["atr_period"]
            mt = param["multiplier"]
            st_col = f"supertrend_{ap}_{mt}"
            price_c = param.get("price_col", "close")

            if (st_col not in df.columns) or (price_c not in df.columns):
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_c].values
            st_arr = df[st_col].values

            up_mask = price_arr > st_arr
            signals_arr[up_mask] = 1
            signals_arr[~up_mask] = -1

        elif ttype == "FIBO":
            # 피보나치 시그널
            # 예: fibo_cols=["fibo_0.382_set1", "fibo_0.618_set1", ...]
            fibo_cols = param.get("fibo_cols", [])
            price_col = param.get("price_col", "close")
            mode = param.get("mode", "above_last")

            if price_col not in df.columns:
                partial_signals_list.append(signals_arr)
                continue

            price_arr = df[price_col].values
            if not fibo_cols:
                # no fibo cols -> skip
                partial_signals_list.append(signals_arr)
                continue

            # 단순 예: 마지막 레벨보다 위면 +1, 아래면 -1
            # => fibo_cols[-1]
            last_fibo = fibo_cols[-1]
            if last_fibo not in df.columns:
                partial_signals_list.append(signals_arr)
                continue

            fibo_arr = df[last_fibo].values
            up_mask = price_arr > fibo_arr
            down_mask = price_arr < fibo_arr
            signals_arr[up_mask] = 1
            signals_arr[down_mask] = -1

        # partial 시그널 추가
        partial_signals_list.append(signals_arr)

    # partial_signals_list: [arr1, arr2, ...] 각 arr의 shape=(n,)
    # 합산하여 최종 시그널 +1/-1/0 계산
    sum_signals = np.zeros(n, dtype=int)
    for arr in partial_signals_list:
        sum_signals += arr

    # sum_signals>0 -> +1, sum_signals<0 -> -1, else 0
    final_signals = np.where(sum_signals > 0, 1, np.where(sum_signals < 0, -1, 0))

    # pandas DataFrame에 최종 시그널 기록
    df[out_col] = final_signals
    return df
