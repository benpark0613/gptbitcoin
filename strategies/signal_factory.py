# gptbitcoin/strategies/signal_factory.py
# 여러 지표 파라미터(콤보)를 받아 매매 시그널을 한 번에 생성하는 모듈
# 주석은 최소한의 한글로 작성, docstring은 구글 스타일

import pandas as pd
from typing import List, Dict, Any

# 동일 디렉토리에 있는 signal_logic.py 불러옴
from .signal_logic import (
    ma_crossover_signal,
    rsi_signal,
    obv_signal,
    filter_rule_signal,
    support_resistance_signal,
    channel_breakout_signal,
    combine_signals
)


def create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]],
    out_col: str = "signal_final"
) -> pd.DataFrame:
    """
    여러 지표 파라미터(콤보)를 받아, 각 지표별 시그널(+1/-1/0)을 모두 계산 후 합산하여
    최종 시그널을 out_col 컬럼에 기록한다.

    Args:
        df (pd.DataFrame): 보조지표 칼럼이 포함된 DataFrame
        combo_params (List[Dict[str, Any]]): 각 지표 설정(dict)들의 리스트
            예) [
              {"type": "MA", "short_period": 5, "long_period": 20},
              {"type": "RSI", "length": 14, "overbought": 70, "oversold": 30},
              ...
            ]
        out_col (str, optional): 최종 시그널 칼럼명, 기본값 'signal_final'.

    Returns:
        pd.DataFrame: df에 out_col 칼럼으로 최종 시그널(+1/0/-1)이 추가된 상태
    """
    df_local = df.copy()
    temp_cols = []

    for i, param in enumerate(combo_params):
        signal_col = f"temp_combo_sig_{i}"
        df_local[signal_col] = 0

        ttype = param.get("type", "")
        if ttype == "MA":
            # MA 지표: short_period, long_period가 필수
            sp = param["short_period"]
            lp = param["long_period"]
            df_local = ma_crossover_signal(
                df_local,
                short_ma_col=f"ma_{sp}",
                long_ma_col=f"ma_{lp}",
                signal_col=signal_col
            )

        elif ttype == "RSI":
            # RSI 지표: length, overbought, oversold가 필수
            length = param["length"]
            ub = param["overbought"]
            lb = param["oversold"]
            df_local = rsi_signal(
                df_local,
                rsi_col=f"rsi_{length}",
                lower_bound=lb,
                upper_bound=ub,
                signal_col=signal_col
            )

        elif ttype == "OBV":
            # OBV 지표: obv_col, threshold 등 옵션
            obv_col = param.get("obv_col", "obv_raw")
            threshold = param.get("threshold", 0.0)
            df_local = obv_signal(
                df_local,
                obv_col=obv_col,
                threshold=threshold,
                signal_col=signal_col
            )

        elif ttype == "Filter":
            # Filter 지표: window, x_pct, y_pct
            w = param["window"]
            x_ = param["x_pct"]
            y_ = param["y_pct"]
            df_local = filter_rule_signal(
                df_local,
                close_col="close",
                window=w,
                x_pct=x_,
                y_pct=y_,
                signal_col=signal_col
            )

        elif ttype == "Support_Resistance":
            # S/R 지표: window, band_pct
            w = param["window"]
            band = param["band_pct"]
            df_local = support_resistance_signal(
                df_local,
                rolling_min_col=f"sr_min_{w}",
                rolling_max_col=f"sr_max_{w}",
                band_pct=band,
                signal_col=signal_col
            )

        elif ttype == "Channel_Breakout":
            # Channel Breakout 지표: window, c_value
            w = param["window"]
            cval = param["c_value"]
            df_local = channel_breakout_signal(
                df_local,
                rolling_min_col=f"ch_min_{w}",
                rolling_max_col=f"ch_max_{w}",
                breakout_pct=cval,
                signal_col=signal_col
            )

        else:
            raise ValueError(f"지원되지 않는 지표 타입입니다: {ttype}")

        temp_cols.append(signal_col)

    # 여러 개의 임시 시그널을 합산해 최종 시그널을 out_col에 기록
    df_local = combine_signals(df_local, signal_cols=temp_cols, out_col=out_col)
    return df_local
