# gptbitcoin/strategies/signal_factory.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 여러 지표 파라미터(콤보)를 받아 매매 시그널(+1/-1/0)을 생성하고 합산하는 역할.
# time_delays, holding_periods는 engine에서 처리하므로 여기서는 band_filter 등 지표별 파라미터를 기반으로 시그널만 계산.

import pandas as pd
from typing import List, Dict, Any

# 아래 import들은 동일 디렉토리에 위치한 signal_logic.py 내 함수들
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
    여러 지표 파라미터(콤보)를 받아 각 지표별 매매 신호(+1, -1, 0)를 계산하고,
    이를 합산(결합)하여 최종 신호를 out_col에 기록한다.

    Args:
        df (pd.DataFrame): 보조지표(예: ma_10, rsi_14, sr_min_20 등)가 포함된 DataFrame.
        combo_params (List[Dict[str, Any]]): 지표별 설정(dict)들의 리스트.
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
              {
                "type": "Filter",
                "lookback": 10,
                "buy_filter": 5,
                "sell_filter": 10
              },
              ...
            ]
        out_col (str, optional): 최종 합산 시그널 칼럼명. 기본 "signal_final".

    Returns:
        pd.DataFrame: df에 out_col 컬럼으로 최종 신호(+1/-1/0)가 추가된 DataFrame
    """
    df_local = df.copy()
    temp_cols = []

    # combo_params 내 각 지표 파라미터를 순회하면서 시그널 계산
    for i, param in enumerate(combo_params):
        signal_col = f"temp_combo_sig_{i}"
        df_local[signal_col] = 0

        # 지표 타입
        ttype = str(param.get("type", "")).upper()

        if ttype == "MA":
            # 이동평균 크로스오버
            sp = param["short_period"]
            lp = param["long_period"]
            bf = param.get("band_filter", 0.0)  # default=0.0
            df_local = ma_crossover_signal(
                df_local,
                short_ma_col=f"ma_{sp}",
                long_ma_col=f"ma_{lp}",
                band_filter=bf,
                signal_col=signal_col
            )

        elif ttype == "RSI":
            # RSI 신호
            lb = param["lookback"]
            upper = param["overbought"]
            lower = param["oversold"]
            df_local = rsi_signal(
                df_local,
                rsi_col=f"rsi_{lb}",
                lower_bound=lower,
                upper_bound=upper,
                signal_col=signal_col
            )

        elif ttype == "OBV":
            # OBV 신호
            obv_col = param.get("obv_col", "obv_raw")  # 기본 'obv_raw'
            threshold = param.get("threshold", param.get("band_filter", 0.0))
            df_local = obv_signal(
                df_local,
                obv_col=obv_col,
                threshold=threshold,
                signal_col=signal_col
            )

        elif ttype == "FILTER":
            # Filter 룰
            lb = param["lookback"]
            b_f = param["buy_filter"]   # 예: 5 => 5%
            s_f = param["sell_filter"]  # 예: 10 => 10%
            x_pct = b_f / 100.0
            y_pct = s_f / 100.0
            df_local = filter_rule_signal(
                df_local,
                close_col="close",
                window=lb,
                x_pct=x_pct,
                y_pct=y_pct,
                signal_col=signal_col
            )

        elif ttype == "SR":
            # 지지·저항
            lb = param["lookback"]
            bf = param.get("band_filter", 0.0)
            sr_band = bf / 100.0  # %
            df_local = support_resistance_signal(
                df_local,
                rolling_min_col=f"sr_min_{lb}",
                rolling_max_col=f"sr_max_{lb}",
                price_col="close",
                band_pct=sr_band,
                signal_col=signal_col
            )

        elif ttype == "CB":
            # 채널 돌파
            lb = param["lookback"]
            cch = param["c_channel"]
            breakout_pct = cch / 100.0  # %
            df_local = channel_breakout_signal(
                df_local,
                rolling_min_col=f"ch_min_{lb}",
                rolling_max_col=f"ch_max_{lb}",
                price_col="close",
                breakout_pct=breakout_pct,
                signal_col=signal_col
            )

        else:
            raise ValueError(f"지원하지 않는 지표 타입입니다: {ttype}")

        temp_cols.append(signal_col)

    # 여러 시그널을 합산해 최종 시그널 생성
    df_local = combine_signals(df_local, signal_cols=temp_cols, out_col=out_col)
    return df_local
