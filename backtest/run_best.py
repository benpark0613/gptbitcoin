# gptbitcoin/backtest/run_best.py
# 특정 지표 콤보만 백테스트하고, 트레이드 로그 + 시작/종료 자본, 바이앤홀드(B/H) 시작/종료 자본을 함께 콘솔 출력
# 최소한의 한글 주석, 구글 스타일 docstring

import pandas as pd
from typing import List, Dict, Any

try:
    from backtest.engine import run_backtest
except ImportError:
    raise ImportError("[run_best.py] engine.py import 오류")

try:
    from strategies.signal_logic import (
        ma_crossover_signal,
        rsi_signal,
        obv_signal,
        filter_rule_signal,
        support_resistance_signal,
        channel_breakout_signal,
        combine_signals
    )
except ImportError:
    raise ImportError("[run_best.py] signal_logic.py import 오류")

try:
    from utils.date_time import ms_to_kst_str
except ImportError:
    raise ImportError("[run_best.py] date_time.py import 오류")

try:
    from config.config import ALLOW_SHORT, START_CAPITAL
except ImportError:
    ALLOW_SHORT = True
    START_CAPITAL = 1_000_000


def run_best_single(
    df: pd.DataFrame,
    combo_info: Dict[str, Any],
    start_capital: float = START_CAPITAL
) -> None:
    """
    combo_info로 단일 콤보 백테스트 후, 트레이드 로그와 시작/종료 자본을 콘솔 출력.
    추가로 Buy & Hold의 시작/종료 자본도 함께 출력.
    """
    timeframe = combo_info["timeframe"]
    combos = combo_info["combo_params"]
    allow_short = ALLOW_SHORT

    if not combos:
        print("[run_best_single] combo_params가 비어 있습니다.")
        return

    # 1) 콤보에 대한 시그널 생성
    signals = _create_signals_for_combo(df, combos)

    # 2) 콤보 백테스트
    engine_out = run_backtest(
        df=df,
        signals=signals,
        start_capital=start_capital,
        allow_short=allow_short
    )

    # 3) 콤보 트레이드 로그 (KST 시각)
    trades = engine_out["trades"]
    if not trades:
        print("[run_best_single] 매매가 발생하지 않았습니다.")
    else:
        print("[run_best_single] Trades Log (KST):")
        for i, trade in enumerate(trades, start=1):
            e_idx = trade.get("entry_index", None)
            x_idx = trade.get("exit_index", None)
            ptype = trade.get("position_type", "N/A")

            if isinstance(e_idx, int) and 0 <= e_idx < len(df):
                ms_entry = df.iloc[e_idx]["open_time"]
                entry_kst = ms_to_kst_str(ms_entry)
            else:
                entry_kst = "N/A"

            if isinstance(x_idx, int) and 0 <= x_idx < len(df):
                ms_exit = df.iloc[x_idx]["open_time"]
                exit_kst = ms_to_kst_str(ms_exit)
            else:
                exit_kst = "End"

            print(f"  [{i}] {ptype.upper()} Entry={entry_kst}, Exit={exit_kst}")

    # 4) 콤보 Start/End Capital
    equity_curve = engine_out["equity_curve"]
    end_capital = equity_curve[-1] if equity_curve else start_capital
    print(f"\n[run_best_single] (Combo) Start Capital: {start_capital:.2f}, End Capital: {end_capital:.2f}")

    # 5) Buy & Hold 백테스트 (Always LONG = 1)
    bh_signals = [1] * len(df)
    bh_out = run_backtest(
        df=df,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_eq = bh_out["equity_curve"]
    bh_end_capital = bh_eq[-1] if bh_eq else start_capital

    print(f"[run_best_single] (Buy & Hold) Start Capital: {start_capital:.2f}, End Capital: {bh_end_capital:.2f}")


def _create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]]
) -> List[int]:
    """
    combo_params를 기반으로 지표 시그널 생성 후 합산 시그널(1/-1/0) 반환.
    """
    df_local = df.copy()
    temp_signal_cols = []

    for i, param in enumerate(combo_params):
        ttype = param["type"]
        signal_col = f"temp_best_sig_{i}"
        df_local[signal_col] = 0

        if ttype == "MA":
            sp = param["short_period"]
            lp = param["long_period"]
            df_local = ma_crossover_signal(
                df_local,
                short_ma_col=f"ma_{sp}",
                long_ma_col=f"ma_{lp}",
                signal_col=signal_col
            )
        elif ttype == "RSI":
            length = param["length"]
            overbought = param["overbought"]
            oversold = param["oversold"]
            df_local = rsi_signal(
                df_local,
                rsi_col=f"rsi_{length}",
                lower_bound=oversold,
                upper_bound=overbought,
                signal_col=signal_col
            )
        elif ttype == "OBV":
            threshold = param["threshold"]
            df_local = obv_signal(
                df_local,
                obv_col="obv_raw",
                threshold=threshold,
                signal_col=signal_col
            )
        elif ttype == "Filter":
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
            w = param["window"]
            bp = param["band_pct"]
            df_local = support_resistance_signal(
                df_local,
                rolling_min_col=f"sr_min_{w}",
                rolling_max_col=f"sr_max_{w}",
                band_pct=bp,
                signal_col=signal_col
            )
        elif ttype == "Channel_Breakout":
            w = param["window"]
            c_ = param["c_value"]
            df_local = channel_breakout_signal(
                df_local,
                rolling_min_col=f"ch_min_{w}",
                rolling_max_col=f"ch_max_{w}",
                breakout_pct=c_,
                signal_col=signal_col
            )
        else:
            raise ValueError(f"지원되지 않는 지표 타입: {ttype}")

        temp_signal_cols.append(signal_col)

    # 여러 시그널 합산
    df_local = combine_signals(df_local, signal_cols=temp_signal_cols, out_col="signal_final")
    return df_local["signal_final"].tolist()
