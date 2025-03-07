# gptbitcoin/backtest/run_best.py
# 전량 매수·매도 방식의 단일 콤보 백테스트 모듈
# 특정 지표 콤보만 백테스트하고 트레이드 로그 + 자본 변화를 콘솔에 출력한다.
# (DB 접근/coverage 로직은 없음. DataFrame(df)은 이미 상위에서 준비해 건네준다고 가정)

import pandas as pd
from datetime import datetime
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

# config.py 기본값 (ALLOW_SHORT, START_CAPITAL 등) - DB 접근은 하지 않음
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
    combo_info에 설정된 파라미터로 단일 콤보를 백테스트하고,
    매매 로그와 시작/종료 자본을 콘솔에 출력한다.
    또한 Buy & Hold (항상 매수) 전략과의 자본 비교도 함께 출력.

    Args:
        df (pd.DataFrame): 이미 전처리/보조지표 계산이 끝난 DataFrame (open_time, close 등 포함)
        combo_info (Dict[str, Any]): {
            "timeframe": ...,
            "combo_params": [ {...}, {...} ]  # 예: [{"type":"MA","short_period":5,"long_period":20}, ...]
        }
        start_capital (float, optional): 초기 자본

    Returns:
        None
    """
    timeframe = combo_info.get("timeframe", "unknown")
    combo_params = combo_info.get("combo_params", [])
    allow_short = ALLOW_SHORT

    if not combo_params:
        print("[run_best_single] combo_params가 비어 있습니다.")
        return

    print("[run_best_single] >> combo_info:", combo_info)

    # 1) 콤보 시그널 생성
    signals = _create_signals_for_combo(df, combo_params)

    # 2) 콤보 백테스트
    engine_out = run_backtest(
        df=df,
        signals=signals,
        start_capital=start_capital,
        allow_short=allow_short
    )

    # 3) 트레이드 로그 출력
    trades = engine_out["trades"]
    if not trades:
        print("[run_best_single] 매매가 발생하지 않았습니다. (Combo)")
    else:
        print(f"[run_best_single] Trades Log (UTC) for Combo:")
        trades_log_str = _record_trades_info(df, trades)
        print(trades_log_str)

    # 4) 콤보 결과 자본
    equity_curve = engine_out["equity_curve"]
    end_capital = equity_curve[-1] if equity_curve else start_capital
    print(f"\n[run_best_single] (Combo) Timeframe={timeframe}, "
          f"Start Capital: {start_capital:.2f}, End Capital: {end_capital:.2f}")

    # 5) Buy & Hold 비교
    bh_signals = [1] * len(df)  # 항상 매수
    bh_out = run_backtest(
        df=df,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_eq = bh_out["equity_curve"]
    bh_end_capital = bh_eq[-1] if bh_eq else start_capital

    print(f"[run_best_single] (Buy & Hold) Timeframe={timeframe}, "
          f"Start Capital: {start_capital:.2f}, End Capital: {bh_end_capital:.2f}")


def _create_signals_for_combo(
    df: pd.DataFrame,
    combo_params: List[Dict[str, Any]]
) -> List[int]:
    """
    combo_params(지표 파라미터 목록)을 기반으로 시그널을 생성 후 합산 시그널(1/-1/0) 반환.

    Args:
        df (pd.DataFrame): 종가, 지표 등이 들어있는 DataFrame
        combo_params (List[Dict[str, Any]]):
            예) [{"type":"MA","short_period":5,"long_period":20}, {"type":"RSI","length":14,...}, ...]

    Returns:
        List[int]: 각 시점별 최종 시그널(1/-1/0)
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
            # threshold가 없으면 0.0 기본
            threshold = param.get("threshold", 0.0)
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

    df_local = combine_signals(df_local, signal_cols=temp_signal_cols, out_col="signal_final")
    return df_local["signal_final"].tolist()


def _record_trades_info(df: pd.DataFrame, trades: List[Dict[str, Any]]) -> str:
    """
    매매 내역(trades)을 간단히 문자열로 요약하여 반환.
    여기서는 UTC 시각만을 출력한다.

    Args:
        df (pd.DataFrame): 백테스트 시 사용된 시계열(OHLCV). 'open_time'을 UTC ms로 가정.
        trades (List[Dict[str, Any]]): run_backtest 결과물 중 "trades"

    Returns:
        str: 사람 친화적 트레이드 정보 문자열
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")
        pnl = t.get("pnl", 0.0)

        # 진입 시간
        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_entry = df.iloc[e_idx]["open_time"]  # UTC ms
            entry_dt = datetime.utcfromtimestamp(ms_entry / 1000.0)
            entry_str = entry_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            entry_str = "N/A"

        # 청산 시간
        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_exit = df.iloc[x_idx]["open_time"]
            exit_dt = datetime.utcfromtimestamp(ms_exit / 1000.0)
            exit_str = exit_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            exit_str = "End"

        logs.append(
            f"[{i}] {ptype.upper()} Entry={entry_str}, Exit={exit_str}, PnL={pnl:.2f}"
        )

    return "\n".join(logs)