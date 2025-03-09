# gptbitcoin/backtest/run_best.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 단일 콤보(베스트 콤보) 백테스트 + 바이앤홀드(B/H) 백테스트를 함께 진행하는 모듈.
# combo_info 내 buy_time_delay / sell_time_delay / holding_period 가 있으면
# run_backtest(engine)에 전달한다.

import pandas as pd
from typing import Dict, Any, List

from backtest.engine import run_backtest
from analysis.scoring import calculate_metrics
from strategies.signal_factory import create_signals_for_combo
from utils.date_time import ms_to_kst_str


def _detect_final_position(trades: List[Dict[str, Any]], df_len: int) -> str:
    """
    마지막 포지션이 청산되지 않았다면 LONG/SHORT, 청산됐다면 FLAT을 반환.

    Args:
        trades (List[Dict[str, Any]]): 매매 내역 리스트
        df_len (int): 백테스트 구간 DF 길이

    Returns:
        str: "LONG", "SHORT", "FLAT"
    """
    if not trades:
        return "FLAT"

    last_trade = trades[-1]
    exit_idx = last_trade.get("exit_index", None)
    if exit_idx is not None and exit_idx >= df_len:
        ptype = last_trade.get("position_type", "").upper()
        if ptype == "LONG":
            return "LONG"
        elif ptype == "SHORT":
            return "SHORT"
    return "FLAT"


def _record_trades_info(df: pd.DataFrame, trades: List[Dict[str, Any]]) -> str:
    """
    트레이드 로그를 문자열로 요약한다.
    - Entry/Exit 시각을 KST 기준으로 변환해 출력
    - 간단히 매매 내역을 확인할 수 있도록 한다.

    Args:
        df (pd.DataFrame): 백테스트에 사용된 DataFrame (open_time 칼럼 포함)
        trades (List[Dict[str, Any]]): 매매 내역 (pnl, position_type 등 포함)

    Returns:
        str: 매매 기록 요약 문자열
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")
        pnl = t.get("pnl", 0.0)

        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_val_entry = df.iloc[e_idx]["open_time"]
            entry_time_str = ms_to_kst_str(ms_val_entry)
        else:
            entry_time_str = "N/A"

        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_val_exit = df.iloc[x_idx]["open_time"]
            exit_time_str = ms_to_kst_str(ms_val_exit)
        else:
            exit_time_str = "End"

        logs.append(
            f"[{i}] {ptype.upper()} Entry={entry_time_str}, Exit={exit_time_str}, PnL={pnl:.2f}"
        )

    return "\n".join(logs)


def run_best_single(
    df: pd.DataFrame,
    combo_info: Dict[str, Any]
) -> Dict[str, Any]:
    """
    주어진 콤보(단일)로 백테스트를 1회 수행하고,
    바이앤홀드(B/H) 결과와 함께 반환한다.

    1) combo_params 내 지표 파라미터를 기반으로 매매 시그널을 생성한다.
    2) run_backtest 수행 (buy_time_delay, sell_time_delay, holding_period 있으면 전달)
    3) 분석 지표(calculate_metrics) 산출
    4) B/H도 별도 백테스트하여 동일 지표 산출
    5) 두 결과를 모두 반환

    Args:
        df (pd.DataFrame): 이미 보조지표가 계산된 DF (open_time, close 등 필수)
        combo_info (Dict[str, Any]): {
            "timeframe": str,
            "combo_params": [ {type=..., ...}, {...} ]
        }

    Returns:
        Dict[str, Any]: {
            "combo_score": {...},      # 콤보 백테스트 성과 지표
            "combo_trades": [...],
            "combo_position": "LONG"/"SHORT"/"FLAT",
            "combo_trades_log": str,
            "bh_score": {...},         # Buy & Hold 성과 지표
            "bh_trades": [...],
            "bh_trades_log": str
        }
    """
    # 1) 콤보 시그널 생성
    combo_params = combo_info.get("combo_params", [])
    timeframe = combo_info.get("timeframe", "unknown")

    df_combo = df.copy()
    df_combo = create_signals_for_combo(df_combo, combo_params, out_col="signal_final")
    combo_signals = df_combo["signal_final"].tolist()

    # 콤보에 명시된 buy_time_delay, sell_time_delay, holding_period 추출
    buy_td = -1
    sell_td = -1
    hold_p = 0
    for cdict in combo_params:
        if "buy_time_delay" in cdict:
            buy_td = cdict["buy_time_delay"]
        if "sell_time_delay" in cdict:
            sell_td = cdict["sell_time_delay"]
        if "holding_period" in cdict:
            hold_p = cdict["holding_period"]

    # 2) 콤보 백테스트
    combo_engine_out = run_backtest(
        df=df_combo,
        signals=combo_signals,
        allow_short=True,          # config
        buy_time_delay=buy_td,
        sell_time_delay=sell_td,
        holding_period=hold_p
    )
    combo_trades = combo_engine_out["trades"]

    combo_score = calculate_metrics(
        equity_curve=combo_engine_out["equity_curve"],
        daily_returns=combo_engine_out["daily_returns"],
        start_capital=combo_engine_out["equity_curve"][0] if combo_engine_out["equity_curve"] else 100_000,
        trades=combo_trades,
        timeframe=timeframe
    )

    combo_trades_log = _record_trades_info(df_combo, combo_trades)
    combo_position = _detect_final_position(combo_trades, len(df_combo))

    # 3) 바이앤홀드(B/H) 백테스트
    bh_signals = [1] * len(df)
    bh_engine_out = run_backtest(df, signals=bh_signals)
    bh_trades = bh_engine_out["trades"]

    bh_score = calculate_metrics(
        equity_curve=bh_engine_out["equity_curve"],
        daily_returns=bh_engine_out["daily_returns"],
        start_capital=bh_engine_out["equity_curve"][0] if bh_engine_out["equity_curve"] else 100_000,
        trades=bh_trades,
        timeframe=timeframe
    )
    bh_trades_log = _record_trades_info(df, bh_trades)

    return {
        "combo_score": combo_score,
        "combo_trades": combo_trades,
        "combo_position": combo_position,
        "combo_trades_log": combo_trades_log,
        "bh_score": bh_score,
        "bh_trades": bh_trades,
        "bh_trades_log": bh_trades_log
    }
