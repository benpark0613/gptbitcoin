# gptbitcoin/backtest/run_nosplit.py
# 구글 스타일 docstring, 최소한의 한글 주석
# 단일(전체) 구간 백테스트 모듈.
# - Buy & Hold 전략 + 여러 지표 콤보 전략을 모두 테스트
# - trades_log(매매 내역 로그)를 함께 기록해 CSV 등에 저장 가능

import json
from typing import List, Dict, Any

import pandas as pd
from joblib import Parallel, delayed

from utils.date_time import ms_to_kst_str
from backtest.engine import run_backtest
from analysis.scoring import calculate_metrics
from strategies.signal_factory import create_signals_for_combo


def run_nosplit(
    df: pd.DataFrame,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    risk_free_rate_annual: float = 0.0
) -> List[Dict[str, Any]]:
    """
    단일(전체) 구간 백테스트:
      1) Buy & Hold (항상 매수) 전략
      2) combos 내 여러 지표 파라미터 전략
    을 모두 테스트하고, 결과(성과 + 매매로그)를 리스트(dict)로 반환한다.

    Args:
        df (pd.DataFrame): 백테스트용 DF (OHLCV + 지표)
        combos (List[List[Dict[str, Any]]]): 지표 파라미터 조합 목록
        timeframe (str): 예) "1d", "4h" 등
        risk_free_rate_annual (float, optional): 연간 무위험이자율 (샤프 계산용)

    Returns:
        List[Dict[str, Any]]:
            [
              {
                "timeframe": str,
                "start_cap": float,
                "end_cap": float,
                "returns": float,
                "trades": int,
                "sharpe": float,
                "mdd": float,
                "used_indicators": str,
                "trades_log": str
              },
              ...
            ]
    """
    results = []

    # 1) Buy & Hold
    bh_signals = [1] * len(df)
    bh_out = run_backtest(df=df, signals=bh_signals)
    bh_score = calculate_metrics(
        equity_curve=bh_out["equity_curve"],
        daily_returns=bh_out["daily_returns"],
        start_capital=bh_out["equity_curve"][0],
        trades=bh_out["trades"],
        timeframe=timeframe,
        risk_free_rate_annual=risk_free_rate_annual
    )
    bh_trades_log = _record_trades_info(df, bh_out["trades"])

    # Buy & Hold 결과
    results.append({
        "timeframe": f"{timeframe}(B/H)",
        "start_cap": bh_score["StartCapital"],
        "end_cap": bh_score["EndCapital"],
        "returns": bh_score["Return"],
        "trades": bh_score["Trades"],
        "sharpe": bh_score["Sharpe"],
        "mdd": bh_score["MDD"],
        "used_indicators": "Buy & Hold",
        "trades_log": bh_trades_log
    })

    # 2) combos 병렬 백테스트
    def _process_combo_single(combo: List[Dict[str, Any]]) -> Dict[str, Any]:
        # 콤보 시그널 생성
        df_local = create_signals_for_combo(df, combo, out_col="signal_final")
        signals = df_local["signal_final"].tolist()

        # 백테스트
        engine_out = run_backtest(df_local, signals=signals)
        score = calculate_metrics(
            equity_curve=engine_out["equity_curve"],
            daily_returns=engine_out["daily_returns"],
            start_capital=engine_out["equity_curve"][0],
            trades=engine_out["trades"],
            timeframe=timeframe,
            risk_free_rate_annual=risk_free_rate_annual
        )

        # 매매 내역 로그
        combo_trades_log = _record_trades_info(df_local, engine_out["trades"])

        # used_indicators에 combo 정보 저장
        combo_info = {"timeframe": timeframe, "combo_params": combo}
        used_str = json.dumps(combo_info, ensure_ascii=False)

        return {
            "timeframe": timeframe,
            "start_cap": score["StartCapital"],
            "end_cap": score["EndCapital"],
            "returns": score["Return"],
            "trades": score["Trades"],
            "sharpe": score["Sharpe"],
            "mdd": score["MDD"],
            "used_indicators": used_str,
            "trades_log": combo_trades_log
        }

    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo_single)(combo) for combo in combos
    )
    results.extend(parallel_out)

    return results


def _record_trades_info(df: pd.DataFrame, trades: List[Dict[str, Any]]) -> str:
    """
    매매 내역(trades)을 문자열로 요약 (entry, exit를 KST로 변환).
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")
        pnl_val = t.get("pnl", 0.0)

        # 진입 시각 KST
        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_entry = df.iloc[e_idx]["open_time"]
            entry_time_str = ms_to_kst_str(ms_entry)
        else:
            entry_time_str = "N/A"

        # 청산 시각 KST
        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_exit = df.iloc[x_idx]["open_time"]
            exit_time_str = ms_to_kst_str(ms_exit)
        elif isinstance(x_idx, int) and x_idx >= len(df):
            # exit_index가 df 범위를 벗어나면 마지막 봉 청산
            exit_time_str = "End"
        else:
            exit_time_str = "N/A"

        logs.append(
            f"[{i}] {ptype.upper()} Entry={entry_time_str}, Exit={exit_time_str}, PnL={pnl_val:.2f}"
        )

    return "; ".join(logs)
