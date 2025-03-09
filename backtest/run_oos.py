# gptbitcoin/backtest/run_oos.py
# 구글 스타일 docstring 사용, 최소한의 한글 주석.
# IS 통과 여부와 상관없이 전체 콤보에 대해 OOS(아웃샘플) 구간 백테스트를 수행하고,
# 매매 내역과 로그(oos_trades_log)까지 함께 저장해 반환한다.

import json
from typing import List, Dict, Any

import pandas as pd
from joblib import Parallel, delayed

from config.config import ALLOW_SHORT, START_CAPITAL
from analysis.scoring import calculate_metrics
from backtest.engine import run_backtest
from strategies.signal_factory import create_signals_for_combo
from utils.date_time import ms_to_kst_str


def _record_trades_info(df: pd.DataFrame, trades: List[Dict[str, Any]]) -> str:
    """
    OOS 구간에서 발생한 매매 내역(trades)을 KST 시각으로 요약하여 문자열로 반환한다.

    Args:
        df (pd.DataFrame): OOS 구간 DataFrame (open_time 칼럼 포함)
        trades (List[Dict[str, Any]]): 매매 내역 리스트

    Returns:
        str: 매매 내역 요약 문자열 (예: "No Trades" if empty)
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")
        pnl_val = t.get("pnl", 0.0)

        # 진입 시점
        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_val_entry = df.iloc[e_idx]["open_time"]
            entry_time_str = ms_to_kst_str(ms_val_entry)
        else:
            entry_time_str = "N/A"

        # 청산 시점
        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_val_exit = df.iloc[x_idx]["open_time"]
            exit_time_str = ms_to_kst_str(ms_val_exit)
        elif isinstance(x_idx, int) and x_idx >= len(df):
            # DF 범위를 넘어선 경우 End 처리
            exit_time_str = "End"
        else:
            exit_time_str = "N/A"

        logs.append(
            f"[{i}] {ptype.upper()} Entry={entry_time_str}, "
            f"Exit={exit_time_str}, PnL={pnl_val:.2f}"
        )

    return "; ".join(logs)


def run_oos(
    df_oos: pd.DataFrame,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    start_capital: float = START_CAPITAL
) -> List[Dict[str, Any]]:
    """
    OOS(아웃샘플) 백테스트:
      1) Buy & Hold 전략(항상 매수)으로 전체 구간 백테스트 후 oos_* 결과 산출.
      2) combos 내 모든 지표 파라미터 조합별로 백테스트를 병렬로 수행 (oos_trades_log 포함).
      3) 각 콤보별 OOS 성과(딕셔너리)를 리스트로 반환.

    Args:
        df_oos (pd.DataFrame): OOS 구간 시계열 데이터 (OHLCV 및 지표)
        combos (List[List[Dict[str, Any]]]): 파라미터 조합(콤보) 목록
        timeframe (str): 예) "1d"
        start_capital (float, optional): OOS 구간 시작 자본

    Returns:
        List[Dict[str, Any]]: [
            {
                "timeframe": ...,
                "oos_start_cap": ...,
                "oos_end_cap": ...,
                "oos_return": ...,
                "oos_trades": ...,
                "oos_trades_log": ...,
                "oos_sharpe": ...,
                "oos_mdd": ...,
                "used_indicators": ...
            },
            ...
        ]
    """
    if df_oos.empty:
        return []

    # OOS 구간 로그용 시각 (KST)
    oos_start_ms = df_oos.iloc[0]["open_time"]
    oos_end_ms = df_oos.iloc[-1]["open_time"]
    oos_start_kst = ms_to_kst_str(oos_start_ms)
    oos_end_kst = ms_to_kst_str(oos_end_ms)
    print(f"[INFO] OOS({timeframe}) range: {oos_start_kst} ~ {oos_end_kst}, rows={len(df_oos)}")

    # 1) Buy & Hold (항상 매수) 백테스트
    bh_signals = [1] * len(df_oos)
    bh_result = run_backtest(
        df=df_oos,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_score = calculate_metrics(
        equity_curve=bh_result["equity_curve"],
        daily_returns=bh_result["daily_returns"],
        start_capital=start_capital,
        trades=bh_result["trades"],
        timeframe=timeframe
    )

    # 바이앤홀드 매매 로그
    bh_trades_log = _record_trades_info(df_oos, bh_result["trades"])

    bh_row = {
        "timeframe": f"{timeframe}(B/H)",
        "oos_start_cap": bh_score["StartCapital"],
        "oos_end_cap": bh_score["EndCapital"],
        "oos_return": bh_score["Return"],
        "oos_trades": bh_score["Trades"],
        "oos_trades_log": bh_trades_log,
        "oos_sharpe": bh_score["Sharpe"],
        "oos_mdd": bh_score["MDD"],
        "used_indicators": "Buy and Hold"
    }

    # 2) combos 병렬 백테스트
    def _process_combo_oos(combo: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        주어진 콤보(복수 지표)로 OOS 구간 백테스트 후 결과(성과 + 매매 로그)를 반환한다.
        """
        # 콤보 시그널 생성
        df_local = create_signals_for_combo(df_oos, combo, out_col="signal_oos_final")
        signals = df_local["signal_oos_final"].tolist()

        # buy_time_delay, sell_time_delay, holding_period 추출
        buy_td = -1
        sell_td = -1
        hold_p = 0
        for cdict in combo:
            if "buy_time_delay" in cdict:
                buy_td = cdict["buy_time_delay"]
            if "sell_time_delay" in cdict:
                sell_td = cdict["sell_time_delay"]
            if "holding_period" in cdict:
                hold_p = cdict["holding_period"]

        # 백테스트 수행
        engine_out = run_backtest(
            df=df_local,
            signals=signals,
            start_capital=start_capital,
            allow_short=ALLOW_SHORT,
            buy_time_delay=buy_td,
            sell_time_delay=sell_td,
            holding_period=hold_p
        )
        score = calculate_metrics(
            equity_curve=engine_out["equity_curve"],
            daily_returns=engine_out["daily_returns"],
            start_capital=start_capital,
            trades=engine_out["trades"],
            timeframe=timeframe
        )

        # 매매 로그
        combo_trades_log = _record_trades_info(df_local, engine_out["trades"])

        combo_info = {"timeframe": timeframe, "combo_params": combo}
        used_str = json.dumps(combo_info, ensure_ascii=False)

        return {
            "timeframe": timeframe,
            "oos_start_cap": score["StartCapital"],
            "oos_end_cap": score["EndCapital"],
            "oos_return": score["Return"],
            "oos_trades": score["Trades"],
            "oos_trades_log": combo_trades_log,
            "oos_sharpe": score["Sharpe"],
            "oos_mdd": score["MDD"],
            "used_indicators": used_str
        }

    # B/H 결과 먼저 저장
    results = [bh_row]

    # 병렬 처리로 모든 콤보 실행
    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo_oos)(combo) for combo in combos
    )
    results.extend(parallel_out)

    return results
