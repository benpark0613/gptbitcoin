# gptbitcoin/backtest/run_nosplit.py
# 최소한의 한글 주석, 구글 스타일 docstring을 사용하는 단일(전체) 구간 백테스트 모듈.
# - Buy & Hold(항상 매수) 전략 + 여러 지표 콤보 전략을 모두 테스트
# - combo 내 buy_time_delay, sell_time_delay, holding_period가 있으면 자동 적용

import json
from typing import List, Dict, Any

import pandas as pd
from joblib import Parallel, delayed

from config.config import ALLOW_SHORT, START_CAPITAL
from backtest.engine import run_backtest
from analysis.scoring import calculate_metrics
from strategies.signal_factory import create_signals_for_combo
from utils.date_time import ms_to_kst_str


def _record_trades_info(df: pd.DataFrame, trades: List[Dict[str, Any]]) -> str:
    """
    매매 내역(trades)을 KST 시각으로 요약하여 문자열로 반환한다.

    Args:
        df (pd.DataFrame): 백테스트에 사용된 시계열 데이터 (open_time 칼럼 포함)
        trades (List[Dict[str, Any]]): 매매 내역 (pnl, position_type 등)

    Returns:
        str: 매매 내역 요약 문자열. trades가 없으면 "No Trades"
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")
        pnl_val = t.get("pnl", 0.0)

        # 진입 시각
        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_entry = df.iloc[e_idx]["open_time"]
            entry_time_str = ms_to_kst_str(ms_entry)
        else:
            entry_time_str = "N/A"

        # 청산 시각
        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_exit = df.iloc[x_idx]["open_time"]
            exit_time_str = ms_to_kst_str(ms_exit)
        elif isinstance(x_idx, int) and x_idx >= len(df):
            exit_time_str = "End"
        else:
            exit_time_str = "N/A"

        logs.append(
            f"[{i}] {ptype.upper()} Entry={entry_time_str}, "
            f"Exit={exit_time_str}, PnL={pnl_val:.2f}"
        )

    return "; ".join(logs)


def run_nosplit(
    df: pd.DataFrame,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    risk_free_rate_annual: float = 0.0,
    start_capital: float = START_CAPITAL
) -> List[Dict[str, Any]]:
    """
    단일(전체) 구간 백테스트:
      1) Buy & Hold(항상 매수) 전략을 실행
      2) combos 내 여러 지표 파라미터 조합을 병렬로 백테스트
      3) 결과(성과 + 매매 로그)를 리스트(dict)로 반환

    Args:
        df (pd.DataFrame): 백테스트용 DataFrame (OHLCV + 지표)
        combos (List[List[Dict[str, Any]]]): 지표 파라미터 조합 목록
        timeframe (str): 예) "1d", "4h", "15m" 등
        risk_free_rate_annual (float, optional): 연간 무위험이자율 (샤프 계산용)
        start_capital (float, optional): 초기자본

    Returns:
        List[Dict[str, Any]]: 각 콤보와 Buy&Hold 결과가 담긴 리스트.
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
    if df.empty:
        return []

    # 구간 시작/끝 시점(UTC ms)을 KST 문자열로 변환 (로그용)
    start_ms = df.iloc[0]["open_time"]
    end_ms = df.iloc[-1]["open_time"]
    start_kst = ms_to_kst_str(start_ms)
    end_kst = ms_to_kst_str(end_ms)
    print(f"[INFO] No-Split({timeframe}) range: {start_kst} ~ {end_kst}, rows={len(df)}")

    results = []

    # 1) Buy & Hold (항상 매수) 전략
    bh_signals = [1] * len(df)
    bh_out = run_backtest(
        df=df,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_score = calculate_metrics(
        equity_curve=bh_out["equity_curve"],
        daily_returns=bh_out["daily_returns"],
        start_capital=bh_out["equity_curve"][0] if bh_out["equity_curve"] else start_capital,
        trades=bh_out["trades"],
        timeframe=timeframe,
        risk_free_rate_annual=risk_free_rate_annual
    )

    # 매매 내역 로그
    bh_trades_log = _record_trades_info(df, bh_out["trades"])

    # Buy & Hold 결과 저장
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
        """
        단일 콤보(복수 지표)로 백테스트하여 성과 및 매매 로그를 반환한다.
        """
        # 시그널 생성
        df_local = create_signals_for_combo(df, combo, out_col="signal_final")
        signals = df_local["signal_final"].tolist()

        # combo 내 buy_time_delay, sell_time_delay, holding_period 추출
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

        # 백테스트
        engine_out = run_backtest(
            df_local,
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
            start_capital=engine_out["equity_curve"][0] if engine_out["equity_curve"] else start_capital,
            trades=engine_out["trades"],
            timeframe=timeframe,
            risk_free_rate_annual=risk_free_rate_annual
        )

        # 매매 내역 로그
        combo_trades_log = _record_trades_info(df_local, engine_out["trades"])

        # used_indicators 필드에 combo 정보를 저장
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

    from strategies.signal_factory import create_signals_for_combo
    from joblib import Parallel, delayed

    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo_single)(combo) for combo in combos
    )
    results.extend(parallel_out)

    return results
