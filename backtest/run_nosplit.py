# gptbitcoin/backtest/run_nosplit.py
# 구글 스타일 docstring, 최소한의 한글 주석
# 단일 구간(인샘플·아웃샘플 구분 없음) 백테스트 모듈.
# - Buy & Hold 전략 + 여러 지표 콤보 전략을 모두 테스트
# - trades_log(매매 로그)를 함께 기록해 CSV 등에 저장 가능

import json
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from joblib import Parallel, delayed

try:
    from backtest.engine import run_backtest
except ImportError:
    raise ImportError("[run_nosplit.py] engine.py import 오류")

try:
    from analysis.scoring import calculate_metrics
except ImportError:
    raise ImportError("[run_nosplit.py] scoring.py import 오류")

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
    raise ImportError("[run_nosplit.py] signal_logic.py import 오류")


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
        df (pd.DataFrame): 백테스트용 DF (OHLCV+보조지표, 'close', 'ma_5', 'rsi_14', etc.)
        combos (List[List[Dict[str, Any]]]): 지표 파라미터 조합의 리스트. 예:
            [
              [ {"type":"MA","short_period":5,"long_period":20}, {"type":"RSI","length":14} ],
              [ {"type":"OBV","short_period":5,"long_period":30} ],
              ...
            ]
        timeframe (str): 예) "1d", "4h" (연환산 샤프 계산용)
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
                "trades_log": str  # 매매 내역 요약
              },
              ...
            ]
    """
    results = []

    # 1) Buy & Hold 먼저 계산
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
        단일 콤보(여러 지표 dict)로 매매 시그널 → 백테스트 → 성과 + 매매로그 생성
        """
        signals = _create_signals_for_combo(df, combo)
        engine_out = run_backtest(df, signals=signals)
        score = calculate_metrics(
            equity_curve=engine_out["equity_curve"],
            daily_returns=engine_out["daily_returns"],
            start_capital=engine_out["equity_curve"][0],
            trades=engine_out["trades"],
            timeframe=timeframe,
            risk_free_rate_annual=risk_free_rate_annual
        )

        combo_info = {"timeframe": timeframe, "combo_params": combo}
        used_str = json.dumps(combo_info, ensure_ascii=False)

        combo_trades_log = _record_trades_info(df, engine_out["trades"])

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


def _create_signals_for_combo(
    df: pd.DataFrame,
    combo: List[Dict[str, Any]]
) -> List[int]:
    """
    combos 내 지표 설정대로 매매 시그널 생성.
    한 콤보에 여러 지표 dict가 있을 수 있으므로, 시그널 합산(1/-1/0).
    """
    df_local = df.copy()
    temp_signal_cols = []

    for i, param in enumerate(combo):
        ttype = param.get("type")
        if not ttype:
            raise ValueError("지표 'type'이 지정되지 않았습니다.")

        signal_col = f"temp_sig_{i}"
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
            # calc_all_indicators에서 obv_raw가 계산됐다고 가정
            df_local = obv_signal(
                df_local,
                obv_col="obv_raw",
                threshold=0.0,
                signal_col=signal_col
            )
        elif ttype == "Filter":
            w = param["window"]
            x_ = param["x_pct"]
            y_ = param["y_pct"]
            df_local = filter_rule_signal(
                df_local,
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
    매매 내역(trades)을 간단히 문자열로 요약.
    - df: 백테스트 시 사용된 시계열(OHLCV) DF (open_time 등)
    - trades: run_backtest 결과물 중 "trades"
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index")
        x_idx = t.get("exit_index")
        ptype = t.get("position_type", "N/A")
        pnl = t.get("pnl", 0.0)

        # entry_time, exit_time (KST or UTC, 여기서는 DF의 open_time=UTC ms)
        entry_time_str = "N/A"
        exit_time_str = "N/A"

        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_entry = df.iloc[e_idx]["open_time"]
            entry_dt = datetime.utcfromtimestamp(ms_entry / 1000.0)
            entry_time_str = entry_dt.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_exit = df.iloc[x_idx]["open_time"]
            exit_dt = datetime.utcfromtimestamp(ms_exit / 1000.0)
            exit_time_str = exit_dt.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(x_idx, int) and x_idx >= len(df):
            # exit_index가 df 범위보다 1큰 경우 = 마지막 봉에서 청산
            exit_time_str = "End"

        logs.append(
            f"[{i}] {ptype.upper()} "
            f"Entry={entry_time_str}, Exit={exit_time_str}, PnL={pnl:.2f}"
        )

    return "; ".join(logs)
