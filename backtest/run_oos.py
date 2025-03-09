# gptbitcoin/backtest/run_oos.py
# 구글 스타일, 최소한의 한글 주석
# IS(인샘플)에서 통과한(is_passed=True) 전략만 OOS(아웃샘플)에서 재검증 후,
# 그 결과를 is_rows에 반영해 반환한다.

import json
from typing import List, Dict, Any, Tuple

from joblib import Parallel, delayed

from analysis.scoring import calculate_metrics
from backtest.engine import run_backtest
from config.config import (
    ALLOW_SHORT,
    START_CAPITAL
)
from utils.date_time import ms_to_kst_str

# signal_factory에서 create_signals_for_combo 함수를 import
from strategies.signal_factory import create_signals_for_combo


def run_oos(
    df_oos,
    is_rows: List[Dict[str, Any]],
    timeframe: str,
    start_capital: float = START_CAPITAL
) -> List[Dict[str, Any]]:
    """
    OOS(아웃샘플) 백테스트:
      1) Buy & Hold(OOS) 결과를 is_rows 중 B/H 항목에 기록
      2) is_passed=True인 전략만 OOS 구간 재검증
      3) 그 결과를 is_rows에 갱신(oos_* 컬럼)

    Args:
        df_oos (pd.DataFrame): OOS 구간 데이터(지표 포함)
        is_rows (List[Dict[str,Any]]): run_is 결과(인샘플 성과) 목록
        timeframe (str): 예) "1d", "4h" 등
        start_capital (float, optional): OOS 시작 자본

    Returns:
        List[Dict[str, Any]]: OOS 결과가 반영된 is_rows
    """
    if df_oos.empty:
        return is_rows

    oos_start_ms = df_oos.iloc[0]["open_time"]
    oos_end_ms = df_oos.iloc[-1]["open_time"]
    oos_start_kst = ms_to_kst_str(oos_start_ms)
    oos_end_kst = ms_to_kst_str(oos_end_ms)

    print(f"[INFO] OOS({timeframe}) range: {oos_start_kst} ~ {oos_end_kst}, rows={len(df_oos)}")

    # 1) Buy & Hold (OOS)
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

    # is_rows 중 B/H 항목(used_indicators="Buy and Hold")에 OOS 결과 반영
    for row in is_rows:
        tf_str = row.get("timeframe", "")
        used_str = row.get("used_indicators", "")
        if tf_str == f"{timeframe}(B/H)" and used_str == "Buy and Hold":
            row["oos_start_cap"] = start_capital
            row["oos_end_cap"] = bh_score["EndCapital"]
            row["oos_return"] = bh_score["Return"]
            row["oos_trades"] = bh_score["Trades"]
            row["oos_sharpe"] = bh_score["Sharpe"]
            row["oos_mdd"] = bh_score["MDD"]
            row["oos_trades_log"] = "N/A"  # B/H는 매매내역 굳이 기록X
            break

    # 2) is_passed=True인 전략만 OOS 백테스트
    pass_indices = []
    for i, row in enumerate(is_rows):
        if row.get("used_indicators", "") == "Buy and Hold":
            continue
        val = row.get("is_passed", "False")
        # "True" or bool(True) -> 통과
        is_ok = (val is True) if isinstance(val, bool) else (val.strip().lower() == "true")
        if is_ok:
            pass_indices.append(i)

    if not pass_indices:
        # 통과된 전략이 없다면 그대로 반환
        return is_rows

    # 병렬 실행
    results = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_oos_row)(idx, is_rows, df_oos, timeframe, start_capital)
        for idx in pass_indices
    )

    # 결과 반영
    for idx, new_row in results:
        is_rows[idx] = new_row

    return is_rows


def _process_oos_row(
    idx: int,
    is_rows: List[Dict[str, Any]],
    df_oos,
    timeframe: str,
    start_capital: float
) -> Tuple[int, Dict[str, Any]]:
    """
    is_rows[idx]의 used_indicators(=JSON)에서 콤보 파라미터를 파싱 후,
    df_oos 구간에 대해 백테스트. 그 결과를 oos_* 컬럼에 기록.
    """
    row_copy = dict(is_rows[idx])  # 원본 손상 방지
    used_str = row_copy.get("used_indicators", "")

    try:
        combo_info = json.loads(used_str)
    except json.JSONDecodeError:
        # JSON 형식이 아니면 OOS 불가
        return idx, row_copy

    allow_short_oos = combo_info.get("allow_short", ALLOW_SHORT)
    combo_params = combo_info.get("combo_params", [])

    # 시그널 생성(Out-of-Sample 용)
    # out_col="signal_oos_final"로 OOS 시그널만 별도 저장
    df_local = df_oos.copy()
    df_local = create_signals_for_combo(
        df_local,
        combo_params,
        out_col="signal_oos_final"
    )
    signals = df_local["signal_oos_final"].tolist()

    # 백테스트
    engine_out = run_backtest(
        df=df_local,
        signals=signals,
        start_capital=start_capital,
        allow_short=allow_short_oos
    )
    score = calculate_metrics(
        equity_curve=engine_out["equity_curve"],
        daily_returns=engine_out["daily_returns"],
        start_capital=start_capital,
        trades=engine_out["trades"],
        timeframe=timeframe
    )

    # 매매내역 요약
    trades_log = _record_trades_info(df_local, engine_out["trades"])

    # oos_* 칼럼에 기록
    row_copy["oos_start_cap"] = start_capital
    row_copy["oos_end_cap"] = score["EndCapital"]
    row_copy["oos_return"] = score["Return"]
    row_copy["oos_trades"] = score["Trades"]
    row_copy["oos_sharpe"] = score["Sharpe"]
    row_copy["oos_mdd"] = score["MDD"]
    row_copy["oos_trades_log"] = trades_log

    return idx, row_copy


def _record_trades_info(df, trades: List[Dict[str, Any]]) -> str:
    """
    OOS 구간에서 발생한 트레이드 목록을 문자열로 요약 반환.
    (entry, exit를 KST로 변환해 표시)
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")

        # entry 시각 KST 변환
        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_val = df.iloc[e_idx]["open_time"]
            entry_ot = ms_to_kst_str(ms_val)
        else:
            entry_ot = "N/A"

        # exit 시각 KST 변환
        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            ms_val_exit = df.iloc[x_idx]["open_time"]
            exit_ot = ms_to_kst_str(ms_val_exit)
        else:
            exit_ot = "End"

        logs.append(f"[{i}] {ptype.upper()} Entry={entry_ot}, Exit={exit_ot}")

    return "; ".join(logs)
