# gptbitcoin/backtest/run_oos.py
# 구글 스타일, 최소한의 한글 주석
# IS(인샘플)에서 통과한(is_passed=True) 전략만 OOS(아웃샘플)에서 재검증 후,
# 그 결과를 is_rows에 덧붙여 반환한다.

import json
from typing import List, Dict, Any

from joblib import Parallel, delayed

from utils.date_time import ms_to_kst_str

try:
    from config.config import (
        ALLOW_SHORT,
        START_CAPITAL
    )
except ImportError:
    raise ImportError("config.py에서 ALLOW_SHORT, START_CAPITAL를 가져올 수 없습니다.")

try:
    from backtest.engine import run_backtest
except ImportError:
    raise ImportError("engine.py를 찾을 수 없거나 경로가 잘못되었습니다.")

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
    raise ImportError("signal_logic.py에서 시그널 함수를 가져올 수 없습니다.")

try:
    from analysis.scoring import calculate_metrics
except ImportError:
    raise ImportError("scoring.py에서 calculate_metrics 함수를 가져올 수 없습니다.")


def run_oos(
    df_oos,
    is_rows: List[Dict[str, Any]],
    timeframe: str,
    start_capital: float = START_CAPITAL
) -> List[Dict[str, Any]]:
    """
    OOS(아웃샘플) 백테스트:
    1) Buy & Hold(OOS) 결과를 is_rows의 BH 항목에 기록
    2) is_passed=True인 전략만 병렬로 OOS 구간 재검증
    3) 그 결과를 is_rows에 갱신(oos_* 컬럼)

    Args:
        df_oos (pd.DataFrame): OOS 구간 데이터(지표 포함)
        is_rows (List[Dict[str,Any]]): run_is 결과(인샘플 결과) 목록
        timeframe (str): "1d", "4h" 등
        start_capital (float): OOS 시작 자본

    Returns:
        List[Dict[str,Any]]: OOS 결과가 반영된 is_rows
    """
    if df_oos.empty:
        return is_rows

    # 로그 출력
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

    # is_rows 내 Buy and Hold 항목에 OOS 결과 업데이트
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
            row["oos_trades_log"] = "N/A"  # B/H는 거래 내역 별도 저장 안 함
            break

    # 2) is_passed=True인 전략만 OOS 적용
    pass_indices = []
    for i, row in enumerate(is_rows):
        if row.get("used_indicators", "") == "Buy and Hold":
            continue
        val = row.get("is_passed", "False")
        is_ok = (val is True) if isinstance(val, bool) else (val.strip().lower() == "true")
        if is_ok:
            pass_indices.append(i)

    if not pass_indices:
        return is_rows

    # 병렬 실행
    results = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_oos_row)(i, is_rows, df_oos, timeframe, start_capital)
        for i in pass_indices
    )

    for idx, new_row in results:
        is_rows[idx] = new_row

    return is_rows


def _process_oos_row(
    idx: int,
    is_rows: List[Dict[str, Any]],
    df_oos,
    timeframe: str,
    start_capital: float
) -> (int, Dict[str, Any]):
    """
    is_rows[idx]의 used_indicators(=JSON)에서 콤보 파라미터를 파싱,
    OOS 구간에서 백테스트 후 oos_* 결과를 갱신한다.
    """
    row_copy = dict(is_rows[idx])
    used_str = row_copy.get("used_indicators", "")

    try:
        combo_info = json.loads(used_str)
    except json.JSONDecodeError:
        # JSON 형식이 아니면 OOS 불가
        return idx, row_copy

    allow_short_oos = combo_info.get("allow_short", ALLOW_SHORT)
    combo_params = combo_info.get("combo_params", [])

    # 콤보 파라미터 기반으로 OOS 시그널 생성
    signals = _create_signals_for_oos(df_oos, combo_params)

    # 백테스트
    engine_out = run_backtest(
        df=df_oos,
        signals=signals,
        start_capital=start_capital,
        allow_short=allow_short_oos
    )

    # 성과 계산
    score = calculate_metrics(
        equity_curve=engine_out["equity_curve"],
        daily_returns=engine_out["daily_returns"],
        start_capital=start_capital,
        trades=engine_out["trades"],
        timeframe=timeframe
    )

    trades_log = _record_trades_info(df_oos, engine_out["trades"])

    # row 업데이트
    row_copy["oos_start_cap"] = start_capital
    row_copy["oos_end_cap"] = score["EndCapital"]
    row_copy["oos_return"] = score["Return"]
    row_copy["oos_trades"] = score["Trades"]
    row_copy["oos_sharpe"] = score["Sharpe"]
    row_copy["oos_mdd"] = score["MDD"]
    row_copy["oos_trades_log"] = trades_log

    return idx, row_copy


def _create_signals_for_oos(df, combo_params: List[Dict[str, Any]]) -> List[int]:
    """
    combo_params로부터 OOS 구간 시그널을 생성.
    필수 파라미터 누락 시 예외 발생.
    """
    df_local = df.copy()
    temp_cols = []

    for i, param in enumerate(combo_params):
        t = param.get("type", None)
        if not t:
            raise ValueError("지표 'type'이 지정되지 않았습니다.")

        col_name = f"temp_oos_signal_{i}"
        df_local[col_name] = 0

        if t == "MA":
            if "short_period" not in param:
                raise ValueError("MA 지표에 'short_period'가 없습니다.")
            if "long_period" not in param:
                raise ValueError("MA 지표에 'long_period'가 없습니다.")
            sp = param["short_period"]
            lp = param["long_period"]
            df_local = ma_crossover_signal(
                df_local,
                short_ma_col=f"ma_{sp}",
                long_ma_col=f"ma_{lp}",
                signal_col=col_name
            )

        elif t == "RSI":
            if "length" not in param:
                raise ValueError("RSI 지표에 'length'가 없습니다.")
            if "overbought" not in param or "oversold" not in param:
                raise ValueError("RSI 지표에 'overbought' / 'oversold'가 필요합니다.")
            length = param["length"]
            ob = param["overbought"]
            os_ = param["oversold"]
            df_local = rsi_signal(
                df_local,
                rsi_col=f"rsi_{length}",
                lower_bound=os_,
                upper_bound=ob,
                signal_col=col_name
            )

        elif t == "OBV":
            df_local = obv_signal(
                df_local,
                obv_col="obv_raw",
                threshold=0.0,
                signal_col=col_name
            )

        elif t == "Filter":
            if "window" not in param:
                raise ValueError("Filter 지표에 'window'가 없습니다.")
            if "x_pct" not in param or "y_pct" not in param:
                raise ValueError("Filter 지표에 'x_pct','y_pct'가 필요합니다.")
            w = param["window"]
            x_ = param["x_pct"]
            y_ = param["y_pct"]
            df_local = filter_rule_signal(
                df_local,
                window=w,
                x_pct=x_,
                y_pct=y_,
                signal_col=col_name
            )

        elif t == "Support_Resistance":
            if "window" not in param:
                raise ValueError("Support_Resistance 지표에 'window'가 없습니다.")
            if "band_pct" not in param:
                raise ValueError("Support_Resistance 지표에 'band_pct'가 없습니다.")
            w = param["window"]
            bp = param["band_pct"]
            df_local = support_resistance_signal(
                df_local,
                rolling_min_col=f"sr_min_{w}",
                rolling_max_col=f"sr_max_{w}",
                band_pct=bp,
                signal_col=col_name
            )

        elif t == "Channel_Breakout":
            if "window" not in param:
                raise ValueError("Channel_Breakout 지표에 'window'가 없습니다.")
            if "c_value" not in param:
                raise ValueError("Channel_Breakout 지표에 'c_value'가 없습니다.")
            w = param["window"]
            c_ = param["c_value"]
            df_local = channel_breakout_signal(
                df_local,
                rolling_min_col=f"ch_min_{w}",
                rolling_max_col=f"ch_max_{w}",
                breakout_pct=c_,
                signal_col=col_name
            )

        else:
            raise ValueError(f"지원되지 않는 지표 타입입니다: {t}")

        temp_cols.append(col_name)

    df_local = combine_signals(
        df_local,
        signal_cols=temp_cols,
        out_col="signal_oos_final"
    )
    signals = df_local["signal_oos_final"].tolist()
    return signals


def _record_trades_info(df, trades: List[Dict[str, Any]]) -> str:
    """
    OOS 구간에서 발생한 트레이드 목록을 간단히 요약해 문자열로 반환.
    """
    if not trades:
        return "No Trades"

    logs = []
    for i, t in enumerate(trades, start=1):
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)
        ptype = t.get("position_type", "N/A")

        if isinstance(e_idx, int) and 0 <= e_idx < len(df):
            ms_val = df.iloc[e_idx]["open_time"]
            entry_ot = ms_to_kst_str(ms_val)
        else:
            entry_ot = "N/A"

        if isinstance(x_idx, int) and 0 <= x_idx < len(df):
            exit_ot = df.iloc[x_idx]["open_time"]
        else:
            exit_ot = "End"

        logs.append(f"[{i}] {ptype.upper()} Entry={entry_ot}, Exit={exit_ot}")

    return "; ".join(logs)
