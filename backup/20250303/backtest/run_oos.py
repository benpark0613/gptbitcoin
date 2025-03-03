# gptbitcoin/backtest/run_oos.py

import json
from typing import List, Dict, Any

from joblib import Parallel, delayed

from config.config import ALLOW_SHORT
from strategies.signal_logic import (
    generate_signal_ma,
    generate_signal_rsi,
    generate_signal_filter,
    generate_signal_snr,
    generate_signal_channel_breakout,
    generate_signal_obv,
)
from analysis.scoring import calculate_metrics
from backtest.engine import run_backtest

def run_oos(
    df_oos,
    is_rows: List[Dict[str, Any]],
    timeframe: str,
    start_capital: float = 100000
) -> List[Dict[str, Any]]:
    if df_oos.empty:
        return is_rows

    oos_start_dt = df_oos.iloc[0]["datetime_utc"]
    oos_end_dt   = df_oos.iloc[-1]["datetime_utc"]
    print(f"[INFO] OOS ({timeframe}) range: {oos_start_dt} ~ {oos_end_dt} (rows={len(df_oos)})")

    # Buy & Hold
    bh_signals = [1]*len(df_oos)
    bh_result = run_backtest(
        df=df_oos,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_score = _calc_score(bh_result, start_capital)

    # B/H 성과
    for row in is_rows:
        if row.get("timeframe","") == f"{timeframe}(B/H)" and row.get("used_indicators","") == "Buy and Hold":
            row["oos_start_cap"]    = start_capital
            row["oos_end_cap"]      = bh_score["EndCapital"]
            row["oos_return"]       = bh_score["Return"]
            row["oos_trades"]       = bh_score["Trades"]
            row["oos_cagr"]         = bh_score["CAGR"]
            row["oos_sharpe"]       = bh_score["Sharpe"]
            row["oos_mdd"]          = bh_score["MDD"]
            row["oos_win_rate"]     = bh_score["WinRate"]
            row["oos_profit_factor"]        = bh_score["ProfitFactor"]
            row["oos_avg_holding_period"]   = bh_score["AvgHoldingPeriod"]
            row["oos_avg_pnl_per_trade"]    = bh_score["AvgPnlPerTrade"]
            break

    # is_passed=true만 OOS 테스트
    pass_indices = []
    for i, row in enumerate(is_rows):
        if row.get("used_indicators","") == "Buy and Hold":
            continue
        val = row.get("is_passed","False")
        if isinstance(val, bool):
            is_passed = val
        else:
            is_passed = (val.strip().lower()=="true")
        if is_passed:
            pass_indices.append(i)

    if not pass_indices:
        return is_rows

    results = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_oos_row)(i, is_rows, df_oos, timeframe, start_capital)
        for i in pass_indices
    )
    for idx, newrow in results:
        is_rows[idx] = newrow

    return is_rows

def _process_oos_row(
    idx: int,
    is_rows: List[Dict[str, Any]],
    df_oos,
    timeframe: str,
    start_capital: float
) -> (int, Dict[str, Any]):
    r = dict(is_rows[idx])
    used_str = r.get("used_indicators","")
    try:
        combo_json = json.loads(used_str)  # 순수 JSON
    except json.JSONDecodeError:
        return idx, r

    allow_short_oos = combo_json.get("allow_short", ALLOW_SHORT)
    combo_list      = combo_json.get("indicators", [])

    sigs = _merge_signals(df_oos, combo_list)
    engine_out = run_backtest(
        df=df_oos,
        signals=sigs,
        start_capital=start_capital,
        allow_short=allow_short_oos
    )
    score = _calc_score(engine_out, start_capital)

    trades_info = _record_trades_info(df_oos, engine_out["trades"])

    # OOS 거래기록을 별도 컬럼 "oos_trades"에 저장
    r["oos_trades_log"]          = trades_info
    r["oos_start_cap"]           = start_capital
    r["oos_end_cap"]             = score["EndCapital"]
    r["oos_return"]              = score["Return"]
    r["oos_trades"]              = score["Trades"]
    r["oos_cagr"]                = score["CAGR"]
    r["oos_sharpe"]              = score["Sharpe"]
    r["oos_mdd"]                 = score["MDD"]
    r["oos_win_rate"]            = score["WinRate"]
    r["oos_profit_factor"]       = score["ProfitFactor"]
    r["oos_avg_holding_period"]  = score["AvgHoldingPeriod"]
    r["oos_avg_pnl_per_trade"]   = score["AvgPnlPerTrade"]
    return idx, r


def _merge_signals(df, combo: List[Dict[str, Any]]) -> List[int]:
    length = len(df)
    merged = [0]*length
    for cfg in combo:
        t = cfg["indicator"]
        if t == "MA":
            s = generate_signal_ma(
                df=df,
                short_period=cfg["short_period"],
                long_period=cfg["long_period"],
                band_filter=cfg.get("band_filter",0.0)
            )
        elif t == "RSI":
            s = generate_signal_rsi(
                df=df,
                period=cfg["length"],
                overbought=cfg["overbought"],
                oversold=cfg["oversold"]
            )
        elif t == "Filter":
            s = generate_signal_filter(
                df=df,
                window=cfg["window"],
                x=cfg["x"],
                y=cfg["y"]
            )
        elif t == "Support_Resistance":
            s = generate_signal_snr(
                df=df,
                window=cfg["window"],
                band_pct=cfg["band_pct"]
            )
        elif t == "Channel_Breakout":
            s = generate_signal_channel_breakout(
                df=df,
                window=cfg["window"],
                c_value=cfg["c_value"]
            )
        elif t == "OBV":
            s = generate_signal_obv(
                df=df,
                short_period=cfg["short_period"],
                long_period=cfg["long_period"]
            )
        else:
            s = [0]*length

        for i in range(length):
            val = s.iloc[i] if hasattr(s, "iloc") else s[i]
            merged[i] += val

    final_sigs = []
    for val in merged:
        if val>0:
            final_sigs.append(1)
        elif val<0:
            final_sigs.append(-1)
        else:
            final_sigs.append(0)
    return final_sigs

def _calc_score(engine_out: Dict[str,Any], start_cap: float) -> Dict[str,float]:
    eq = engine_out["equity_curve"]
    rets = engine_out["daily_returns"]
    trades = engine_out["trades"]
    return calculate_metrics(eq, rets, start_cap, trades, days_in_test=len(eq))

def _record_trades_info(df, trades: List[Dict[str,Any]]) -> str:
    if not trades:
        return "No Trades"
    info_list = []
    for i, t in enumerate(trades, start=1):
        e_i = t.get("entry_index",None)
        x_i = t.get("exit_index",None)
        ptype = t.get("position_type","N/A")

        if 0<=e_i<len(df):
            edt = str(df.iloc[e_i]["datetime_utc"])
        else:
            edt = "N/A"
        if 0<=x_i<len(df):
            xdt = str(df.iloc[x_i]["datetime_utc"])
        else:
            xdt = "End"
        info_list.append(f"[{i}] {ptype.upper()} Entry={edt}, Exit={xdt}")
    return "; ".join(info_list)
