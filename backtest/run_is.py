# gptbitcoin/backtest/run_is.py

import json
from typing import List, Dict, Any
from datetime import datetime

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

def run_is(
    df_is,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    start_capital: float = 100000
) -> List[Dict[str, Any]]:
    if df_is.empty:
        return []

    is_start_dt = df_is.iloc[0]["datetime_utc"]
    is_end_dt   = df_is.iloc[-1]["datetime_utc"]
    print(f"[INFO] IS ({timeframe}) range: {is_start_dt} ~ {is_end_dt} (rows={len(df_is)})")

    # Buy & Hold
    bh_signals = [1]*len(df_is)
    bh_result = run_backtest(
        df=df_is,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_score = _calc_score(bh_result, start_capital)
    bh_return = bh_score["Return"]

    bh_row = {
        "timeframe": f"{timeframe}(B/H)",
        "is_start_cap": start_capital,
        "is_end_cap": bh_score["EndCapital"],
        "is_return": bh_score["Return"],
        "is_trades": bh_score["Trades"],
        "used_indicators": "Buy and Hold",
        "is_passed": "N/A"
    }

    def _process_combo(combo_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        signals = _merge_signals(df_is, combo_list)
        engine_out = run_backtest(
            df=df_is,
            signals=signals,
            start_capital=start_capital,
            allow_short=ALLOW_SHORT
        )
        score = _calc_score(engine_out, start_capital)
        pass_bool = (score["Return"] >= bh_return)

        # JSON 직렬화 (IS 거래 기록은 저장하지 않음)
        combo_plus_short = {
            "allow_short": ALLOW_SHORT,
            "indicators": combo_list
        }
        combo_json = json.dumps(combo_plus_short, ensure_ascii=False)

        row = {
            "timeframe": timeframe,
            "is_start_cap": start_capital,
            "is_end_cap": score["EndCapital"],
            "is_return": score["Return"],
            "is_trades": score["Trades"],
            "used_indicators": combo_json,  # 순수 JSON
            "is_passed": "True" if pass_bool else "False"
        }
        return row

    results = [bh_row]

    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo)(combo) for combo in combos
    )
    results.extend(parallel_out)

    return results

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
                band_filter=cfg.get("band_filter", 0.0)
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

    final_signals = []
    for val in merged:
        if val > 0:
            final_signals.append(1)
        elif val < 0:
            final_signals.append(-1)
        else:
            final_signals.append(0)
    return final_signals

def _calc_score(engine_out: Dict[str, Any], start_cap: float) -> Dict[str, float]:
    eq = engine_out["equity_curve"]
    rets = engine_out["daily_returns"]
    trades = engine_out["trades"]
    return calculate_metrics(eq, rets, start_cap, trades, days_in_test=len(eq))
