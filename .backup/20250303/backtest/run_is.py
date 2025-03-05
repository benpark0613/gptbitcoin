# gptbitcoin/backtest/run_is.py
# 구글 스타일, 최소한의 한글 주석
# IS(In-Sample) 단계에서 시장수익률 외에도 Sharpe, MDD 조건을 추가로 필터링하고,
# CSV 기록 시 is_sharpe, is_mdd 컬럼도 함께 저장하도록 값을 row에 추가한다.

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

def run_is(
    df_is,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    start_capital: float = 100000
) -> List[Dict[str, Any]]:
    """
    In-Sample(IS) 백테스트:
      1) Buy & Hold 수익률 계산
      2) combos를 병렬로 백테스트 실행
      3) 시장수익률 + Sharpe + MDD 등을 만족하면 is_passed=True로 설정
      4) 각 조합의 Sharpe, MDD를 row에 담아 CSV 기록에 활용
    """
    if df_is.empty:
        return []

    is_start_dt = df_is.iloc[0]["datetime_utc"]
    is_end_dt   = df_is.iloc[-1]["datetime_utc"]
    print(f"[INFO] IS ({timeframe}) range: {is_start_dt} ~ {is_end_dt} (rows={len(df_is)})")

    # 1) Buy & Hold
    bh_signals = [1]*len(df_is)
    bh_result = run_backtest(
        df=df_is,
        signals=bh_signals,
        start_capital=start_capital,
        allow_short=False
    )
    bh_score = _calc_score(bh_result, start_capital)
    bh_return = bh_score["Return"]
    bh_sharpe = bh_score["Sharpe"]
    bh_mdd = bh_score["MDD"]

    bh_row = {
        "timeframe": f"{timeframe}(B/H)",
        "is_start_cap": start_capital,
        "is_end_cap": bh_score["EndCapital"],
        "is_return": bh_score["Return"],
        "is_trades": bh_score["Trades"],
        # 추가: BH Sharpe, BH MDD
        "is_sharpe": bh_score["Sharpe"],
        "is_mdd": bh_score["MDD"],
        "used_indicators": "Buy and Hold",
        "is_passed": "N/A"
    }

    def _process_combo(combo_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        combos 내 인디케이터 조합 하나를 받아 IS 백테스트 후
        시장수익률, Sharpe, MDD를 충족하면 is_passed=True.
        """
        signals = _merge_signals(df_is, combo_list)
        engine_out = run_backtest(
            df=df_is,
            signals=signals,
            start_capital=start_capital,
            allow_short=ALLOW_SHORT
        )
        score = _calc_score(engine_out, start_capital)

        # 2) 추가 필터링 조건 예시:
        #    (1) 전략 수익 >= 시장수익
        #    (2) Sharpe >= 1.0
        #    (3) MDD <= 0.3
        # pass_bool = (
        #     (score["Return"] >= bh_return) and
        #     (score["Sharpe"] >= 1.0) and
        #     (score["MDD"] <= 0.5)
        # )

        pass_bool = (
            (score["Return"] >= bh_return) and
            (score["Sharpe"] >= bh_sharpe) and
            (score["MDD"] <= bh_mdd)
        )

        # JSON 직렬화 (IS 때 거래 기록은 저장하지 않음)
        combo_plus_short = {
            "timeframe": timeframe,
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
            # 추가: is_sharpe, is_mdd
            "is_sharpe": score["Sharpe"],
            "is_mdd": score["MDD"],
            "used_indicators": combo_json,
            "is_passed": "True" if pass_bool else "False"
        }
        return row

    results = [bh_row]

    # 병렬 실행
    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo)(combo) for combo in combos
    )
    results.extend(parallel_out)

    return results

def _merge_signals(df, combo: List[Dict[str, Any]]) -> List[int]:
    """
    여러 인디케이터 설정을 합산하여 최종 시그널을 만든다. (+1 / 0 / -1)
    """
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
    """
    백테스트 결과를 바탕으로 성과지표(Sharpe, MDD 등)를 포함해 계산한다.
    """
    eq = engine_out["equity_curve"]
    rets = engine_out["daily_returns"]
    trades = engine_out["trades"]
    return calculate_metrics(eq, rets, start_cap, trades, days_in_test=len(eq))
