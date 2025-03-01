# gptbitcoin/backtest/run_best.py
# 구글 스타일 Docstring, 최소한의 한글 주석
# 단 하나의 파라미터 조합(Combo)을 대상으로
# 전체 구간에 백테스트 후 성과지표 및 엔진 결과를 반환한다.

from typing import List, Dict, Any
import pandas as pd

from backtest.engine import run_backtest
from analysis.scoring import calculate_metrics
from strategies.signal_logic import (
    generate_signal_ma,
    generate_signal_rsi,
    generate_signal_filter,
    generate_signal_snr,
    generate_signal_channel_breakout,
    generate_signal_obv
)

def run_best_combo(
    df: pd.DataFrame,
    best_combo: List[Dict[str, Any]],
    start_capital: float,
    allow_short: bool
) -> Dict[str, Any]:
    """
    단 하나의 인디케이터 조합(best_combo)에 대해 백테스트를 실행하고 성과지표를 반환한다.

    Args:
        df (pd.DataFrame): 백테스트에 사용할 전체 구간의 OHLCV+지표 DataFrame.
            - 최소 열: ["datetime_utc","open","high","low","close","volume"]와
              strategies.signal_logic에서 사용하는 지표 컬럼
        best_combo (List[Dict[str, Any]]): 인디케이터 설정(복수의 인디케이터를 하나의 콤보로 묶은 리스트)
            예) [
                  {"indicator":"OBV","short_period":10,"long_period":50},
                  {"indicator":"RSI","length":14,"overbought":70,"oversold":30}
                ]
        start_capital (float): 초기자본
        allow_short (bool): 숏 포지션 허용 여부

    Returns:
        Dict[str, Any]:
            {
              "engine_out": {
                 "equity_curve": [...],
                 "daily_returns": [...],
                 "trades": [...]
               },
              "score": {
                 "StartCapital": ...,
                 "EndCapital": ...,
                 "Return": ...,
                 "CAGR": ...,
                 "Sharpe": ...,
                 "MDD": ...,
                 "Trades": ...,
                 "WinRate": ...,
                 "ProfitFactor": ...,
                 "AvgHoldingPeriod": ...,
                 "AvgPnlPerTrade": ...,
                 ...
               }
            }
    """
    signals = _merge_signals(df, best_combo)
    engine_out = run_backtest(
        df=df,
        signals=signals,
        start_capital=start_capital,
        allow_short=allow_short
    )

    score = calculate_metrics(
        equity_curve=engine_out["equity_curve"],
        daily_returns=engine_out["daily_returns"],
        start_capital=start_capital,
        trades=engine_out["trades"],
        days_in_test=len(engine_out["equity_curve"])
    )

    return {
        "engine_out": engine_out,
        "score": score
    }


def _merge_signals(df: pd.DataFrame, combo: List[Dict[str, Any]]) -> List[int]:
    """
    여러 인디케이터 설정을 합산하여 최종 시그널을 만든다. (+1 / -1 / 0)

    Args:
        df (pd.DataFrame): 지표가 포함된 DataFrame
        combo (List[Dict[str, Any]]): 인디케이터 설정 리스트
            예) [{"indicator":"OBV","short_period":5,"long_period":30}, ...]

    Returns:
        List[int]: 인디케이터 시그널 합산 후 +1/0/-1 값의 리스트
    """
    length = len(df)
    merged = [0] * length

    for cfg in combo:
        itype = cfg.get("indicator", "")
        if itype == "MA":
            sig = generate_signal_ma(
                df=df,
                short_period=cfg["short_period"],
                long_period=cfg["long_period"],
                band_filter=cfg.get("band_filter", 0.0)
            )
        elif itype == "RSI":
            sig = generate_signal_rsi(
                df=df,
                period=cfg["length"],
                overbought=cfg["overbought"],
                oversold=cfg["oversold"]
            )
        elif itype == "Filter":
            sig = generate_signal_filter(
                df=df,
                window=cfg["window"],
                x=cfg["x"],
                y=cfg["y"]
            )
        elif itype == "Support_Resistance":
            sig = generate_signal_snr(
                df=df,
                window=cfg["window"],
                band_pct=cfg["band_pct"]
            )
        elif itype == "Channel_Breakout":
            sig = generate_signal_channel_breakout(
                df=df,
                window=cfg["window"],
                c_value=cfg["c_value"]
            )
        elif itype == "OBV":
            sig = generate_signal_obv(
                df=df,
                short_period=cfg["short_period"],
                long_period=cfg["long_period"]
            )
        else:
            sig = [0] * length

        # 시그널 합산
        for i in range(length):
            val = sig.iloc[i] if hasattr(sig, "iloc") else sig[i]
            merged[i] += val

    # 최종 시그널: 합산 결과 > 0 => +1, < 0 => -1, = 0 => 0
    final_signals = []
    for val in merged:
        if val > 0:
            final_signals.append(1)
        elif val < 0:
            final_signals.append(-1)
        else:
            final_signals.append(0)

    return final_signals
