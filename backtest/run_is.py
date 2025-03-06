# gptbitcoin/backtest/run_is.py
# 구글 스타일, 최소한의 한글 주석
# In-Sample(IS) 구간에서 여러 지표 조합(콤보)을 테스트 후,
# 시장수익률 대비 비교하고 is_passed 여부를 결정하여 리스트로 반환.

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
    raise ImportError("engine.py 파일을 찾을 수 없거나 경로가 잘못되었습니다.")

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


def run_is(
    df_is,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    start_capital: float = START_CAPITAL
) -> List[Dict[str, Any]]:
    """
    In-Sample(IS) 백테스트:
      1) Buy & Hold 수익률(=항상 매수)로 시장수익률을 측정.
      2) combos 내 지표 조합별 백테스트를 병렬 실행.
      3) 시장수익률과 Sharpe, MDD 등을 비교 후 is_passed 결정.
      4) 각 조합 결과를 리스트(dict)로 반환.

    Args:
        df_is (pd.DataFrame): IS 구간 시계열 데이터(지표 포함).
        combos (List[List[Dict[str, Any]]]):
            [
              [ {"type":"MA","short_period":5,"long_period":20}, {"type":"RSI","length":14,...} ],
              [ {"type":"OBV","short_period":5,"long_period":30} ],
              ...
            ]
        timeframe (str): 예) "1d", "4h" 등
        start_capital (float): 초기 자본

    Returns:
        List[Dict[str, Any]]:
            [
              {
                "timeframe": ...,
                "is_start_cap": ...,
                "is_end_cap": ...,
                "is_return": ...,
                "is_trades": ...,
                "is_sharpe": ...,
                "is_mdd": ...,
                "used_indicators": ...,
                "is_passed": "True"/"False"/"N/A"
              }, ...
            ]
    """
    if df_is.empty:
        return []

    # 간단 로그
    is_start_ms = df_is.iloc[0]["open_time"]
    is_end_ms = df_is.iloc[-1]["open_time"]

    # UTC ms -> KST 문자열
    is_start_kst = ms_to_kst_str(is_start_ms)
    is_end_kst = ms_to_kst_str(is_end_ms)

    print(f"[INFO] IS({timeframe}) range: {is_start_kst} ~ {is_end_kst}, rows={len(df_is)}")

    # 1) Buy & Hold 결과
    bh_signals = [1] * len(df_is)
    bh_result = run_backtest(
        df=df_is,
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
    bh_return = bh_score["Return"]
    bh_sharpe = bh_score["Sharpe"]
    bh_mdd = bh_score["MDD"]

    bh_row = {
        "timeframe": f"{timeframe}(B/H)",
        "is_start_cap": bh_score["StartCapital"],
        "is_end_cap": bh_score["EndCapital"],
        "is_return": bh_score["Return"],
        "is_trades": bh_score["Trades"],
        "is_sharpe": bh_score["Sharpe"],
        "is_mdd": bh_score["MDD"],
        "used_indicators": "Buy and Hold",
        "is_passed": "N/A"
    }

    def _process_combo(combo: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        주어진 지표 콤보(여러 dict)를 이용해 IS 구간 백테스트.
        수익률 >= BH, Sharpe >= BH, MDD <= BH 면 is_passed=True.
        """
        signals = _create_signals_for_combo(df_is, combo)
        engine_out = run_backtest(
            df=df_is,
            signals=signals,
            start_capital=start_capital,
            allow_short=ALLOW_SHORT
        )
        score = calculate_metrics(
            equity_curve=engine_out["equity_curve"],
            daily_returns=engine_out["daily_returns"],
            start_capital=start_capital,
            trades=engine_out["trades"],
            timeframe=timeframe
        )

        pass_bool = (
            (score["Return"] >= bh_return) and
            (score["Sharpe"] >= bh_sharpe) and
            (score["MDD"] <= bh_mdd)
        )

        combo_info = {
            "timeframe": timeframe,
            "combo_params": combo
        }
        combo_json = json.dumps(combo_info, ensure_ascii=False)

        row = {
            "timeframe": timeframe,
            "is_start_cap": score["StartCapital"],
            "is_end_cap": score["EndCapital"],
            "is_return": score["Return"],
            "is_trades": score["Trades"],
            "is_sharpe": score["Sharpe"],
            "is_mdd": score["MDD"],
            "used_indicators": combo_json,
            "is_passed": "True" if pass_bool else "False"
        }
        return row

    results = [bh_row]
    from joblib import Parallel, delayed
    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo)(combo) for combo in combos
    )
    results.extend(parallel_out)
    return results


def _create_signals_for_combo(df, combo: List[Dict[str, Any]]) -> List[int]:
    """
    combo의 각 지표 설정에 맞춰 시그널을 생성하고, 합산하여 최종 시그널 리스트(1/-1/0)를 반환.
    short_period 등의 필수 파라미터가 없으면 예외 발생.
    """
    df_local = df.copy()
    temp_signal_cols = []

    for i, param in enumerate(combo):
        indicator_type = param.get("type", None)
        if not indicator_type:
            raise ValueError("지표 'type'이 지정되지 않았습니다.")

        signal_col = f"temp_signal_{i}"
        df_local[signal_col] = 0

        if indicator_type == "MA":
            # short_period, long_period는 필수
            if "short_period" not in param:
                raise ValueError("MA 지표에 'short_period'가 없습니다.")
            if "long_period" not in param:
                raise ValueError("MA 지표에 'long_period'가 없습니다.")
            short_p = param["short_period"]
            long_p = param["long_period"]

            df_local = ma_crossover_signal(
                df_local,
                short_ma_col=f"ma_{short_p}",
                long_ma_col=f"ma_{long_p}",
                signal_col=signal_col
            )

        elif indicator_type == "RSI":
            if "length" not in param:
                raise ValueError("RSI 지표에 'length'가 없습니다.")
            if "overbought" not in param or "oversold" not in param:
                raise ValueError("RSI 지표에 'overbought' / 'oversold' 파라미터가 없습니다.")
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

        elif indicator_type == "OBV":
            # short_period, long_period를 필수로 둘 수도 있지만
            # 실제 obv_signal은 threshold 방식이므로, 여기서는 필수 안 두고 예시
            df_local = obv_signal(
                df_local,
                obv_col="obv_raw",
                threshold=0.0,
                signal_col=signal_col
            )

        elif indicator_type == "Filter":
            if "window" not in param:
                raise ValueError("Filter 지표에 'window'가 없습니다.")
            if "x_pct" not in param or "y_pct" not in param:
                raise ValueError("Filter 지표에 'x_pct'와 'y_pct'가 필요합니다.")
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

        elif indicator_type == "Support_Resistance":
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
                signal_col=signal_col
            )

        elif indicator_type == "Channel_Breakout":
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
                signal_col=signal_col
            )

        else:
            raise ValueError(f"지원되지 않는 지표 타입입니다: {indicator_type}")

        temp_signal_cols.append(signal_col)

    # 여러 시그널을 합산하여 최종 시그널(1/-1/0)
    df_local = combine_signals(df_local, signal_cols=temp_signal_cols, out_col="signal_final")
    final_signals = df_local["signal_final"].tolist()
    return final_signals
