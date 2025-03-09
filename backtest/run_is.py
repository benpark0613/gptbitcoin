# gptbitcoin/backtest/run_is.py
# 구글 스타일 docstring, 최소한의 한글 주석을 사용한 In-Sample (IS) 백테스트 모듈.
# - IS 구간에서 여러 지표 조합(콤보)을 테스트하여, Buy & Hold와 비교
# - combos 내 각 파라미터가 buy_time_delay, sell_time_delay, holding_period를 포함할 수 있으면
#   run_backtest 호출 시 자동으로 넘겨준다.
# - 각 콤보별 성과 지표 + is_passed 여부를 반환한다.

import json
from typing import List, Dict, Any

import pandas as pd
from joblib import Parallel, delayed

from utils.date_time import ms_to_kst_str
from config.config import ALLOW_SHORT, START_CAPITAL
from backtest.engine import run_backtest
from analysis.scoring import calculate_metrics
from strategies.signal_factory import create_signals_for_combo


def run_is(
    df_is: pd.DataFrame,
    combos: List[List[Dict[str, Any]]],
    timeframe: str,
    start_capital: float = START_CAPITAL
) -> List[Dict[str, Any]]:
    """
    In-Sample (IS) 백테스트를 수행한다.

    1) Buy & Hold 전략(항상 매수)의 수익률을 산출한다.
    2) combos에 있는 각 지표 파라미터 조합을 병렬로 백테스트한다.
    3) 각 콤보의 Return을 Buy & Hold Return과 비교해 is_passed 여부를 결정한다.
    4) 각 콤보의 결과(성과 지표)를 dict 형태로 리스트에 담아 반환한다.

    Args:
        df_is (pd.DataFrame): IS 구간 시계열 데이터 (OHLCV + 지표)
        combos (List[List[Dict[str, Any]]]): 지표 파라미터 조합들
        timeframe (str): 예) "1d", "4h", "1h" 등
        start_capital (float, optional): 초기자본

    Returns:
        List[Dict[str, Any]]: 각 콤보의 백테스트 결과 리스트.
            각 원소는 다음 필드를 포함한다:
            - "timeframe"
            - "is_start_cap"
            - "is_end_cap"
            - "is_return"
            - "is_trades"
            - "is_sharpe"
            - "is_mdd"
            - "used_indicators"
            - "is_passed"
    """
    if df_is.empty:
        return []

    # IS 구간 시작/끝 시점을 KST 문자열로 변환(로그용)
    is_start_ms = df_is.iloc[0]["open_time"]
    is_end_ms = df_is.iloc[-1]["open_time"]
    is_start_kst = ms_to_kst_str(is_start_ms)
    is_end_kst = ms_to_kst_str(is_end_ms)
    print(f"[INFO] IS({timeframe}) range: {is_start_kst} ~ {is_end_kst}, rows={len(df_is)}")

    # 1) Buy & Hold (IS)
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

    # 첫 행: Buy & Hold
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

    # 2) combos 병렬 백테스트
    def _process_combo(combo: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        콤보(여러 지표 dict)로 IS 백테스트 후 결과를 반환한다.
        """
        # 콤보별 매매 시그널 생성
        df_local = create_signals_for_combo(df_is, combo, out_col="signal_final")
        signals = df_local["signal_final"].tolist()

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

        # B/H 대비 수익률 비교
        pass_bool = (score["Return"] >= bh_return)

        # 콤보 정보를 JSON 문자열로 변환
        combo_info = {
            "timeframe": timeframe,
            "combo_params": combo
        }
        combo_json = json.dumps(combo_info, ensure_ascii=False)

        return {
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

    results = [bh_row]
    parallel_out = Parallel(n_jobs=-1, verbose=5)(
        delayed(_process_combo)(combo) for combo in combos
    )
    results.extend(parallel_out)
    return results
