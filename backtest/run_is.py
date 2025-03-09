# gptbitcoin/backtest/run_is.py
# 구글 스타일, 최소한의 한글 주석
# In-Sample(IS) 구간에서 여러 지표 조합(콤보)을 테스트 후,
# 시장수익률 대비 비교하고 is_passed 여부를 결정하여 리스트로 반환.

import json
from typing import List, Dict, Any

from joblib import Parallel, delayed

from utils.date_time import ms_to_kst_str
from config.config import (
    ALLOW_SHORT,
    START_CAPITAL
)
from backtest.engine import run_backtest
from analysis.scoring import calculate_metrics

# 새로 추가된 signal_factory 모듈에서 create_signals_for_combo 함수를 import
from strategies.signal_factory import create_signals_for_combo


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
      3) 시장수익률과 Sharpe, MDD 등을 비교 후 is_passed 여부 결정.
      4) 각 조합 결과를 리스트(dict)로 반환.

    Args:
        df_is (pd.DataFrame): IS 구간 시계열 데이터
        combos (List[List[Dict[str,Any]]]): 지표 파라미터 조합들
        timeframe (str): 예) "1d", "4h" 등
        start_capital (float, optional): 초기 자본

    Returns:
        List[Dict[str,Any]]:
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
              },
              ...
            ]
    """
    if df_is.empty:
        return []

    # IS 구간의 시작/끝 시점(UTC ms)을 KST 문자열로 변환
    is_start_ms = df_is.iloc[0]["open_time"]
    is_end_ms = df_is.iloc[-1]["open_time"]
    is_start_kst = ms_to_kst_str(is_start_ms)
    is_end_kst = ms_to_kst_str(is_end_ms)

    print(f"[INFO] IS({timeframe}) range: {is_start_kst} ~ {is_end_kst}, rows={len(df_is)}")

    # 1) Buy & Hold 결과 산출
    bh_signals = [1] * len(df_is)  # 모든 시점 매수
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

    # Buy & Hold 결과를 첫 행으로 추가
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
        주어진 콤보(여러 지표 dict)로 IS 구간 백테스트,
        결과와 is_passed 여부를 dict로 반환.
        """
        # 콤보 시그널 생성
        df_local = create_signals_for_combo(df_is, combo, out_col="signal_final")
        signals = df_local["signal_final"].tolist()

        # 백테스트 실행
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

        # 시장수익률과 비교 (예: Return이 B/H 이상이면 True)
        pass_bool = (score["Return"] >= bh_return)
        # 원한다면 Sharpe, MDD 조건도 추가 가능
        # pass_bool = pass_bool and (score["Sharpe"] >= bh_sharpe)
        # pass_bool = pass_bool and (score["MDD"] <= bh_mdd)

        # used_indicators 칼럼에 combo_params 정보를 JSON 형태로 저장
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
