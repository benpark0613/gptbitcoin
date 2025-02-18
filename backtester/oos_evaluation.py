# gptbitcoin/backtester/oos_evaluation.py

"""
oos_evaluation.py

In-Sample(IS)과 Out-of-Sample(OOS) 구간을 나눠 여러 파라미터 조합을 백테스트하고,
IS에서 성과가 낮은 조합을 제외한 뒤, 남은 조합을 OOS에서 재검증하는 모듈.

사용 예시 (main.py 등에서):
    from backtester.oos_evaluation import run_is_oos_evaluation

    results = run_is_oos_evaluation(
        df=...,
        param_combinations=[...],
        generate_signals_func=...,
        ...
    )
    # IS, OOS 결과와 Buy & Hold 성과를 함께 받아서 활용

주요 함수:
  - train_test_split
  - compute_buy_and_hold_metrics
  - run_is_oos_evaluation
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Any, Tuple

from backtester.backtest_engine import run_backtest
from backtester.metrics import summarize_metrics


def train_test_split(df: pd.DataFrame, train_ratio: float = 0.7) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    시계열 df를 train_ratio 비율로 In-Sample(IS) / Out-of-Sample(OOS)로 분리한다.
    상위 train_ratio 비중을 IS로, 나머지 OOS로 나눈다.

    Returns
    -------
    (df_is, df_oos)
        df_is: In-Sample 구간
        df_oos: Out-of-Sample 구간
    """
    n = len(df)
    split_idx = int(n * train_ratio)
    df_is = df.iloc[:split_idx].copy()
    df_oos = df.iloc[split_idx:].copy()
    return df_is, df_oos


def compute_buy_and_hold_metrics(
    df: pd.DataFrame,
    initial_capital: float,
    timeframe_hours: float,
    scale_slippage: bool
) -> dict:
    """
    특정 구간(df)에 대해 Buy & Hold를 백테스트해, 성과지표를 계산한다.
    (전 구간 +1 포지션 유지)

    Parameters
    ----------
    df : pd.DataFrame
        백테스트 구간 (IS 또는 OOS) 데이터. 최소 'close' 열 포함.
    initial_capital : float
        초기 자본금
    timeframe_hours : float
        캔들 하나가 몇 시간인지 (마진이자 계산 등 사용)
    scale_slippage : bool
        짧은 시간봉이면 슬리피지를 가중하는 여부

    Returns
    -------
    dict
        summarize_metrics로 계산된 Buy & Hold 성과지표
    """
    position_series = pd.Series([1]*len(df), index=df.index)
    result, trades_info = run_backtest(
        df,
        position_series,
        initial_capital=initial_capital,
        timeframe_hours=timeframe_hours,
        scale_slippage=scale_slippage
    )
    metrics_bh = summarize_metrics(result, trades_info, period_per_year=365, risk_free_rate=0.0)
    return metrics_bh


def run_is_oos_evaluation(
    df: pd.DataFrame,
    param_combinations: List[dict],
    generate_signals_func: Callable[[pd.DataFrame, dict], pd.Series],
    initial_capital: float = 100000,
    train_ratio: float = 0.7,
    timeframe_hours: float = 24.0,
    scale_slippage: bool = True,
    compare_to_buyandhold: bool = True,
    cagr_threshold: float = 0.0,
    sharpe_threshold: float = 0.0
) -> Dict[str, Any]:
    """
    IS/OOS 평가 파이프라인:
      1) df를 train_test_split으로 IS/OOS로 분리
      2) IS 구간에서 모든 param_combinations 백테스트 → 성과지표
      3) Buy & Hold IS 성과(비교용)와 cagr_threshold, sharpe_threshold 등을 기준으로
         '성과 저조' 전략을 필터링
      4) 남은 전략들만 OOS 구간에서 백테스트, 성과지표 측정
      5) OOS 구간의 Buy & Hold 성과도 계산

    Returns
    -------
    {
      "df_is": pd.DataFrame,
      "df_oos": pd.DataFrame,
      "metrics_bh_is": dict,
      "metrics_bh_oos": dict,
      "is_results": {
         combo_key: {
           "params": dict,
           "metrics": dict,
           "result_df": pd.DataFrame,
           "trades_info": ...
         },
         ...
      },
      "oos_results": {
         combo_key: {...}   # same structure as is_results
      },
      "excluded_in_is": list[str]  # combo_key list
    }
    """
    # (1) Split df into IS/OOS
    df_is, df_oos = train_test_split(df, train_ratio=train_ratio)

    # (2) IS 구간 Buy & Hold 성과
    metrics_bh_is = {}
    if compare_to_buyandhold:
        metrics_bh_is = compute_buy_and_hold_metrics(
            df_is,
            initial_capital=initial_capital,
            timeframe_hours=timeframe_hours,
            scale_slippage=scale_slippage
        )

    # (3) IS 백테스트 for all param_combinations
    is_results = {}
    for combo in param_combinations:
        combo_key = str(combo)  # 간단히 combo dict를 문자열화

        # 신호 생성
        position_series_is = generate_signals_func(df_is, combo)

        # 백테스트
        result_is, trades_is = run_backtest(
            df_is,
            position_series_is,
            initial_capital=initial_capital,
            timeframe_hours=timeframe_hours,
            scale_slippage=scale_slippage
        )
        # 성과 지표
        metrics_is = summarize_metrics(result_is, trades_is, period_per_year=365, risk_free_rate=0.0)

        is_results[combo_key] = {
            "params": combo,
            "metrics": metrics_is,
            "result_df": result_is,
            "trades_info": trades_is
        }

    # (4) IS 필터링
    excluded_in_is = []

    bh_return_pct = metrics_bh_is.get("return_pct", 0.0)
    bh_sharpe = metrics_bh_is.get("sharpe", 0.0)

    def passes_filter(m):
        """
        IS 성과 m가
        - B&H 대비 괜찮은지
        - cagr_threshold, sharpe_threshold를 만족하는지
        를 확인
        """
        # A) Buy & Hold 비교
        pass_bh = True
        if compare_to_buyandhold and metrics_bh_is:
            if m.get("return_pct", 0.0) < bh_return_pct:
                pass_bh = False
            if m.get("sharpe", 0.0) < bh_sharpe:
                pass_bh = False

        # B) 임계값
        pass_thresh = True
        if m.get("return_pct", 0.0) < cagr_threshold:
            pass_thresh = False
        if m.get("sharpe", 0.0) < sharpe_threshold:
            pass_thresh = False

        return pass_bh and pass_thresh

    combos_passed = []
    for combo_key, val in is_results.items():
        m_is = val["metrics"]
        if passes_filter(m_is):
            combos_passed.append(combo_key)
        else:
            excluded_in_is.append(combo_key)

    # (5) OOS 구간 Buy & Hold
    metrics_bh_oos = {}
    if compare_to_buyandhold:
        metrics_bh_oos = compute_buy_and_hold_metrics(
            df_oos,
            initial_capital=initial_capital,
            timeframe_hours=timeframe_hours,
            scale_slippage=scale_slippage
        )

    # (6) OOS 백테스트 (통과된 콤보만)
    oos_results = {}
    for combo_key in combos_passed:
        combo_params = is_results[combo_key]["params"]  # IS 결과에 저장된 params

        # 신호 생성
        position_series_oos = generate_signals_func(df_oos, combo_params)

        # 백테스트
        result_oos, trades_oos = run_backtest(
            df_oos,
            position_series_oos,
            initial_capital=initial_capital,
            timeframe_hours=timeframe_hours,
            scale_slippage=scale_slippage
        )

        # 성과 지표
        metrics_oos = summarize_metrics(result_oos, trades_oos, period_per_year=365, risk_free_rate=0.0)

        oos_results[combo_key] = {
            "params": combo_params,
            "metrics": metrics_oos,
            "result_df": result_oos,
            "trades_info": trades_oos
        }

    # (7) 종합 반환
    return {
        "df_is": df_is,
        "df_oos": df_oos,
        "metrics_bh_is": metrics_bh_is,
        "metrics_bh_oos": metrics_bh_oos,
        "is_results": is_results,
        "oos_results": oos_results,
        "excluded_in_is": excluded_in_is
    }
