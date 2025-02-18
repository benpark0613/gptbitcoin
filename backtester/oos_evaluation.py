# gptbitcoin/backtester/oos_evaluation.py

"""
oos_evaluation.py

IS/OOS를 완전히 분리:
 - IS 끝나면 포지션 무조건 청산(즉 OOS는 현금 상태)
 - 필터링 후, 통과된 콤보만 config.py의 INIT_CAPITAL로 OOS 백테스트
"""

import os
import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Any, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from backtester.backtest_engine import run_backtest
from backtester.metrics import summarize_metrics, summarize_metrics_lite
from settings import config


def train_test_split(df: pd.DataFrame, train_ratio: float = 0.7) -> Tuple[pd.DataFrame, pd.DataFrame]:
    n = len(df)
    split_idx = int(n * train_ratio)
    df_is = df.iloc[:split_idx].copy()
    df_oos = df.iloc[split_idx:].copy()
    return df_is, df_oos


def compute_buy_and_hold_metrics(
    df: pd.DataFrame,
    initial_capital: float,
    timeframe_hours: float,
    scale_slippage: bool,
    period_per_year: int
) -> dict:
    """
    B&H 백테스트 → summarize_metrics(상세).
    IS/OOS 구분 없이, 인자로 받은 초기자금으로 시작.
    """
    position_series = pd.Series([1]*len(df), index=df.index)
    result, trades_info = run_backtest(
        df,
        position_series,
        initial_capital=initial_capital,
        timeframe_hours=timeframe_hours,
        scale_slippage=scale_slippage
    )
    metrics_bh = summarize_metrics(
        result, trades_info,
        period_per_year=period_per_year,
        risk_free_rate=0.0
    )

    # 시작·끝 자금
    eq = result["equity"].dropna()
    if not eq.empty:
        metrics_bh["start_cap"] = float(eq.iloc[0])
        metrics_bh["end_cap"]   = float(eq.iloc[-1])
    else:
        metrics_bh["start_cap"] = None
        metrics_bh["end_cap"]   = None

    return metrics_bh


def _evaluate_single_param_is(
    df_is: pd.DataFrame,
    combo: dict,
    generate_signals_func: Callable[[pd.DataFrame, dict], pd.Series],
    initial_capital: float,
    timeframe_hours: float,
    scale_slippage: bool,
    period_per_year: int
):
    """
    IS 백테스트:
     - initial_capital로 시작
     - summarize_metrics_lite(간단) + start_cap/end_cap 기록
    """
    combo_key = str(combo)
    position_series_is = generate_signals_func(df_is, combo)

    result_is, trades_is = run_backtest(
        df_is,
        position_series_is,
        initial_capital=initial_capital,      # IS 자본
        timeframe_hours=timeframe_hours,
        scale_slippage=scale_slippage
    )

    metrics_is = summarize_metrics_lite(
        result_df=result_is,
        period_per_year=period_per_year,
        risk_free_rate=0.0
    )

    eq = result_is["equity"].dropna()
    if not eq.empty:
        metrics_is["start_cap"] = float(eq.iloc[0])
        metrics_is["end_cap"]   = float(eq.iloc[-1])
    else:
        metrics_is["start_cap"] = None
        metrics_is["end_cap"]   = None

    return combo_key, {
        "params": combo,
        "metrics": metrics_is,
        "result_df": result_is,
        "trades_info": trades_is
    }


def _evaluate_single_param_oos(
    df_oos: pd.DataFrame,
    combo_params: dict,
    generate_signals_func: Callable[[pd.DataFrame, dict], pd.Series],
    timeframe_hours: float,
    scale_slippage: bool,
    period_per_year: int
):
    """
    OOS 백테스트:
     - config.INIT_CAPITAL로 새로 시작 (IS 포지션과 무관)
     - summarize_metrics(상세) + start_cap/end_cap
    """
    combo_key = str(combo_params)

    position_series_oos = generate_signals_func(df_oos, combo_params)
    result_oos, trades_oos = run_backtest(
        df_oos,
        position_series_oos,
        initial_capital=config.INIT_CAPITAL,  # 강제: OOS 새 자본
        timeframe_hours=timeframe_hours,
        scale_slippage=scale_slippage
    )

    metrics_oos = summarize_metrics(
        result_oos, trades_oos,
        period_per_year=period_per_year,
        risk_free_rate=0.0
    )

    eq = result_oos["equity"].dropna()
    if not eq.empty:
        metrics_oos["start_cap"] = float(eq.iloc[0])
        metrics_oos["end_cap"]   = float(eq.iloc[-1])
    else:
        metrics_oos["start_cap"] = None
        metrics_oos["end_cap"]   = None

    return combo_key, {
        "params": combo_params,
        "metrics": metrics_oos,
        "result_df": result_oos,
        "trades_info": trades_oos
    }


def run_is_oos_evaluation(
    df: pd.DataFrame,
    param_combinations: List[dict],
    generate_signals_func: Callable[[pd.DataFrame, dict], pd.Series],
    initial_capital: float = 100000,   # IS 자본
    train_ratio: float = 0.7,
    timeframe_hours: float = 24.0,
    scale_slippage: bool = True,
    compare_to_buyandhold: bool = True,
    cagr_threshold: float = 0.0,
    sharpe_threshold: float = 0.0
) -> Dict[str, Any]:
    """
    IS/OOS 평가 파이프라인.
      - IS 끝나면 포지션 청산 (즉 OOS 완전 별도)
      - IS: initial_capital
      - OOS: config.INIT_CAPITAL
    """
    df_is, df_oos = train_test_split(df, train_ratio=train_ratio)
    period_per_year = int((24.0 / timeframe_hours) * 365)

    # IS B&H
    metrics_bh_is = {}
    if compare_to_buyandhold:
        # IS도 initial_capital
        metrics_bh_is = compute_buy_and_hold_metrics(
            df_is,
            initial_capital=initial_capital,
            timeframe_hours=timeframe_hours,
            scale_slippage=scale_slippage,
            period_per_year=period_per_year
        )

    from concurrent.futures import ProcessPoolExecutor, as_completed
    max_workers = os.cpu_count() or 1

    # (A) IS 콤보 병렬
    is_results = {}
    futures_is = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for combo in param_combinations:
            fut_is = executor.submit(
                _evaluate_single_param_is,
                df_is, combo,
                generate_signals_func,
                initial_capital,
                timeframe_hours,
                scale_slippage,
                period_per_year
            )
            futures_is.append(fut_is)

        for fut in tqdm(as_completed(futures_is), total=len(futures_is), desc="IS combos"):
            combo_key, val = fut.result()
            is_results[combo_key] = val

    # (B) IS 필터
    excluded_in_is = []
    bh_return_pct = metrics_bh_is.get("return_pct", 0.0) if compare_to_buyandhold else 0.0
    bh_sharpe     = metrics_bh_is.get("sharpe", 0.0)     if compare_to_buyandhold else 0.0

    def passes_filter(m_is):
        pass_bh = True
        if compare_to_buyandhold and metrics_bh_is:
            if m_is.get("return_pct", 0.0) < bh_return_pct:
                pass_bh = False
            if m_is.get("sharpe", 0.0) < bh_sharpe:
                pass_bh = False

        pass_thresh = True
        if m_is.get("return_pct", 0.0) < cagr_threshold:
            pass_thresh = False
        if m_is.get("sharpe", 0.0) < sharpe_threshold:
            pass_thresh = False

        return pass_bh and pass_thresh

    combos_passed = []
    for ckey, val in is_results.items():
        m_is = val["metrics"]  # {return_pct,sharpe,start_cap,end_cap}
        if passes_filter(m_is):
            combos_passed.append(ckey)
        else:
            excluded_in_is.append(ckey)

    # (C) OOS B&H
    metrics_bh_oos = {}
    if compare_to_buyandhold:
        # OOS는 config.INIT_CAPITAL로 새로 시작
        metrics_bh_oos = compute_buy_and_hold_metrics(
            df_oos,
            initial_capital=config.INIT_CAPITAL,
            timeframe_hours=timeframe_hours,
            scale_slippage=scale_slippage,
            period_per_year=period_per_year
        )

    # (D) 통과 콤보 OOS 병렬
    oos_results = {}
    if combos_passed:
        combos_passed_params = [is_results[k]["params"] for k in combos_passed]

        futures_oos = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for combo_params in combos_passed_params:
                fut_oos = executor.submit(
                    _evaluate_single_param_oos,
                    df_oos, combo_params,
                    generate_signals_func,
                    timeframe_hours,
                    scale_slippage,
                    period_per_year
                )
                futures_oos.append(fut_oos)

            for fut_oos in tqdm(as_completed(futures_oos), total=len(futures_oos), desc="OOS combos"):
                combo_key, val_oos = fut_oos.result()
                oos_results[combo_key] = val_oos

    # (E) 종합 반환
    return {
        "df_is": df_is,
        "df_oos": df_oos,
        "metrics_bh_is": metrics_bh_is,   # IS B&H
        "metrics_bh_oos": metrics_bh_oos, # OOS B&H
        "is_results": is_results,         # combo is
        "oos_results": oos_results,       # combo oos
        "excluded_in_is": excluded_in_is
    }


if __name__ == "__main__":
    print("Use run_is_oos_evaluation(...) from main.py with if __name__=='__main__': guard.")
