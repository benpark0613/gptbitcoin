# gptbitcoin/analysis/scoring.py
# 주석은 필수 최소한 한글, docstring은 구글 스타일
# IS/OOS 백테스트 결과(또는 raw 로그/시계열)를 받아 성과지표 계산, 최종 CSV 저장

import math
import os
from typing import Dict, List, Any

import numpy as np
import pandas as pd


def calc_mdd(equity_series: List[float]) -> float:
    """
    주어진 에쿼티 시계열에서 최대낙폭(MDD, Max Drawdown)을 계산한다.
    """
    peak = -math.inf
    drawdown = 0.0
    mdd = 0.0

    for eq in equity_series:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak
        if dd > drawdown:
            drawdown = dd
    mdd = drawdown
    return mdd


def calc_sharpe(equity_series: List[float], rf: float = 0.0) -> float:
    """
    간단한 방식의 샤프비율 계산.
    equity_series가 각 시점의 '누적 자산'이라고 가정하고,
    하루(또는 봉) 단위 수익률 -> 평균/표준편차 기반 Sharpe.

    Args:
        equity_series (List[float]): 시점별 누적자산
        rf (float): 무위험수익률(일간 기준)

    Returns:
        float: 샤프비율
    """
    if len(equity_series) < 2:
        return 0.0

    # 시점별 로그수익률(또는 단순 수익률)
    rets = []
    for i in range(1, len(equity_series)):
        prev = equity_series[i - 1]
        curr = equity_series[i]
        if prev <= 0:
            continue
        ret = (curr - prev) / prev
        # ret = math.log(curr / prev)  # 로그수익률을 쓰는 방법도 있음
        rets.append(ret)

    if len(rets) < 2:
        return 0.0

    avg_ret = np.mean(rets) - rf
    std_ret = np.std(rets, ddof=1)  # 샘플 표준편차

    if std_ret < 1e-14:
        return 0.0
    sharpe = (avg_ret / std_ret) * math.sqrt(len(rets))  # 일간기준 => 연율화 시 sqrt(연간거래일수) 곱
    return sharpe


def calc_trade_stats(trade_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    체결 로그를 기반으로, 거래 횟수, 평균 홀딩 기간 등 통계를 계산하는 예시 함수.

    Args:
        trade_logs (List[Dict[str, Any]]):
            각 원소가 {"datetime": datetime, "type": "BUY"/"SELL", "price": float, "size": float} 등
    Returns:
        Dict[str, Any]:
            {
              "trades": int,               # 총 거래 횟수
              "avg_holding_period": float, # 평균 보유 기간(일 또는 봉)
              "win_rate": float,           # 승률(0~1)
              "profit_factor": float,      # profit factor
              "avg_pnl_per_trade": float,  # 평균 트레이드당 pnl
            }
    """
    if not trade_logs:
        return {
            "trades": 0,
            "avg_holding_period": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_pnl_per_trade": 0.0,
        }

    # 여기서는 실제로 trade_logs만으로 모든 통계를 계산하려면
    # 포지션 진입/청산 시점, PnL이 logging되어 있어야 한다.
    # 예: net pnl을 기록해두었거나, partial fill 로직 등 고려 필요.
    # 간단 예시는 "trades = len(trade_logs)" 식으로만 구현.
    stats = {}
    stats["trades"] = len(trade_logs)

    # 승률 / profit factor를 정확히 하려면 trade별 pnl이 있어야 함
    # 예: order에 net pnl 필드를 저장해두었다고 가정
    wins = 0
    total_win_pnl = 0.0
    total_loss_pnl = 0.0
    total_pnl = 0.0
    for t in trade_logs:
        # 예: t["pnl"] 라고 가정
        if "pnl" not in t:
            continue
        trade_pnl = t["pnl"]
        total_pnl += trade_pnl
        if trade_pnl >= 0:
            wins += 1
            total_win_pnl += trade_pnl
        else:
            total_loss_pnl += abs(trade_pnl)

    stats["win_rate"] = (wins / stats["trades"]) if stats["trades"] > 0 else 0.0
    if total_loss_pnl == 0:
        stats["profit_factor"] = float("inf") if total_win_pnl > 0 else 0.0
    else:
        stats["profit_factor"] = total_win_pnl / total_loss_pnl
    stats["avg_pnl_per_trade"] = total_pnl / stats["trades"] if stats["trades"] > 0 else 0.0

    # 평균 홀딩 기간: 별도 로직 필요(주문-청산 시점)
    stats["avg_holding_period"] = 0.0  # 실제 구현 시 보강

    return stats


def compute_backtest_metrics(
        equity_curve: List[float],
        trade_logs: List[Dict[str, Any]],
        start_cap: float
) -> Dict[str, Any]:
    """
    백테스트 결과(에쿼티 시계열, 체결 로그, 초기자본)를 받아
    최종 자본, 수익률, 샤프, MDD, 트레이드 통계 등을 계산.

    Returns:
        Dict[str, Any]:
          {
            "start_cap": ...,
            "end_cap": ...,
            "return": ...,
            "sharpe": ...,
            "mdd": ...,
            "trades": ...,
            "win_rate": ...,
            "profit_factor": ...,
            "avg_holding_period": ...,
            "avg_pnl_per_trade": ...
          }
    """
    if not equity_curve:
        return {
            "start_cap": start_cap,
            "end_cap": start_cap,
            "return": 0.0,
            "sharpe": 0.0,
            "mdd": 0.0,
            "trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_holding_period": 0.0,
            "avg_pnl_per_trade": 0.0,
        }

    end_cap = equity_curve[-1]
    total_return = (end_cap - start_cap) / start_cap if start_cap != 0 else 0.0

    mdd = calc_mdd(equity_curve)
    sharpe = calc_sharpe(equity_curve, rf=0.0)

    tstats = calc_trade_stats(trade_logs)

    return {
        "start_cap": start_cap,
        "end_cap": end_cap,
        "return": total_return,
        "sharpe": sharpe,
        "mdd": mdd,
        "trades": tstats["trades"],
        "win_rate": tstats["win_rate"],
        "profit_factor": tstats["profit_factor"],
        "avg_holding_period": tstats["avg_holding_period"],
        "avg_pnl_per_trade": tstats["avg_pnl_per_trade"],
    }


def merge_and_score_is_oos(
        is_results_path: str,
        oos_results_path: str,
        out_csv_path: str,
        buyhold_is_return: float
) -> None:
    """
    1) is_results.csv 와 oos_results.csv를 로드
    2) is_return >= buyhold_is_return인 항목만 'is_passed=True' 처리
    3) IS/OOS 정보 + 사용 지표(used_indicators) 등을 모아 하나의 CSV로 저장

    Args:
        is_results_path (str): IS 결과 CSV
        oos_results_path (str): OOS 결과 CSV
        out_csv_path (str): 최종 성과 csv
        buyhold_is_return (float): B/H의 인샘플 수익률
    """
    if not os.path.isfile(is_results_path):
        print(f"[merge_and_score_is_oos] is_results.csv 파일 없음: {is_results_path}")
        return
    if not os.path.isfile(oos_results_path):
        print(f"[merge_and_score_is_oos] oos_results.csv 파일 없음: {oos_results_path}")
        return

    df_is = pd.read_csv(is_results_path)
    df_oos = pd.read_csv(oos_results_path)

    # 예: df_is 에는 [timeframe, final_value, start_cap, return, sharpe, mdd, trades, used_indicators..] 식 칼럼이 있다고 가정
    # 실제 코드에선 run_is.py에서 CSV에 필요한 컬럼을 저장해야 함
    # merge 키: [combo_index, timeframe] 등
    merge_cols = ["combo_index", "timeframe"]

    # is_passed = (is_return >= buyhold_is_return)
    if "return" in df_is.columns:
        df_is["is_passed"] = df_is["return"] >= buyhold_is_return
    else:
        df_is["is_passed"] = False

    # 병합
    df_merged = pd.merge(df_is, df_oos, on=merge_cols, how="left", suffixes=("_is", "_oos"))

    # 최종적으로 지정된 컬럼 순서대로 정리
    # 예: timeframe, is_start_cap, is_end_cap, is_return, is_trades, is_sharpe, is_mdd, is_passed,
    #     oos_start_cap, oos_end_cap, oos_return, oos_trades, oos_sharpe, oos_mdd, used_indicators, oos_trades_log
    # 실제로 run_is / run_oos에서 "used_indicators"나 "trades_log"를 어떻게 저장했는지 일치 필요
    wanted_cols = [
        "timeframe",
        "start_cap_is", "end_cap_is", "return_is", "trades_is", "sharpe_is", "mdd_is",
        "is_passed",
        "start_cap_oos", "end_cap_oos", "return_oos", "trades_oos", "sharpe_oos", "mdd_oos",
        "used_indicators",
        "oos_trades_log"
    ]

    # 컬럼명 매핑: run_is / run_oos CSV에서 실제로 "start_cap" -> "start_cap_is" 등으로 저장했는지 맞춰봐야 함
    col_map = {
        "timeframe": "timeframe",
        "start_cap_is": "start_cap_is",
        "end_cap_is": "end_cap_is",
        "return_is": "return_is",
        "trades_is": "trades_is",
        "sharpe_is": "sharpe_is",
        "mdd_is": "mdd_is",
        "start_cap_oos": "start_cap_oos",
        "end_cap_oos": "end_cap_oos",
        "return_oos": "return_oos",
        "trades_oos": "trades_oos",
        "sharpe_oos": "sharpe_oos",
        "mdd_oos": "mdd_oos",
        "used_indicators": "used_indicators",
        "oos_trades_log": "oos_trades_log",
        # ...
    }
    # 실제 df_merged에 존재하는 칼럼을 확인 후 rename or reindex
    # 여기서는 예시로 df_merged["start_cap_is"] = df_merged["start_cap_x"] etc...
    # 코드 예시:
    for c in col_map.values():
        if c not in df_merged.columns:
            # 없는 경우 일단 채워 넣기
            df_merged[c] = None

    # 최종 리오더
    df_final = df_merged[wanted_cols]

    # CSV 저장
    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)
    df_final.to_csv(out_csv_path, index=False, encoding="utf-8")
    print(f"[merge_and_score_is_oos] 최종 성과결과 CSV 저장: {out_csv_path}")


def main():
    """
    간단 예:
    1) is_results.csv / oos_results.csv 로부터 성과를 로드
    2) 인샘플 단계 buy&hold 수익률(buyhold_is_return)을 미리 계산했다고 가정
    3) is_return >= buyhold_is_return => is_passed=True
    4) is + oos 머지 + 특정 컬럼 순서 => final_scores.csv
    """
    is_csv = "results/IS/is_results.csv"
    oos_csv = "results/OOS/oos_results.csv"
    out_csv = "results/final_scores.csv"

    # 예시로 B/H가 인샘플에서 +30%(=0.3) 수익이었다고 가정
    buyhold_is_return = 0.3

    merge_and_score_is_oos(is_csv, oos_csv, out_csv, buyhold_is_return=buyhold_is_return)


if __name__ == "__main__":
    main()

