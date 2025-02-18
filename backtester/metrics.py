# gptbitcoin/backtester/metrics.py

"""
metrics.py

백테스트 결과와 거래 정보를 활용해 성과지표를 계산하는 모듈.
 - result: run_backtest()가 반환하는 DataFrame (equity, pnl 등)
 - trades_info: 각 거래별 (진입, 청산) 정보가 담긴 리스트

주요 지표:
 1) Sharpe Ratio
 2) Max Drawdown (MDD)
 3) Calmar Ratio
 4) Win Ratio (캔들 기준)
 5) 거래 횟수, 승률(거래 단위)
 6) 평균 승리금액, 평균 손실금액, 손익비(R/R)
 7) 연승, 연패 (거래 기준)
"""

import pandas as pd
import numpy as np

def calc_sharpe(equity_series: pd.Series, risk_free_rate: float = 0.0, period_per_year: int = 365) -> float:
    """
    샤프 비율(Sharpe Ratio).
    equity_series: 일별(또는 캔들별) 자산 가치
    period_per_year: 1년 동안의 측정 횟수 (예: 일봉=252/365, 1h봉=8760 등)
    """
    equity_diff = np.log(equity_series).diff().dropna()
    if equity_diff.empty:
        return 0.0

    mean_ret = equity_diff.mean()
    std_ret  = equity_diff.std(ddof=1)  # sample std

    mean_annual = mean_ret * period_per_year
    std_annual  = std_ret * np.sqrt(period_per_year)

    if std_annual == 0:
        return 0.0

    # (연평균수익률 - 무위험이자율) / 변동성
    sharpe = (mean_annual - risk_free_rate) / std_annual
    return sharpe

def calc_max_drawdown(equity_series: pd.Series) -> float:
    """
    최대낙폭(MDD). 예: 0.25 => -25% 낙폭
    """
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    mdd = drawdown.min()  # 가장 작은 값(최대 낙폭)
    return abs(float(mdd))

def calc_calmar(equity_series: pd.Series, period_per_year: int = 365) -> float:
    """
    Calmar Ratio = (연평균 성장률) / (최대낙폭)
    """
    if len(equity_series) < 2:
        return 0.0

    start_val = equity_series.iloc[0]
    end_val   = equity_series.iloc[-1]
    total_periods = len(equity_series) - 1
    years = total_periods / period_per_year

    if start_val <= 0 or years <= 0:
        return 0.0

    cagr = (end_val / start_val)**(1.0 / years) - 1.0
    mdd  = calc_max_drawdown(equity_series)
    if mdd == 0:
        return np.inf if cagr > 0 else 0.0

    return cagr / mdd

def calc_candle_win_ratio(pnl_series: pd.Series) -> float:
    """
    (캔들 단위) 승률:
      - 일별(또는 캔들별) pnl[t] - pnl[t-1] > 0인 구간 비율
    """
    daily_pnl = pnl_series.diff().dropna()
    if daily_pnl.empty:
        return 0.0
    wins = (daily_pnl > 0).sum()
    total = len(daily_pnl)
    return wins / total if total > 0 else 0.0

# ---------------------- 거래 기반 지표 계산 ---------------------- #

def analyze_trades(trades_info: list) -> dict:
    """
    거래별 체결 정보(trades_info)를 바탕으로 거래 기반 지표를 계산.
    trades_info의 각 원소 예:
       {
         "entry_idx": ...,
         "entry_price": ...,
         "entry_time": ...,
         "exit_idx": ...,
         "exit_price": ...,
         "exit_time": ...,
         "direction": 1 or -1,
         "pnl": float(단일 거래 손익),
         "cumulative_pnl": float(누적 손익)
       }

    반환:
      - trade_count  : 거래 횟수
      - trade_win_rate : 거래 승률 (거래 단위)
      - avg_win      : 평균 승리금액
      - avg_loss     : 평균 손실금액
      - reward_risk  : 손익비(평균 승리금액 / 평균 손실금액)
      - max_consec_win : 최대 연승
      - max_consec_loss: 최대 연패
    """

    result = {
        "trade_count": 0,
        "trade_win_rate": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "reward_risk": 0.0,
        "max_consec_win": 0,
        "max_consec_loss": 0
    }
    if not trades_info:
        return result

    trade_count = len(trades_info)
    pnls = [t["pnl"] for t in trades_info]

    # 승패
    win_trades = [p for p in pnls if p > 0]
    loss_trades = [p for p in pnls if p < 0]

    result["trade_count"] = trade_count
    if trade_count > 0:
        result["trade_win_rate"] = len(win_trades) / trade_count

    if win_trades:
        result["avg_win"] = sum(win_trades)/len(win_trades)
    if loss_trades:
        result["avg_loss"] = sum(loss_trades)/len(loss_trades)

    if result["avg_loss"] != 0:
        result["reward_risk"] = abs(result["avg_win"] / result["avg_loss"])

    # 연승/연패 계산
    # 승(1), 패(0)로 변환
    wins_losses = [1 if p>0 else 0 for p in pnls]
    max_consec_win, max_consec_loss = calc_consecutive_win_loss(wins_losses)
    result["max_consec_win"] = max_consec_win
    result["max_consec_loss"] = max_consec_loss

    return result

def calc_consecutive_win_loss(wins_losses: list) -> tuple:
    """
    wins_losses: 1=승, 0=패
    연속 승, 연속 패의 최댓값을 구한다.
    """
    max_win_streak = 0
    max_loss_streak = 0

    current_win = 0
    current_loss = 0

    for w in wins_losses:
        if w == 1:
            current_win += 1
            max_win_streak = max(max_win_streak, current_win)
            # 패 streak 리셋
            current_loss = 0
        else:  # w==0
            current_loss += 1
            max_loss_streak = max(max_loss_streak, current_loss)
            # 승 streak 리셋
            current_win = 0

    return max_win_streak, max_loss_streak

# ----------------------------------------------------- #

def summarize_metrics(
    result_df: pd.DataFrame,
    trades_info: list,
    period_per_year: int = 365,
    risk_free_rate: float = 0.0
) -> dict:
    """
    최종 종합 지표:
     - Sharpe, MDD, Calmar, (캔들 기준) 승률
     - 거래 기반 지표(거래 횟수, 거래 승률, 평균 승패금액, 손익비, 연승/연패 등)

    Parameters
    ----------
    result_df : pd.DataFrame
      run_backtest() 결과. 'equity', 'pnl' 등이 있어야 함.
    trades_info : list
      backtest_engine이 저장한 거래별 체결 정보
    period_per_year : int
      연율화 시 사용하는 연간 기간 수(일봉=365 or 252, 1h봉=8760, ...)
    risk_free_rate : float
      무위험이자율(연단위)

    Returns
    -------
    dict
      종합 지표를 key-value 형태로 반환
    """
    equity = result_df["equity"].dropna()
    pnl    = result_df["pnl"].dropna()

    # (1) Equity 기반 지표
    if len(equity) < 2:
        # 데이터가 너무 적으면 0으로 리턴
        eq_metrics = {
            "final_equity": float(equity.iloc[-1]) if not equity.empty else 0.0,
            "return_pct": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "candle_win_ratio": 0.0
        }
    else:
        final_equity = equity.iloc[-1]
        init_equity  = equity.iloc[0]
        return_pct   = (final_equity / init_equity - 1.0) if init_equity > 0 else 0.0

        eq_metrics = {
            "final_equity": float(final_equity),
            "return_pct": float(return_pct),
            "sharpe": float(calc_sharpe(equity, risk_free_rate, period_per_year)),
            "max_drawdown": float(calc_max_drawdown(equity)),
            "calmar": float(calc_calmar(equity, period_per_year)),
            "candle_win_ratio": float(calc_candle_win_ratio(pnl))
        }

    # (2) 거래 기반 지표
    trade_stats = analyze_trades(trades_info)

    # 종합
    all_metrics = {**eq_metrics, **trade_stats}
    return all_metrics

def main():
    """
    간단 테스트
    """
    # 예시: 임의로 만든 백테스트 결과
    # equity, pnl
    data = {
        "equity": [100000, 102000, 101000, 105000, 104000, 98000, 99000],
        "pnl": [0, 2000, 1000, 5000, 4000, -2000, -1000]
    }
    result_df = pd.DataFrame(data)

    # 임의 trades_info
    trades_info = [
        {
          "entry_idx": 0,
          "entry_price": 100,
          "entry_time": 0,
          "exit_idx": 2,
          "exit_price": 101,
          "exit_time": 2,
          "direction": 1,
          "pnl": 1000,
          "cumulative_pnl": 1000
        },
        {
          "entry_idx": 2,
          "entry_price": 101,
          "entry_time": 2,
          "exit_idx": 5,
          "exit_price": 98,
          "exit_time": 5,
          "direction": 1,
          "pnl": -3000,
          "cumulative_pnl": -2000
        }
    ]

    summary = summarize_metrics(result_df, trades_info, period_per_year=365, risk_free_rate=0.0)

    print("=== Metrics Summary ===")
    for k, v in summary.items():
        print(f"{k} : {v}")

if __name__ == "__main__":
    main()
