# gptbitcoin/backtester/metrics.py

import pandas as pd
import numpy as np

def calc_sharpe(equity_series: pd.Series, risk_free_rate: float = 0.0, period_per_year: int = 365) -> float:
    """
    기존처럼 샤프 비율(Sharpe Ratio)을 계산하는 함수.
    equity_series: 시계열 자산 가치
    period_per_year: 연율화용 횟수 (예: 일봉=365, 1h봉=8760 등)
    """
    equity_diff = np.log(equity_series).diff().dropna()
    if equity_diff.empty:
        return 0.0

    mean_ret = equity_diff.mean()
    std_ret  = equity_diff.std(ddof=1)
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
    mdd = drawdown.min()
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

def analyze_trades(trades_info: list) -> dict:
    """
    거래별 체결 정보 기반으로 거래 단위 지표를 계산.
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
    wins_losses = [1 if p>0 else 0 for p in pnls]
    max_consec_win, max_consec_loss = calc_consecutive_win_loss(wins_losses)
    result["max_consec_win"] = max_consec_win
    result["max_consec_loss"] = max_consec_loss

    return result

def calc_consecutive_win_loss(wins_losses: list) -> tuple:
    """
    wins_losses: 1=승, 0=패
    연속 승, 연속 패의 최댓값 계산
    """
    max_win_streak = 0
    max_loss_streak = 0
    current_win = 0
    current_loss = 0

    for w in wins_losses:
        if w == 1:
            current_win += 1
            max_win_streak = max(max_win_streak, current_win)
            current_loss = 0
        else:
            current_loss += 1
            max_loss_streak = max(max_loss_streak, current_loss)
            current_win = 0

    return max_win_streak, max_loss_streak

def summarize_metrics(
    result_df: pd.DataFrame,
    trades_info: list,
    period_per_year: int = 365,
    risk_free_rate: float = 0.0
) -> dict:
    """
    기존 종합 지표 계산 (Sharpe, MDD, Calmar, (캔들)승률, 거래 기반 지표 등).
    """
    equity = result_df["equity"].dropna()
    pnl    = result_df["pnl"].dropna()

    if len(equity) < 2:
        eq_metrics = {
            "final_equity": float(equity.iloc[-1]) if not equity.empty else 0.0,
            "return_pct": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "candle_win_ratio": 0.0
        }
    else:
        final_equity = float(equity.iloc[-1])
        init_equity  = float(equity.iloc[0])
        return_pct   = (final_equity / init_equity - 1.0) if init_equity > 0 else 0.0

        eq_metrics = {
            "final_equity": final_equity,
            "return_pct": return_pct,
            "sharpe": float(calc_sharpe(equity, risk_free_rate, period_per_year)),
            "max_drawdown": float(calc_max_drawdown(equity)),
            "calmar": float(calc_calmar(equity, period_per_year)),
            "candle_win_ratio": float(calc_candle_win_ratio(pnl))
        }

    trade_stats = analyze_trades(trades_info)
    all_metrics = {**eq_metrics, **trade_stats}
    return all_metrics

# ------------------ 새로 추가: summarize_metrics_lite ------------------ #
def summarize_metrics_lite(
    result_df: pd.DataFrame,
    period_per_year: int = 365,
    risk_free_rate: float = 0.0
) -> dict:
    """
    In-Sample 단계에서 '수익률'과 'Sharpe'만 빠르게 계산하고 싶을 때 사용.
    trades_info 등 거래 기반 지표는 계산하지 않는다.
    """
    equity = result_df["equity"].dropna()

    if len(equity) < 2:
        # 데이터가 충분치 않으면 0 처리
        return {
            "return_pct": 0.0,
            "sharpe": 0.0
        }

    init_equity = float(equity.iloc[0])
    final_equity = float(equity.iloc[-1])

    if init_equity <= 0:
        return {
            "return_pct": 0.0,
            "sharpe": 0.0
        }

    return_pct = (final_equity / init_equity - 1.0)

    # Sharpe
    equity_diff = np.log(equity).diff().dropna()
    mean_ret = equity_diff.mean()
    std_ret  = equity_diff.std(ddof=1)
    if std_ret == 0.0:
        sharpe = 0.0
    else:
        mean_annual = mean_ret * period_per_year
        std_annual  = std_ret * np.sqrt(period_per_year)
        sharpe = (mean_annual - risk_free_rate) / std_annual

    return {
        "return_pct": float(return_pct),
        "sharpe": float(sharpe)
    }
