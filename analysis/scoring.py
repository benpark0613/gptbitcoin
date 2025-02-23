# gptbitcoin/analysis/scoring.py
# 구글 스타일, 최소한의 한글 주석

from typing import List, Dict
import math

def calculate_metrics(
    equity_curve: List[float],
    daily_returns: List[float],
    start_capital: float,
    trades: List[Dict],
    days_in_test: int,
    risk_free_rate_annual: float = 0.0,
    bars_per_year: int = 365
) -> Dict[str, float]:
    """
    백테스트 결과의 성과 지표를 계산한다.

    Args:
        equity_curve (List[float]): 시점별 누적자산曲線
        daily_returns (List[float]): 시점별 각 봉 수익률
        start_capital (float): 초기자금
        trades (List[Dict]): 체결된 거래들의 목록
        days_in_test (int): 테스트 전체 기간 (일 단위)
        risk_free_rate_annual (float): 연간 무위험 이자율
        bars_per_year (int): Sharpe 연 환산용 (일봉이면 365, 4H면 더 크게 등)

    Returns:
        Dict[str, float]: 성과 지표 딕셔너리 (롱/숏 거래 및 수익률 포함)
    """
    # 유효성 검사
    if not equity_curve or len(equity_curve) != len(daily_returns):
        raise ValueError("equity_curve와 daily_returns 길이가 일치해야 함")
    if len(equity_curve) < 2:
        raise ValueError("백테스트 데이터가 충분하지 않음")

    # 최종 자본 / 총 수익률
    end_capital = equity_curve[-1]
    gross_return = (end_capital / start_capital) - 1.0

    # CAGR
    years = days_in_test / 365.0 if days_in_test > 0 else 1.0
    if years > 0:
        cagr = (end_capital / start_capital) ** (1.0 / years) - 1.0
    else:
        cagr = 0.0

    # Sharpe Ratio
    avg_ret = sum(daily_returns) / len(daily_returns)
    std_ret = _stdev(daily_returns)
    rfr_per_bar = risk_free_rate_annual / bars_per_year
    if std_ret > 0:
        sharpe = (avg_ret - rfr_per_bar) * math.sqrt(bars_per_year) / std_ret
    else:
        sharpe = 0.0

    # MDD
    mdd = _calculate_mdd(equity_curve)

    # 거래 지표
    num_trades = len(trades)
    if num_trades > 0:
        wins = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] < 0]
        win_rate = len(wins) / num_trades
        total_win_pnl = sum(t["pnl"] for t in wins) if wins else 0.0
        total_loss_pnl = sum(t["pnl"] for t in losses) if losses else 0.0
        profit_factor = abs(total_win_pnl / total_loss_pnl) if total_loss_pnl != 0 else float("inf")

        avg_hold = sum(t["holding_days"] for t in trades) / num_trades
        avg_pnl = sum(t["pnl"] for t in trades) / num_trades
    else:
        win_rate = 0.0
        profit_factor = 0.0
        avg_hold = 0.0
        avg_pnl = 0.0

    # 롱/숏 거래 지표
    long_trades = [t for t in trades if t.get("position_type") == "long"]
    short_trades = [t for t in trades if t.get("position_type") == "short"]

    num_long_trades = len(long_trades)
    num_short_trades = len(short_trades)

    total_long_pnl = sum(t["pnl"] for t in long_trades) if long_trades else 0.0
    total_short_pnl = sum(t["pnl"] for t in short_trades) if short_trades else 0.0

    long_return = ((start_capital + total_long_pnl) / start_capital) - 1.0 if start_capital > 0 else 0.0
    short_return = ((start_capital + total_short_pnl) / start_capital) - 1.0 if start_capital > 0 else 0.0

    return {
        "StartCapital": start_capital,
        "EndCapital": end_capital,
        "Return": gross_return,
        "CAGR": cagr,
        "Sharpe": sharpe,
        "MDD": mdd,
        "Trades": num_trades,
        "WinRate": win_rate,
        "ProfitFactor": profit_factor,
        "AvgHoldingPeriod": avg_hold,
        "AvgPnlPerTrade": avg_pnl,
        "LongTrades": num_long_trades,
        "ShortTrades": num_short_trades,
        "LongReturn": long_return,
        "ShortReturn": short_return,
    }

def _stdev(data: List[float]) -> float:
    """표준편차 계산."""
    if len(data) < 2:
        return 0.0
    mean_val = sum(data) / len(data)
    var = sum((x - mean_val) ** 2 for x in data) / (len(data) - 1)
    return math.sqrt(var)

def _calculate_mdd(equity_curve: List[float]) -> float:
    """
    최대낙폭(MDD)을 계산한다.
    MDD = (최고점 대비 최대 하락폭) / 최고점
    """
    peak = equity_curve[0]
    max_drawdown = 0.0

    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (peak - val) / peak
        if dd > max_drawdown:
            max_drawdown = dd
    return max_drawdown
