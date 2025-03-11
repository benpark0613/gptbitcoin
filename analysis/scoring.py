# gptbitcoin/analysis/scoring.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 최종 CSV에 필요한 컬럼(시작/최종 자본, 수익률, 트레이드 수, 샤프, MDD, 마지막 10% 구간 기울기)에
# "Score"를 추가하기 위한 수정 버전.

import math
from typing import List, Dict


def _stdev(data: List[float]) -> float:
    """
    표준편차 계산.

    Args:
        data (List[float]): 분산 계산 대상 리스트

    Returns:
        float: data의 표준편차
    """
    if len(data) < 2:
        return 0.0
    mean_val = sum(data) / len(data)
    var = sum((x - mean_val) ** 2 for x in data) / (len(data) - 1)
    return math.sqrt(var)


def _calculate_mdd(equity_curve: List[float]) -> float:
    """
    최대 낙폭(MDD) 계산.

    Args:
        equity_curve (List[float]): 시점별 누적자산 곡선

    Returns:
        float: MDD 값 (0 ~ 1 범위)
    """
    if not equity_curve:
        return 0.0

    peak = equity_curve[0]
    max_drawdown = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (peak - val) / peak
        if dd > max_drawdown:
            max_drawdown = dd
    return max_drawdown


def _infer_bars_per_year(timeframe: str) -> int:
    """
    타임프레임에 따라 1년간 봉 수를 추정 (샤프 연환산 용).

    Args:
        timeframe (str): "1d", "4h", "1h", "15m" 등

    Returns:
        int: 1년간 예상 봉 수
    """
    tf = timeframe.lower()
    if tf.endswith("d"):
        # 예: "1d" -> 365
        day_count = int(tf.replace("d", "")) if tf.replace("d", "").isdigit() else 1
        return 365 // day_count
    elif tf.endswith("h"):
        hour_count = int(tf.replace("h", "")) if tf.replace("h", "").isdigit() else 1
        bars_per_day = 24 // hour_count if hour_count else 24
        return bars_per_day * 365
    elif tf.endswith("m"):
        minute_count = int(tf.replace("m", "")) if tf.replace("m", "").isdigit() else 1
        bars_per_day = 1440 // minute_count if minute_count else 1440
        return bars_per_day * 365
    return 365  # fallback


def _calculate_slope_last_segment(equity_curve: List[float], portion: float = 0.1) -> float:
    """
    자산 곡선(equity_curve)에서 마지막 portion(기본 10%) 구간의
    회귀선(추세선) 기울기를 계산. (간단한 수식으로 최소제곱법)

    Args:
        equity_curve (List[float]): 시점별 누적자산
        portion (float, optional): 마지막 몇 %를 볼지(기본 0.1 → 10%)

    Returns:
        float: 추세 기울기. 양(+)이면 우상향, 음(-)이면 우하향
    """
    n_total = len(equity_curve)
    if n_total < 2:
        return 0.0

    start_idx = int(n_total * (1.0 - portion))
    if start_idx < 0:
        start_idx = 0

    subset = equity_curve[start_idx:]
    n_sub = len(subset)
    if n_sub < 2:
        return 0.0

    # x: 0 ~ n_sub-1
    x_vals = range(n_sub)
    sum_x = sum(x_vals)
    sum_y = sum(subset)
    mean_x = sum_x / n_sub
    mean_y = sum_y / n_sub

    # 공분산 / 분산
    numerator = 0.0
    denominator = 0.0
    for i, y_val in zip(x_vals, subset):
        numerator += (i - mean_x) * (y_val - mean_y)
        denominator += (i - mean_x) ** 2

    if abs(denominator) < 1e-12:
        return 0.0
    slope = numerator / denominator
    return slope


def calculate_metrics(
    equity_curve: List[float],
    daily_returns: List[float],
    start_capital: float,
    trades: List[Dict],
    timeframe: str = "1d",
    risk_free_rate_annual: float = 0.0
) -> Dict[str, float]:
    """
    백테스트 결과로부터 최종 CSV에 필요한 지표들을 계산한다.
    (StartCapital, EndCapital, Return, Trades, Sharpe, MDD, Last10Slope, Score)

    Score는 간단한 가중 합산 예시:
        Score = wR * Return + wS * Sharpe + wM * (-MDD) + wL * Last10Slope
    (필요시 정규화/표준화 절차를 추가할 수도 있음)

    Args:
        equity_curve (List[float]): 시점별 누적자산
        daily_returns (List[float]): 각 시점별 봉단위 수익률
        start_capital (float): 초기자본
        trades (List[Dict]): 체결된 매매내역 (pnl, position_type 등)
        timeframe (str, optional): 봉 주기 ("1d", "4h" 등). Sharpe 연환산에 사용
        risk_free_rate_annual (float, optional): 연간 무위험이자율 (샤프 계산용)

    Returns:
        Dict[str, float]: {
            "StartCapital": ...,
            "EndCapital": ...,
            "Return": ...,
            "Trades": ...,
            "Sharpe": ...,
            "MDD": ...,
            "Last10Slope": ...,
            "Score": ...
        }
    """
    if not equity_curve or len(equity_curve) != len(daily_returns):
        raise ValueError("equity_curve와 daily_returns 길이가 일치해야 합니다.")
    if len(equity_curve) < 2:
        raise ValueError("백테스트 데이터가 최소 2개 이상 필요합니다.")

    # 최종 자본, 총 수익률
    end_capital = equity_curve[-1]
    total_return = (end_capital / start_capital) - 1.0

    # 전체 트레이드 수
    num_trades = len(trades)

    # Sharpe Ratio
    bars_per_year = _infer_bars_per_year(timeframe)
    avg_ret = sum(daily_returns) / len(daily_returns)
    std_ret = _stdev(daily_returns)
    rfr_per_bar = risk_free_rate_annual / bars_per_year
    if std_ret > 1e-12:
        sharpe = (avg_ret - rfr_per_bar) * math.sqrt(bars_per_year) / std_ret
    else:
        sharpe = 0.0

    # MDD
    mdd = _calculate_mdd(equity_curve)

    # 마지막 10% 구간 기울기
    last_10_slope = _calculate_slope_last_segment(equity_curve, 0.1)

    # 간단한 Score 계산 (예: 임의 가중치, 필요 시 수정)
    wR, wS, wM, wL = 0.3, 0.25, 0.15, 0.3
    score = (wR * total_return) + (wS * sharpe) + (wM * (-mdd)) + (wL * last_10_slope)

    return {
        "StartCapital": start_capital,
        "EndCapital": end_capital,
        "Return": total_return,
        "Trades": num_trades,
        "Sharpe": sharpe,
        "MDD": mdd,
        "Last10Slope": last_10_slope,
        "Score": score
    }
