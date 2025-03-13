# gptbitcoin/backtest/engine.py
# numpy 배열을 사용해 백테스트를 수행하는 엔진 모듈 (time_delay, holding_period 제거)

import numpy as np
import pandas as pd
from typing import Dict, Any, List


def run_backtest(
    df: pd.DataFrame,
    signals: List[int],
    start_capital: float = 100_000.0,
    allow_short: bool = True,
    leverage: float = 1.0,
    margin_type: str = "ISOLATED",
    commission_rate: float = 0.0004,
    slippage_rate: float = 0.0002
) -> Dict[str, Any]:
    """
    백테스트 엔진 (numpy 기반).
    df["close"]와 신호(signals)를 이용해 매수/매도 로직을 처리한다.
    time_delay, holding_period 로직은 제거됨.

    Args:
        df (pd.DataFrame): "close" 칼럼이 포함된 시계열 데이터, signals와 길이가 동일해야 함
        signals (List[int]): 각 시점의 매매 신호 (-1: 매도, 0: 관망, +1: 매수)
        start_capital (float): 초기 자본
        allow_short (bool): 숏 포지션 허용 여부
        leverage (float): 레버리지 배수
        margin_type (str): 마진 유형 ("ISOLATED"만 사용)
        commission_rate (float): 매매 체결 시 왕복 수수료율
        slippage_rate (float): 매매 체결 시 슬리피지 비율

    Returns:
        Dict[str, Any]: {
            "equity_curve": List[float],   # 각 시점별 평가자산
            "daily_returns": List[float],  # 각 시점별 수익률
            "trades": List[dict]           # 체결된 매매 내역
        }
    """
    if df.empty:
        raise ValueError("DataFrame이 비어 있습니다.")
    if "close" not in df.columns:
        raise ValueError("DataFrame에 'close' 칼럼이 필요합니다.")
    if len(df) != len(signals):
        raise ValueError("df 길이와 signals 길이가 다릅니다.")
    if margin_type.upper() != "ISOLATED":
        print("[주의] margin_type은 'ISOLATED'만 가정합니다.")

    n = len(df)
    close_arr = df["close"].values
    signals_arr = np.array(signals, dtype=int)

    capital = start_capital
    position = 0        # 0: 포지션 없음, 1: 롱, -1: 숏
    position_size = 0.0
    entry_price = 0.0
    entry_index = None

    equity_curve = np.zeros(n, dtype=np.float64)
    daily_returns = np.zeros(n, dtype=np.float64)
    trades = []

    prev_equity = capital

    for i in range(n):
        raw_sig = signals_arr[i]
        close_price = close_arr[i]

        # 현재 포지션 평가액
        if position == 1:
            eval_pnl = (close_price - entry_price) * position_size
            current_equity = capital + eval_pnl
        elif position == -1:
            eval_pnl = (entry_price - close_price) * position_size
            current_equity = capital + eval_pnl
        else:
            current_equity = capital

        # 이미 포지션이 있다면 신호가 0 또는 반대가 나오면 청산
        if position != 0:
            if raw_sig == 0 or raw_sig == -position:
                exit_price = close_price
                trade_type = "long" if position == 1 else "short"

                # 슬리피지 반영
                if position == 1:
                    exit_price *= (1.0 - slippage_rate)
                    pnl = (exit_price - entry_price) * position_size
                else:
                    exit_price *= (1.0 + slippage_rate)
                    pnl = (entry_price - exit_price) * position_size

                # 왕복 수수료
                total_price = (entry_price + exit_price) * position_size
                commission = total_price * commission_rate
                net_pnl = pnl - commission

                trades.append({
                    "entry_index": entry_index,
                    "exit_index": i,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": net_pnl,
                    "holding_days": i - entry_index,
                    "position_type": trade_type
                })

                capital += net_pnl
                # 포지션 해제
                position = 0
                position_size = 0.0
                entry_price = 0.0
                entry_index = None
                current_equity = capital

        # 포지션이 없다면 raw_sig=+1(매수) 또는 -1(매도)일 때 진입
        else:
            if raw_sig == 1:
                position = 1
                real_entry_price = close_price * (1.0 + slippage_rate)
                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i
            elif raw_sig == -1 and allow_short:
                position = -1
                real_entry_price = close_price * (1.0 - slippage_rate)
                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i

        # 해당 일자의 수익률, 에쿼티 저장
        ret = 0.0
        if prev_equity != 0.0:
            ret = (current_equity - prev_equity) / prev_equity
        daily_returns[i] = ret
        equity_curve[i] = current_equity
        prev_equity = current_equity

    # 마지막 봉에서 포지션이 남아있다면 강제 청산
    if position != 0:
        final_idx = n - 1
        final_close = close_arr[final_idx]

        if position == 1:
            final_close *= (1.0 - slippage_rate)
            pnl = (final_close - entry_price) * position_size
        else:  # 숏 포지션
            final_close *= (1.0 + slippage_rate)
            pnl = (entry_price - final_close) * position_size

        total_price = (entry_price + final_close) * position_size
        commission = total_price * commission_rate
        net_pnl = pnl - commission
        trade_type = "long" if position == 1 else "short"

        trades.append({
            "entry_index": entry_index,
            "exit_index": final_idx + 1,
            "entry_price": entry_price,
            "exit_price": final_close,
            "pnl": net_pnl,
            "holding_days": (final_idx + 1 - entry_index),
            "position_type": trade_type
        })
        capital += net_pnl

        # 마지막 일자의 수익률 갱신
        ret = 0.0
        if prev_equity != 0.0:
            ret = (capital - prev_equity) / prev_equity
        daily_returns[final_idx] = ret
        equity_curve[final_idx] = capital

    # numpy array -> list 변환
    equity_curve_list = equity_curve.tolist()
    daily_returns_list = daily_returns.tolist()

    return {
        "equity_curve": equity_curve_list,
        "daily_returns": daily_returns_list,
        "trades": trades
    }
