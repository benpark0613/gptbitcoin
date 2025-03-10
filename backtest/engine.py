# gptbitcoin/backtest/engine.py
# 최소한의 한글 주석, 구글 스타일 docstring
# numpy 배열을 사용해 백테스트를 수행하는 엔진 모듈.
# 매수/매도 신호 지연(buy_time_delay, sell_time_delay)과 포지션 보유(holding_period) 로직을 처리한다.
# holding_period가 유한한 값이면 해당 봉 수 이상 보유한 후 반대 신호가 있을 때 청산하고,
# holding_period가 float('inf')인 경우에는 시간 기준 청산 조건 없이 반대 신호나 0 신호가 오면 즉시 청산한다.

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
    slippage_rate: float = 0.0002,
    buy_time_delay: int = 0,
    sell_time_delay: int = 0,
    holding_period: float = float('inf')
) -> Dict[str, Any]:
    """
    백테스트 엔진 (numpy 기반).
    df["close"]와 signals 배열을 이용해 매수/매도 로직을 처리한다.

    Args:
        df (pd.DataFrame): 최소 "close" 칼럼과 signals와 같은 길이의 행이 필요.
        signals (List[int]): 각 시점의 매매 신호 (-1: 매도, 0: 관망, +1: 매수)
        start_capital (float): 초기 자본
        allow_short (bool): 숏 포지션 허용 여부
        leverage (float): 레버리지 배수
        margin_type (str): 마진 유형 ("ISOLATED"만 사용)
        commission_rate (float): 진입/청산 시 왕복 수수료율
        slippage_rate (float): 슬리피지 비율
        buy_time_delay (int): 매수 신호 지연 봉 수
        sell_time_delay (int): 매도 신호 지연 봉 수
        holding_period (float): 포지션 보유 봉 수
          - 유한한 값이면 해당 봉 수 이상 보유한 후 반대 신호(또는 0 신호)가 나오면 청산.
          - float('inf')이면 시간 제한 없이, 반대 신호나 0 신호가 오면 즉시 청산.

    Returns:
        Dict[str, Any]: {
            "equity_curve": List[float],     # 각 시점별 평가자산
            "daily_returns": List[float],    # 각 시점별 수익률
            "trades": List[dict]            # 체결된 매매 내역
        }
    """
    if df.empty:
        raise ValueError("DataFrame이 비어 있습니다.")
    if "close" not in df.columns:
        raise ValueError("DataFrame에 'close' 칼럼이 필요합니다.")
    if len(df) != len(signals):
        raise ValueError("df 길이와 signals 길이가 불일치합니다.")
    if margin_type.upper() != "ISOLATED":
        print("[주의] run_backtest: margin_type은 'ISOLATED'만 가정합니다.")

    n = len(df)

    # numpy array로 변환
    close_arr = df["close"].values
    signals_arr = np.array(signals, dtype=int)

    capital = start_capital
    position = 0            # 0: 포지션 없음, 1: 롱, -1: 숏
    position_size = 0.0
    entry_price = 0.0
    entry_index = None

    last_raw_signal = 0
    raw_signal_count = 0
    bars_held = 0

    equity_curve = np.zeros(n, dtype=np.float64)
    daily_returns = np.zeros(n, dtype=np.float64)
    trades = []

    prev_equity = capital

    for i in range(n):
        raw_sig = signals_arr[i]
        close_price = close_arr[i]

        # 현재 포지션 평가
        if position == 1:
            eval_pnl = (close_price - entry_price) * position_size
            current_equity = capital + eval_pnl
        elif position == -1:
            eval_pnl = (entry_price - close_price) * position_size
            current_equity = capital + eval_pnl
        else:
            current_equity = capital

        if position != 0:
            # 포지션 보유중
            bars_held += 1
            # 보유 기간이 유한하면 bars_held>=holding_period 시점에
            # (raw_sig가 0이거나 반대신호면) exit_condition=True
            # holding_period가 inf라면 시그널이 반대(또는 0)일 때만 exit
            exit_condition = False
            if holding_period != float('inf'):
                if (bars_held >= holding_period) and (raw_sig == 0 or raw_sig == -position):
                    exit_condition = True
            else:
                if (raw_sig == 0 or raw_sig == -position):
                    exit_condition = True

            if exit_condition:
                exit_price = close_price
                trade_type = "long" if position == 1 else "short"

                # 슬리피지 반영
                if position == 1:
                    exit_price *= (1.0 - slippage_rate)
                    pnl = (exit_price - entry_price) * position_size
                else:
                    exit_price *= (1.0 + slippage_rate)
                    pnl = (entry_price - exit_price) * position_size

                # 커미션
                total_price = (entry_price + exit_price) * position_size
                commission = total_price * commission_rate
                net_pnl = pnl - commission

                trades.append({
                    "entry_index": entry_index,
                    "exit_index": i,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": net_pnl,
                    "holding_days": bars_held,
                    "position_type": trade_type
                })

                capital += net_pnl

                # 포지션 청산
                position = 0
                position_size = 0.0
                entry_price = 0.0
                entry_index = None
                bars_held = 0
                current_equity = capital
        else:
            # 포지션 없음
            # 신호 지연 로직
            if raw_sig != last_raw_signal:
                raw_signal_count = 1
                last_raw_signal = raw_sig
            else:
                raw_signal_count = raw_signal_count + 1 if raw_sig != 0 else 0

            if raw_sig > 0:
                required_delay = buy_time_delay
            elif raw_sig < 0:
                required_delay = sell_time_delay
            else:
                required_delay = float('inf')

            can_enter_long = (raw_sig == 1 and raw_signal_count >= required_delay)
            can_enter_short = (raw_sig == -1 and raw_signal_count >= required_delay and allow_short)

            if can_enter_long or can_enter_short:
                position = 1 if can_enter_long else -1
                trade_type = "long" if position == 1 else "short"

                real_entry_price = close_price
                if position == 1:
                    real_entry_price *= (1.0 + slippage_rate)
                else:
                    real_entry_price *= (1.0 - slippage_rate)

                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i
                bars_held = 0

        # daily_returns, equity_curve
        ret = 0.0
        if prev_equity != 0.0:
            ret = (current_equity - prev_equity) / prev_equity
        daily_returns[i] = ret
        equity_curve[i] = current_equity
        prev_equity = current_equity

    # 마지막 봉에서 남은 포지션 강제 청산
    if position != 0:
        final_idx = n - 1
        final_close = close_arr[final_idx]

        if position == 1:
            final_close *= (1.0 - slippage_rate)
            pnl = (final_close - entry_price) * position_size
        else:  # position == -1
            final_close *= (1.0 + slippage_rate)
            pnl = (entry_price - final_close) * position_size

        total_price = (entry_price + final_close) * position_size
        commission = total_price * commission_rate
        net_pnl = pnl - commission
        trade_type = "long" if position == 1 else "short"

        trades.append({
            "entry_index": entry_index,
            "exit_index": final_idx + 1,  # 마지막 index+1
            "entry_price": entry_price,
            "exit_price": final_close,
            "pnl": net_pnl,
            "holding_days": bars_held + 1,
            "position_type": trade_type
        })
        capital += net_pnl

        # 마지막 일자의 리턴 갱신
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
