# gptbitcoin/backtest/engine.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 이 엔진은 매수/매도 신호 지연(buy_time_delay, sell_time_delay)과 포지션 보유(holding_period) 로직을 처리한다.
# holding_period가 유한한 값이면 해당 봉 수 이상 보유한 후 반대 신호가 있을 때 청산하고,
# holding_period가 float('inf')인 경우에는 시간 기준 청산 조건 없이 반대 신호나 0 신호가 오면 즉시 청산한다.

from typing import Dict, Any, List
import pandas as pd


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
    백테스트 엔진.

    매수/매도 신호 지연과 보유 기간을 적용하여 포지션을 진입 및 청산한다.
    holding_period가 유한한 값이면, 해당 봉 수 이상 보유한 후에만 반대 신호(또는 0 신호)가 있을 때 청산한다.
    holding_period가 float('inf')인 경우에는 시간 조건을 무시하고, 반대 신호(또는 0 신호)가 나오면 즉시 청산한다.

    Args:
        df (pd.DataFrame): 'close' 칼럼을 포함한 가격 시계열 데이터 (len(df)==len(signals))
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
            - 유한한 값이면 해당 봉 수 이상 보유한 후 반대 신호가 있으면 청산.
            - float('inf')이면 오직 반대 신호나 0 신호가 발생할 때 청산.

    Returns:
        Dict[str, Any]: {
            "equity_curve": List[float],   # 각 시점별 평가자산
            "daily_returns": List[float],    # 각 시점별 수익률
            "trades": List[dict]             # 체결된 매매 내역
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

    capital = start_capital
    position = 0  # 0: 포지션 없음, 1: 롱, -1: 숏
    position_size = 0.0
    entry_price = 0.0
    entry_index = None

    last_raw_signal = 0
    raw_signal_count = 0
    bars_held = 0

    equity_curve = []
    daily_returns = []
    trades = []
    prev_equity = capital

    for i in range(len(df)):
        close_price = df.iloc[i]["close"]
        raw_sig = signals[i]

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
            bars_held += 1
            # 유한한 holding_period의 경우: 지정된 봉 수 이상 보유 후, 반대 신호 또는 관망 신호가 있을 때 청산
            # holding_period가 float('inf')인 경우: 시간 조건은 무시하고, 반대 신호 또는 관망 신호가 있으면 청산
            exit_condition = False
            if holding_period != float('inf'):
                if bars_held >= holding_period and (raw_sig == 0 or raw_sig == -position):
                    exit_condition = True
            else:
                if raw_sig == 0 or raw_sig == -position:
                    exit_condition = True

            if exit_condition:
                exit_price = close_price
                trade_type = "long" if position == 1 else "short"

                if position == 1:
                    exit_price *= (1 - slippage_rate)
                    pnl = (exit_price - entry_price) * position_size
                else:
                    exit_price *= (1 + slippage_rate)
                    pnl = (entry_price - exit_price) * position_size

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

                position = 0
                position_size = 0.0
                entry_price = 0.0
                entry_index = None
                bars_held = 0
                current_equity = capital
        else:
            # 포지션 없을 때, 연속 신호 카운트 갱신
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
                    real_entry_price *= (1 + slippage_rate)
                else:
                    real_entry_price *= (1 - slippage_rate)

                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i
                bars_held = 0

        ret = (current_equity - prev_equity) / prev_equity if prev_equity != 0 else 0.0
        daily_returns.append(ret)
        equity_curve.append(current_equity)
        prev_equity = current_equity

    # 마지막 봉에서 남은 포지션 강제 청산
    if position != 0:
        final_idx = len(df) - 1
        final_close = df.iloc[final_idx]["close"]
        if position == 1:
            final_close *= (1 - slippage_rate)
            pnl = (final_close - entry_price) * position_size
        else:
            final_close *= (1 + slippage_rate)
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
            "holding_days": bars_held + 1,
            "position_type": trade_type
        })
        capital += net_pnl
        ret = (capital - prev_equity) / prev_equity if prev_equity != 0 else 0.0
        daily_returns[-1] = ret
        equity_curve[-1] = capital

    return {
        "equity_curve": equity_curve,
        "daily_returns": daily_returns,
        "trades": trades
    }
