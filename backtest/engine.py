# gptbitcoin/backtest/engine.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 매수/매도 신호 지연(buy_time_delay, sell_time_delay), 포지션 보유(holding_period) 로직이 포함된 백테스트 엔진

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
    holding_period: int = 0
) -> Dict[str, Any]:
    """
    백테스트 엔진:
      - 매수/매도 신호 지연 로직을 별도 적용:
        * buy_time_delay: 매수 신호가 나온 뒤 지정된 봉 수만큼 동일한 신호가 지속되면 진입
        * sell_time_delay: 매도 신호가 나온 뒤 지정된 봉 수만큼 동일한 신호가 지속되면 진입
      - holding_period: 포지션 진입 후 일정 봉만큼은 유지(0이면 제약 없음)
      - 나머지 로직(수수료, 슬리피지, 레버리지, 숏 진입 등)은 전량 매수/매도 방식으로 처리

    Args:
        df (pd.DataFrame): 'close' 칼럼이 포함된 가격 시계열 (len(df) == len(signals) 가정)
        signals (List[int]): 각 시점의 매매 신호 (-1: 매도, 0: 관망, +1: 매수)
        start_capital (float, optional): 초기 자본
        allow_short (bool, optional): True면 숏 포지션 허용
        leverage (float, optional): 레버리지 배수
        margin_type (str, optional): "ISOLATED"만 가정
        commission_rate (float, optional): 진입/청산 시 왕복 수수료율
        slippage_rate (float, optional): 슬리피지 비율
        buy_time_delay (int, optional): 매수 신호 지연 봉 수
        sell_time_delay (int, optional): 매도 신호 지연 봉 수
        holding_period (int, optional): 포지션 유지 봉 수 (0이면 무제한)

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
        raise ValueError("df 길이와 signals 길이가 불일치.")

    if margin_type.upper() != "ISOLATED":
        print("[주의] run_backtest: margin_type='ISOLATED'만 가정합니다.")

    capital = start_capital
    position = 0    # 0: 무포지션, +1: 롱, -1: 숏
    position_size = 0.0
    entry_price = 0.0
    entry_index = None

    # 포지션 보유, 딜레이 관련 상태
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

        # 현재 포지션 평가자산
        if position == 1:  # 롱 포지션
            eval_pnl = (close_price - entry_price) * position_size
            current_equity = capital + eval_pnl
        elif position == -1:  # 숏 포지션
            eval_pnl = (entry_price - close_price) * position_size
            current_equity = capital + eval_pnl
        else:
            current_equity = capital

        # (A) 포지션 보유 중 => holding_period 체크
        if position != 0:
            bars_held += 1
            can_exit = (holding_period == 0) or (bars_held >= holding_period)

            # 새 신호가 포지션 반대거나 0이면 청산 판단
            if can_exit:
                if raw_sig == 0 or raw_sig == -position:
                    # 청산
                    exit_price = close_price
                    trade_type = "long" if position == 1 else "short"

                    # 슬리피지 적용
                    if position == 1:
                        exit_price *= (1 - slippage_rate)
                        pnl = (exit_price - entry_price) * position_size
                    else:
                        exit_price *= (1 + slippage_rate)
                        pnl = (entry_price - exit_price) * position_size

                    # 왕복 수수료
                    total_price_sum = (entry_price + exit_price) * position_size
                    commission = total_price_sum * commission_rate
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

                    # 포지션 해제
                    position = 0
                    position_size = 0.0
                    entry_price = 0.0
                    entry_index = None
                    bars_held = 0

                    current_equity = capital

        else:
            # (B) 포지션이 없는 경우 => buy_time_delay / sell_time_delay 반영
            # raw_signal_count 업데이트
            if raw_sig != last_raw_signal:
                raw_signal_count = 1
                last_raw_signal = raw_sig
            else:
                if raw_sig != 0:
                    raw_signal_count += 1
                else:
                    raw_signal_count = 0

            # 매수/매도 진입 가능 여부
            if raw_sig > 0:
                required_delay = buy_time_delay
            elif raw_sig < 0:
                required_delay = sell_time_delay
            else:
                required_delay = 999999  # 신호가 0이면 진입 불가

            can_enter_long = (raw_sig == 1 and raw_signal_count >= required_delay)
            can_enter_short = (raw_sig == -1 and raw_signal_count >= required_delay and allow_short)

            if can_enter_long or can_enter_short:
                position = 1 if can_enter_long else -1
                trade_type = "long" if position == 1 else "short"

                real_entry_price = close_price
                if position == 1:
                    real_entry_price *= (1 + slippage_rate)  # 매수 슬리피지
                else:
                    real_entry_price *= (1 - slippage_rate)  # 매도 슬리피지

                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i
                bars_held = 0

        # (C) 일일 수익률(단순 수익률)
        ret = (current_equity - prev_equity) / prev_equity if prev_equity else 0.0
        daily_returns.append(ret)
        equity_curve.append(current_equity)
        prev_equity = current_equity

    # (D) 루프 종료 후 포지션 남았다면 마지막 봉에서 청산
    if position != 0:
        final_idx = len(df) - 1
        final_close = df.iloc[final_idx]["close"]

        if position == 1:
            final_close *= (1 - slippage_rate)
            pnl = (final_close - entry_price) * position_size
        else:
            final_close *= (1 + slippage_rate)
            pnl = (entry_price - final_close) * position_size

        total_price_sum = (entry_price + final_close) * position_size
        commission = total_price_sum * commission_rate
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
