# gptbitcoin/backtest/engine.py
# 전량 매수·매도, ISOLATED 마진, ALLOW_SHORT 사용 등 보완사항 반영

from typing import Dict, Any, List
import pandas as pd

# config.py 설정값 불러오기
from config.config import (
    COMMISSION_RATE,
    SLIPPAGE_RATE,
    START_CAPITAL,
    ALLOW_SHORT,
    LEVERAGE,
    MARGIN_TYPE
)

def run_backtest(
    df: pd.DataFrame,
    signals: List[int],
    start_capital: float = START_CAPITAL,
    allow_short: bool = ALLOW_SHORT,
    leverage: float = LEVERAGE,
    margin_type: str = MARGIN_TYPE
) -> Dict[str, Any]:
    """
    전량 매수·매도 방식의 간단 백테스트 엔진.
    ISOLATED 마진, 레버리지, 숏(ALLOW_SHORT) 사용 가능.

    Args:
        df (pd.DataFrame): 'close' 칼럼 포함된 시계열 데이터.
        signals (List[int]): 각 시점의 매매 시그널. +1(매수), -1(매도), 0(관망).
        start_capital (float, optional): 초기 자본.
        allow_short (bool, optional): True면 숏 진입 가능.
        leverage (float, optional): 레버리지 배수.
        margin_type (str, optional): "ISOLATED" (단일 포지션 마진).
                                     본 코드에서는 ISOLATED만 테스트.

    Returns:
        Dict[str, Any]: {
            "equity_curve": List[float],   # 각 시점별 평가자산
            "daily_returns": List[float],  # 각 시점별 일일수익률
            "trades": List[dict]          # 트레이드 내역
        }
    """
    if df.empty:
        raise ValueError("DataFrame is empty.")
    if "close" not in df.columns:
        raise ValueError("DataFrame에 'close' 칼럼이 필요합니다.")
    if len(df) != len(signals):
        raise ValueError("df 길이와 signals 길이가 일치하지 않습니다.")
    if margin_type.upper() != "ISOLATED":
        # 여기서는 ISOLATED만 가정(강제청산 등은 별도 고려 안 함)
        print("[주의] 본 백테스트는 ISOLATED만 테스트합니다.")

    capital = start_capital
    position = 0      # 0=무포지션, 1=롱, -1=숏
    entry_price = 0.0
    entry_index = None
    position_size = 0.0  # 레버리지를 고려한 포지션 수량

    equity_curve = []
    daily_returns = []
    trades = []
    prev_equity = capital

    for i in range(len(df)):
        close_price = df.iloc[i]["close"]
        sig = signals[i]

        # 포지션 평가자산 계산(단순 평가)
        if position == 1:
            # 롱 포지션인 경우 (exit_price- entry_price) * 수량
            current_price_eval = close_price  # 평가 시점 가격(슬리피지X)
            eval_pnl = (current_price_eval - entry_price) * position_size
            current_equity = capital + eval_pnl
        elif position == -1:
            # 숏 포지션인 경우 (entry_price - exit_price) * 수량
            current_price_eval = close_price
            eval_pnl = (entry_price - current_price_eval) * position_size
            current_equity = capital + eval_pnl
        else:
            current_equity = capital

        # 시그널 변동 시 기존 포지션 청산 / 신규 진입
        if sig != position:
            # 1) 기존 포지션 청산
            if position != 0:
                exit_price = close_price
                trade_type = "long" if position == 1 else "short"

                # 슬리피지 적용(청산 시)
                if position == 1:
                    exit_price *= (1 - SLIPPAGE_RATE)
                    pnl = (exit_price - entry_price) * position_size
                else:
                    exit_price *= (1 + SLIPPAGE_RATE)
                    pnl = (entry_price - exit_price) * position_size

                # 왕복 수수료(진입+청산 가격 합 × 수량 × 커미션)
                # entry_price, exit_price는 이미 슬리피지 반영된 가격
                total_price_sum = (entry_price + exit_price) * position_size
                commission = total_price_sum * COMMISSION_RATE
                net_pnl = pnl - commission

                holding_days = (i - entry_index) if entry_index is not None else 1
                trades.append({
                    "entry_index": entry_index,
                    "exit_index": i,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": net_pnl,
                    "holding_days": holding_days,
                    "position_type": trade_type
                })
                capital += net_pnl

            # 2) 새 포지션 진입(롱 or 숏)
            if sig == 1:
                position = 1
                # 진입 시 슬리피지 반영
                real_entry_price = close_price * (1 + SLIPPAGE_RATE)
                # 레버리지 고려, 전액 진입 => capital * leverage / 진입가격 = 수량
                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i
            elif sig == -1 and allow_short:
                position = -1
                real_entry_price = close_price * (1 - SLIPPAGE_RATE)
                position_size = (capital * leverage) / real_entry_price
                entry_price = real_entry_price
                entry_index = i
            else:
                # 포지션 청산 후, 새로 진입하지 않음
                position = 0
                position_size = 0.0
                entry_price = 0.0
                entry_index = None

            current_equity = capital

        # 일별 수익률 계산
        ret = (current_equity - prev_equity) / prev_equity if prev_equity != 0 else 0.0
        daily_returns.append(ret)
        equity_curve.append(current_equity)
        prev_equity = current_equity

    # 루프가 끝난 뒤, 포지션이 남아있으면 마지막 봉에서 청산
    if position != 0:
        final_idx = len(df) - 1
        final_close = df.iloc[final_idx]["close"]

        if position == 1:
            final_close *= (1 - SLIPPAGE_RATE)
            pnl = (final_close - entry_price) * position_size
        else:
            final_close *= (1 + SLIPPAGE_RATE)
            pnl = (entry_price - final_close) * position_size

        total_price_sum = (entry_price + final_close) * position_size
        commission = total_price_sum * COMMISSION_RATE
        net_pnl = pnl - commission
        trade_type = "long" if position == 1 else "short"
        holding_days = (final_idx - entry_index) + 1 if entry_index is not None else 1

        trades.append({
            "entry_index": entry_index,
            "exit_index": final_idx + 1,
            "entry_price": entry_price,
            "exit_price": final_close,
            "pnl": net_pnl,
            "holding_days": holding_days,
            "position_type": trade_type
        })
        capital += net_pnl

        # 마지막 수익률 갱신
        ret = (capital - prev_equity) / prev_equity if prev_equity != 0 else 0.0
        daily_returns[-1] = ret
        equity_curve[-1] = capital

    return {
        "equity_curve": equity_curve,
        "daily_returns": daily_returns,
        "trades": trades
    }
