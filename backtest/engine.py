# gptbitcoin/backtest/engine.py

from typing import Dict, Any, List
from datetime import datetime
import math
import pandas as pd
from config.config import COMMISSION_RATE, SLIPPAGE_RATE


def run_backtest(
        df: pd.DataFrame,
        signals: List[int],
        start_capital: float = 100000,
        allow_short: bool = True
) -> Dict[str, Any]:
    """
    종가 체결 기준의 간단 백테스트 엔진.
    - df: 사전 계산된 보조지표와 OHLCV가 들어있는 DataFrame. 반드시 'close' 열 필요.
    - signals: +1(매수), -1(매도), 0(관망) 리스트
    - start_capital: 초기자금
    - allow_short: 숏 가능 여부

    Returns:
        {
          "equity_curve": [...],
          "daily_returns": [...],
          "trades": [
             {
               "entry_index": ...,
               "exit_index": ...,
               "entry_price": ...,
               "exit_price": ...,
               "pnl": ...,
               "holding_days": ...,
               "position_type": "long" or "short"
             }, ...
          ]
        }
    """

    if df.empty:
        raise ValueError("DataFrame is empty")
    if "close" not in df.columns:
        raise ValueError("df must contain 'close' column")

    length = len(df)
    if length != len(signals):
        raise ValueError("df length != signals length")

    capital = start_capital
    position = 0  # 0=현금, 1=롱, -1=숏
    entry_price = 0.0
    entry_index = None
    equity_curve = []
    daily_returns = []
    trades = []
    prev_equity = capital

    for i in range(length):
        close_price = df.iloc[i]["close"]
        sig = signals[i]

        # 평가금액 계산
        if position == 1:
            current_equity = capital + (close_price - entry_price)
        elif position == -1:
            current_equity = capital + (entry_price - close_price)
        else:
            current_equity = capital

        # 시그널 변화 감지
        if sig != position:
            # 기존 포지션 청산
            if position != 0:
                pnl = current_equity - capital
                exit_price = close_price
                trade_type = "long" if position == 1 else "short"

                if position == 1:
                    exit_price *= (1 - SLIPPAGE_RATE)
                else:
                    exit_price *= (1 + SLIPPAGE_RATE)

                commission = abs(exit_price - entry_price) * COMMISSION_RATE
                net_pnl = pnl - commission

                holding_days = 1
                if entry_index is not None:
                    holding_days = (i - entry_index)

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

            # 새 포지션 진입
            if sig == 1:
                position = 1
                entry_price = close_price * (1 + SLIPPAGE_RATE)
                entry_index = i
            elif sig == -1 and allow_short:
                position = -1
                entry_price = close_price * (1 - SLIPPAGE_RATE)
                entry_index = i
            else:
                position = 0
                entry_price = 0.0
                entry_index = None

            current_equity = capital

        # 일별 수익률
        ret = (current_equity - prev_equity) / prev_equity if prev_equity != 0 else 0.0
        daily_returns.append(ret)
        equity_curve.append(current_equity)
        prev_equity = current_equity

    # 마지막 봉에서 포지션 청산
    if position != 0:
        final_idx = length - 1
        final_close = df.iloc[final_idx]["close"]

        if position == 1:
            final_close *= (1 - SLIPPAGE_RATE)
        else:
            final_close *= (1 + SLIPPAGE_RATE)

        pnl = (final_close - entry_price) if position == 1 else (entry_price - final_close)
        commission = abs(final_close - entry_price) * COMMISSION_RATE
        net_pnl = pnl - commission
        trade_type = "long" if position == 1 else "short"

        holding_days = 1
        if entry_index is not None:
            holding_days = (final_idx - entry_index) + 1

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

        # 마지막에 수익률 업데이트
        ret = (capital - prev_equity) / prev_equity if prev_equity else 0.0
        daily_returns[-1] = ret
        equity_curve[-1] = capital

    return {
        "equity_curve": equity_curve,
        "daily_returns": daily_returns,
        "trades": trades
    }
