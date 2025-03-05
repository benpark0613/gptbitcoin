# gptbitcoin/backtest/engine.py
# 구글 스타일, 최소한의 한글 주석

from typing import Dict, Any, List
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
    - df: 지표 및 OHLCV가 들어있는 DataFrame. 'close' 열 필수.
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

    if len(df) != len(signals):
        raise ValueError("df length != signals length")

    capital = start_capital
    position = 0  # 0=현금, 1=롱, -1=숏
    entry_price = 0.0
    entry_index = None
    equity_curve = []
    daily_returns = []
    trades = []
    prev_equity = capital

    for i in range(len(df)):
        close_price = df.iloc[i]["close"]
        sig = signals[i]

        # 현재 평가자산 계산
        if position == 1:
            current_equity = capital + (close_price - entry_price)
        elif position == -1:
            current_equity = capital + (entry_price - close_price)
        else:
            current_equity = capital

        # 시그널 변동 시 포지션 청산/진입
        if sig != position:
            # 기존 포지션 청산
            if position != 0:
                exit_price = close_price
                trade_type = "long" if position == 1 else "short"
                if position == 1:
                    exit_price *= (1 - SLIPPAGE_RATE)
                    pnl = exit_price - entry_price
                else:
                    exit_price *= (1 + SLIPPAGE_RATE)
                    pnl = entry_price - exit_price

                # 진입/청산 금액합 * 수수료율 = round trip 수수료
                round_trip_commission = (entry_price + exit_price) * COMMISSION_RATE
                net_pnl = pnl - round_trip_commission

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

            # 새로운 포지션 진입
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
        final_idx = len(df) - 1
        final_close = df.iloc[final_idx]["close"]

        if position == 1:
            final_close *= (1 - SLIPPAGE_RATE)
            pnl = final_close - entry_price
        else:
            final_close *= (1 + SLIPPAGE_RATE)
            pnl = entry_price - final_close

        round_trip_commission = (entry_price + final_close) * COMMISSION_RATE
        net_pnl = pnl - round_trip_commission
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

        # 마지막 수익률 재계산
        ret = (capital - prev_equity) / prev_equity if prev_equity else 0.0
        daily_returns[-1] = ret
        equity_curve[-1] = capital

    return {
        "equity_curve": equity_curve,
        "daily_returns": daily_returns,
        "trades": trades
    }
