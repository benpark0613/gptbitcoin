# gptbitcoin/backtester/backtest_engine.py

import pandas as pd
import numpy as np
from settings import config

def run_backtest(
    df: pd.DataFrame,
    position_series: pd.Series,
    initial_capital: float = None,
    timeframe_hours: float = 4.0,
    scale_slippage: bool = True
):
    """
    거래별 PnL(체결 정보)을 추가로 저장하는 백테스트 엔진.
    - df : 반드시 'close' 컬럼이 있어야 함
    - position_series : +1(롱), -1(숏), 0(현금). 인덱스=df.index와 동일
    - 초기 자본 등 설정값은 config.py와 연동(미지정 시 default)

    Returns
    -------
    result : pd.DataFrame
        (index=df.index)
        - 'position' : 최종 포지션(+1/0/-1) (int)
        - 'price'    : 종가 (float)
        - 'cash'     : 현금잔고 (float)
        - 'pnl'      : 누적손익 (float)
        - 'equity'   : 총자산(현금+포지션 평가, float)
    trades_info : list of dict
        거래별 상세 정보 목록.
        각 원소: {
          "entry_idx": ...,
          "entry_price": ...,
          "entry_time": ...,
          "exit_idx": ...,
          "exit_price": ...,
          "exit_time": ...,
          "direction": (1 or -1),
          "pnl": float(단일 거래 손익),
          "cumulative_pnl": float(해당 시점 누적 손익)
        }
    """

    if initial_capital is None:
        initial_capital = config.INIT_CAPITAL

    FEE_RATE       = config.FEE_RATE
    BID_ASK_SPREAD = config.BID_ASK_SPREAD
    SLIPPAGE_BASE  = config.SLIPPAGE_RATE
    MARGIN_INTEREST= config.MARGIN_INTEREST
    LEVERAGE       = config.LEVERAGE
    ALLOW_SHORT    = config.ALLOW_SHORT

    # 시간프레임별 마진 이자 계산
    daily_fraction = timeframe_hours / 24.0

    # (예시) 모든 봉에 동일한 슬리피지 적용한다고 가정:
    slippage_rate = SLIPPAGE_BASE

    result = pd.DataFrame(index=df.index, columns=["position","price","cash","pnl","equity"])
    # price, cash, pnl, equity 등 모두 float 형태로 다룰 예정
    result["price"] = df["close"].astype(float)

    trades_info = []

    cash = float(initial_capital)  # 현금도 float
    current_position = 0
    entry_price = 0.0
    shares = 0.0
    cumulative_pnl = 0.0

    entry_idx = None
    entry_time = None

    for i, idx in enumerate(df.index):
        desired_pos = position_series.loc[idx]
        price_now = float(df.loc[idx, "close"])

        if not ALLOW_SHORT and desired_pos == -1:
            desired_pos = 0

        # (A) 보유 중이면 마진 이자 차감
        if current_position != 0:
            notional = abs(shares * price_now)
            interest_cost = notional * MARGIN_INTEREST * daily_fraction
            cash -= interest_cost
            cumulative_pnl -= interest_cost

        # (B) 포지션 변경 체크
        if i > 0:
            prev_pos = current_position
            new_pos = desired_pos

            if new_pos != prev_pos:
                # (1) 기존 포지션 청산
                if prev_pos != 0:
                    if prev_pos == 1:
                        fill_price = price_now * (1.0 - (BID_ASK_SPREAD*0.5 + slippage_rate))
                        pnl_close  = (fill_price - entry_price) * shares
                    else:  # prev_pos == -1
                        fill_price = price_now * (1.0 + (BID_ASK_SPREAD*0.5 + slippage_rate))
                        pnl_close  = (entry_price - fill_price) * shares

                    cash += pnl_close
                    cumulative_pnl += pnl_close

                    trade_notional = abs(fill_price * shares)
                    fee = trade_notional * FEE_RATE
                    cash -= fee
                    cumulative_pnl -= fee

                    trades_info.append({
                        "entry_idx": entry_idx,
                        "entry_price": float(entry_price),
                        "entry_time": entry_time,
                        "exit_idx": idx,
                        "exit_price": float(fill_price),
                        "exit_time": idx,
                        "direction": prev_pos,
                        "pnl": float(pnl_close - fee),
                        "cumulative_pnl": float(cumulative_pnl)
                    })

                    current_position = 0
                    shares = 0.0
                    entry_price = 0.0
                    entry_idx = None
                    entry_time = None

                # (2) 새 포지션 진입
                if new_pos != 0:
                    if new_pos == 1:
                        fill_price = price_now * (1.0 + (BID_ASK_SPREAD*0.5 + slippage_rate))
                    else:  # new_pos == -1
                        fill_price = price_now * (1.0 - (BID_ASK_SPREAD*0.5 + slippage_rate))

                    invest_capital = cash * LEVERAGE
                    shares = invest_capital / fill_price

                    trade_notional = abs(fill_price * shares)
                    fee = trade_notional * FEE_RATE
                    cash -= fee
                    cumulative_pnl -= fee

                    current_position = new_pos
                    entry_price = fill_price
                    entry_idx = idx
                    entry_time = idx

        # (C) result에 기록
        result.loc[idx, "position"] = int(current_position)  # position은 int
        result.loc[idx, "cash"]     = float(cash)            # float
        # 포지션 평가
        if current_position == 0:
            pos_valuation = 0.0
        else:
            if current_position == 1:
                pos_valuation = price_now * shares
            else:  # -1
                # 숏 포지션의 평가: (entry_price - 현재가격)*shares + (entry_price*shares)
                # 또는 아래처럼 분해
                pos_valuation = shares * entry_price + shares*(entry_price - price_now)

        eq = cash + pos_valuation
        result.loc[idx, "pnl"]    = float(cumulative_pnl)
        result.loc[idx, "equity"] = float(eq)

        # (D) 잔고가 0 이하 → 중단
        if cash <= 0:
            remain_idx = df.index[i:]
            result.loc[remain_idx, "position"] = 0
            result.loc[remain_idx, "cash"]     = 0.0
            result.loc[remain_idx, "pnl"]      = float(cumulative_pnl)
            result.loc[remain_idx, "equity"]   = 0.0
            break

    # 모든 수치 칼럼이 float로 통일되었는지 최종 확인(혹시 object dtype이 섞이지 않도록)
    # 필요하면 아래처럼 명시적으로 astype을 적용해도 됨:
    result = result.astype({
        "position": "int32",
        "price": "float64",
        "cash": "float64",
        "pnl": "float64",
        "equity": "float64"
    })

    return result, trades_info


def main():
    """
    간단 테스트
    """
    data = {"close": [100, 102, 99, 95, 90, 88, 85, 50, 30, 10]}
    df_example = pd.DataFrame(data)

    # 계속 롱(1)
    pos_series = pd.Series([1]*len(df_example), index=df_example.index)

    result_df, trades_info = run_backtest(df_example, pos_series, timeframe_hours=1.0, scale_slippage=True)
    print("=== Backtest Result ===")
    print(result_df)
    print("dtypes:", result_df.dtypes)

    print("\n=== Trades Info ===")
    for t in trades_info:
        print(t)
