# gptbitcoin/backtester/backtest_engine.py

import pandas as pd
from settings import config


def run_backtest(
    df: pd.DataFrame,
    position_series: pd.Series,
    initial_capital: float = None,
    timeframe_hours: float = 4.0,
    scale_slippage: bool = True
):
    """
    거래별 PnL(체결 정보)을 추가로 저장하는 백테스트 엔진 예시.
    - df : 반드시 'close' 컬럼이 있어야 함
    - position_series : +1(롱), -1(숏), 0(현금). 인덱스=df.index와 동일
    - 초기 자본 등 설정값은 config.py와 연동(미지정 시 default)

    Returns
    -------
    result : pd.DataFrame
        (index=df.index)
        - 'position' : 최종 포지션(+1/0/-1)
        - 'price'    : 종가 (참고용)
        - 'cash'     : 현금잔고
        - 'pnl'      : 누적손익
        - 'equity'   : 총자산(현금+포지션 평가)
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

    # 짧은 봉이면 슬리피지 가중(단순 예시)
    if scale_slippage and timeframe_hours < 4.0:
        factor = 4.0 / timeframe_hours
        slippage_rate = SLIPPAGE_BASE * factor
    else:
        slippage_rate = SLIPPAGE_BASE

    # 결과 저장 DF
    result = pd.DataFrame(index=df.index, columns=["position","price","cash","pnl","equity"])
    result["price"] = df["close"]

    # 거래 정보 저장 리스트
    trades_info = []

    cash = initial_capital
    current_position = 0
    entry_price = 0.0
    shares = 0.0
    cumulative_pnl = 0.0

    # 진입 시점(인덱스)도 저장
    entry_idx = None
    entry_time = None

    # 인덱스 순회 (loc 사용 권장)
    for i, idx in enumerate(df.index):
        desired_pos = position_series.loc[idx]
        price_now = df.loc[idx, "close"]

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

            # 포지션이 바뀌었다면
            if new_pos != prev_pos:
                # (1) 기존 포지션 청산
                if prev_pos != 0:
                    if prev_pos == 1:
                        # 롱 청산 → bid
                        fill_price = price_now * (1.0 - (BID_ASK_SPREAD*0.5 + slippage_rate))
                        pnl_close  = (fill_price - entry_price) * shares
                    else:
                        # 숏 청산 → ask
                        fill_price = price_now * (1.0 + (BID_ASK_SPREAD*0.5 + slippage_rate))
                        pnl_close  = (entry_price - fill_price) * shares

                    cash += pnl_close
                    cumulative_pnl += pnl_close

                    # 수수료
                    trade_notional = abs(fill_price * shares)
                    fee = trade_notional * FEE_RATE
                    cash -= fee
                    cumulative_pnl -= fee

                    # 거래 정보 기록
                    trades_info.append({
                        "entry_idx": entry_idx,
                        "entry_price": entry_price,
                        "entry_time": entry_time,
                        "exit_idx": idx,
                        "exit_price": fill_price,
                        "exit_time": idx,
                        "direction": prev_pos,
                        "pnl": pnl_close - fee,  # 이 건의 순손익
                        "cumulative_pnl": cumulative_pnl
                    })

                    # 포지션 리셋
                    current_position = 0
                    shares = 0.0
                    entry_price = 0.0
                    entry_idx = None
                    entry_time = None

                # (2) 새 포지션 진입
                if new_pos != 0:
                    if new_pos == 1:
                        # 롱 진입 → ask
                        fill_price = price_now * (1.0 + (BID_ASK_SPREAD*0.5 + slippage_rate))
                    else:
                        # 숏 진입 → bid
                        fill_price = price_now * (1.0 - (BID_ASK_SPREAD*0.5 + slippage_rate))

                    invest_capital = cash * LEVERAGE
                    shares = invest_capital / fill_price

                    # 수수료
                    trade_notional = abs(fill_price * shares)
                    fee = trade_notional * FEE_RATE
                    cash -= fee
                    cumulative_pnl -= fee

                    current_position = new_pos
                    entry_price = fill_price
                    entry_idx = idx
                    entry_time = idx

        # (C) 결과 기록 (loc 할당)
        result.loc[idx, "position"] = current_position
        result.loc[idx, "cash"]     = cash

        # 포지션 평가
        if current_position == 0:
            pos_valuation = 0.0
        else:
            if current_position == 1:
                pos_valuation = price_now * shares
            else:
                pos_valuation = shares * entry_price + shares*(entry_price - price_now)

        eq = cash + pos_valuation
        result.loc[idx, "pnl"]    = cumulative_pnl
        result.loc[idx, "equity"] = eq

        # (D) 잔고가 0 이하 → 중단 (남은 구간 0으로)
        if cash <= 0:
            remain_idx = df.index[i:]
            result.loc[remain_idx, "position"] = 0
            result.loc[remain_idx, "cash"]     = 0.0
            result.loc[remain_idx, "pnl"]      = cumulative_pnl
            result.loc[remain_idx, "equity"]   = 0.0
            break

    return result, trades_info


def main():
    """
    간단 테스트
    """
    # 예시 데이터 (10캔들)
    data = {
        "close": [100, 102, 99, 95, 90, 88, 85, 50, 30, 10]
    }
    df_example = pd.DataFrame(data)

    # 계속 롱(1)
    pos_series = pd.Series([1]*len(df_example), index=df_example.index)

    result_df, trades_info = run_backtest(df_example, pos_series, timeframe_hours=1.0, scale_slippage=True)
    print("=== Backtest Result ===")
    print(result_df)
    print("\n=== Trades Info ===")
    for t in trades_info:
        print(t)


if __name__ == "__main__":
    main()
