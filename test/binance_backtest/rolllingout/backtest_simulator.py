# backtest_simulator.py

import pandas as pd
import numpy as np
import math

def run_backtest(
    df: pd.DataFrame,
    signals: pd.Series,
    fee_rate: float = 0.001,
    slip: float = 0.0,
    riskfree_rate: float = 0.0,
    start_equity: float = 10000.0,
) -> dict:
    """
    간단한 인/아웃(1/0) 백테스트 시뮬레이터:
    - df: OHLCV (index ascending, columns=['open','high','low','close','volume',...])
    - signals: 1(보유), 0(현금) (index=df.index 동일)
    - fee_rate: 거래 수수료 (예: 0.1% => 0.001)
    - slip: 슬리피지율(예: 0.0 ~ 0.0005 등)
    - riskfree_rate: 무위험이자율 (연간) => 일간 환산 등 별도 처리 (단순 0가정 가능)
    - start_equity: 초기 자산 (USD 등)

    return: dict {
        'final_equity': ...,
        'total_return': ...,
        'sharpe': ...,
        'max_drawdown': ...,
        'equity_curve': pd.Series,
        ...
    }
    """
    # df와 signals의 index가 동일한 ascending datetime 가정
    # 일단 종가(‘close’)만 사용한 간단 모형

    # === 1) 시그널 변경점을 잡아 거래 발생 시 수수료·슬리피지 반영 ===
    # position(t) = signals[t], 현 시점=1, 이전=0 => 매수발생
    # 수수료 = fee_rate
    # 슬리피지 = slip * price
    # 간단히 종가 체결로 가정
    positions = signals.shift(1).fillna(0)  # t 시점 보유여부 = t-1 시그널
    trades = signals - positions  # +1=매수, -1=매도 (현재는 0→1,1→0)
    # 인덱스별 +1 or -1 값

    closep = df["close"]
    n = len(df)

    # === 2) equity_curve 계산 ===
    equity_curve = pd.Series(data=np.zeros(n), index=df.index, dtype=float)
    cash = start_equity
    coin_amount = 0.0  # 보유 코인 수
    last_equity = start_equity

    for i in range(n):
        idx = df.index[i]
        sig_now = signals[idx]
        trade_now = trades[idx]
        price = closep[idx]

        # (A) 매매 발생 시
        if trade_now == 1:
            # 매수
            # 수수료+슬리피지 고려한 체결가격 = price * (1 + slip)
            # 매수 금액 = cash (전액)
            # 수수료 = fee_rate * 매수금액
            actual_price = price * (1 + slip)
            cost = cash  # 전부 코인 매수
            fee = cost * fee_rate
            cost_after_fee = cost - fee
            if actual_price > 0:
                coin_amount = cost_after_fee / actual_price
            cash = 0.0

        elif trade_now == -1:
            # 매도 (1->0)
            # 실제 체결가 = price * (1 - slip)
            # 수수료 = fee_rate * 매도금액
            actual_price = price * (1 - slip)
            proceeds = coin_amount * actual_price
            fee = proceeds * fee_rate
            cash = proceeds - fee
            coin_amount = 0.0

        # (B) 보유기간이면 coin_amount만큼 평가
        cur_equity = cash + coin_amount * price
        equity_curve.iloc[i] = cur_equity
        last_equity = cur_equity

    final_equity = last_equity
    total_return = (final_equity - start_equity) / start_equity

    # === 3) 성과 지표 계산 (sharpe, MDD 등) ===
    # 일간 로그수익률(=ln(equity_t / equity_{t-1})) 후 샤프 계산
    equity_curve_shift = equity_curve.shift(1).fillna(method="bfill")
    ret_series = np.log(equity_curve / equity_curve_shift).replace([np.inf, -np.inf], 0).fillna(0)

    # 연율화 샤프 지수
    # assume 365 bar/yr if daily, or 252, or depends. 여기선 daily=365
    bar_per_year = 365
    excess_ret = ret_series - (riskfree_rate / bar_per_year)
    mean_excess = excess_ret.mean() * bar_per_year
    std_excess = excess_ret.std() * np.sqrt(bar_per_year)
    sharpe = mean_excess / (std_excess+1e-9) if std_excess>0 else 0.0

    # MDD
    roll_max = equity_curve.cummax()
    dd_series= (equity_curve - roll_max)/roll_max
    max_drawdown = dd_series.min()

    results = {
        "final_equity": final_equity,
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "equity_curve": equity_curve,
        "ret_series": ret_series,
    }
    return results
