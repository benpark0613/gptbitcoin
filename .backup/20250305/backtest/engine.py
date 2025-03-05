# gptbitcoin/backtest/engine.py
# - Backtrader 기반 백테스트 엔진
# - 주석은 필요한 최소한만 한글, docstring은 구글 스타일
# - 여기서는 최종 계산(Sharpe, MDD 등)은 하지 않고,
#   에쿼티 시계열(equity_curve), 체결 로그(trade_logs) 등 원시 결과만 반환한다.

import csv
import os
from typing import Dict, Any, Optional

import backtrader as bt

# config.py에서 세팅값 가져온다고 가정
try:
    from config.config import (
        ALLOW_SHORT,
        COMMISSION_RATE,
        SLIPPAGE_RATE,
        START_CAPITAL,
        LEVERAGE,
    )
except ImportError:
    # 기본값 설정(데모)
    ALLOW_SHORT = True
    COMMISSION_RATE = 0.0004
    SLIPPAGE_RATE = 0.0002
    START_CAPITAL = 1_000_000
    LEVERAGE = 1


class PandasSignalData(bt.feeds.PandasData):
    """
    Backtrader의 기본 PandasData를 확장.
    'signal' 칼럼을 custom 라인으로 정의.
    """
    lines = ('signal',)
    params = (
        ('signal', -1),  # -1이면 DataFrame 자동 매핑 시도
    )


class StrategySignal(bt.Strategy):
    """
    DataFrame 내 'signal' 값을 이용해 매매:
      - signal > 0 => 롱(매수)
      - signal < 0 => 숏(매도), ALLOW_SHORT=True일 때만
      - signal = 0 => 포지션 정리(청산)
    """

    def __init__(self):
        self.current_order = None

    def log(self, txt):
        """간단 로그"""
        dt = self.datas[0].datetime.datetime(0)
        print(f"[{dt}] {txt}")

    def notify_order(self, order):
        """주문 상태 체크"""
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"BUY 체결: 수량={order.executed.size} 가격={order.executed.price}")
            else:
                self.log(f"SELL 체결: 수량={order.executed.size} 가격={order.executed.price}")
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("주문 취소/마진부족/거부")
        self.current_order = None

    def notify_trade(self, trade):
        """트레이드 완료 시점에 PnL 등을 로그"""
        if not trade.isclosed:
            return
        self.log(f"(거래 완료) Gross P/L: {trade.pnl}, Net P/L: {trade.pnlcomm}")

    def next(self):
        """매 시점마다 시그널 읽어 포지션 설정"""
        if self.current_order:
            return

        sig = self.datas[0].signal[0]
        pos_size = self.getposition().size

        # 포지션 청산 함수
        def close_position():
            if pos_size > 0:
                self.current_order = self.sell(size=abs(pos_size))
            elif pos_size < 0:
                self.current_order = self.buy(size=abs(pos_size))

        if sig > 0:
            # 롱 시그널
            if pos_size <= 0:
                if pos_size < 0:
                    close_position()
                # (동일 봉에서 청산+진입 동시처리 예시)
                self.current_order = self.buy()
        elif sig < 0 and ALLOW_SHORT:
            # 숏 시그널
            if pos_size >= 0:
                if pos_size > 0:
                    close_position()
                self.current_order = self.sell()
        else:
            # sig == 0 or 숏 불허
            if pos_size != 0:
                close_position()


class EquityCurveAnalyzer(bt.Analyzer):
    """
    매 시점(캔들)에서 broker.getvalue()를 기록하여 에쿼티 시계열을 저장.
    """

    def __init__(self):
        self.equity_curve = []

    def next(self):
        cur_value = self.strategy.broker.getvalue()
        self.equity_curve.append(cur_value)

    def get_analysis(self):
        return {
            'equity_curve': self.equity_curve
        }


class TradeLogger(bt.Analyzer):
    """
    거래(트레이드) 정보를 저장. notify_trade를 활용.
    - trade가 완전히 종료될 때 PnL을 기록
    - 부분 매도/부분 청산 등은 trade가 나눠질 수 있으니 유의
    """

    def __init__(self):
        self.trade_logs = []

    def notify_trade(self, trade):
        if trade.isclosed:
            # trade가 완전히 종료된 시점
            dt = self.strategy.datas[0].datetime.datetime(0)
            rec = {
                "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "pnl": trade.pnl,
                "pnlcomm": trade.pnlcomm,
                "size": trade.size
            }
            self.trade_logs.append(rec)

    def get_analysis(self):
        return {
            'trade_logs': self.trade_logs
        }


def run_backtest(
        df,
        result_csv_path: Optional[str] = None,
        strategy_name: str = "StrategySignal",
        start_cash: float = START_CAPITAL
) -> Dict[str, Any]:
    """
    Backtrader로 백테스트를 실행. 나머지 성과 지표(MDD, Sharpe 등)는 scoring.py에서 계산.

    Args:
        df (pd.DataFrame): 컬럼에 ["open","high","low","close","volume","signal"] 포함
        result_csv_path (str, optional): 거래체결 로그 CSV 경로
        strategy_name (str, optional): 사용할 전략(여기서는 StrategySignal)
        start_cash (float, optional): 초기 자본금

    Returns:
        Dict[str, Any]:
          {
            "start_cap": float,
            "final_cap": float,
            "pnl": float,
            "equity_curve": List[float],
            "trade_logs": List[dict],
          }
    """
    required_cols = ["open", "high", "low", "close", "volume", "signal"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"run_backtest(): df에 '{c}' 칼럼이 없습니다.")

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(start_cash)

    # 슬리피지, 커미션
    cerebro.broker.setcommission(commission=COMMISSION_RATE)
    cerebro.broker.set_slippage_perc(SLIPPAGE_RATE)

    data_feed = PandasSignalData(dataname=df)
    cerebro.adddata(data_feed)

    # 전략
    if strategy_name == "StrategySignal":
        cerebro.addstrategy(StrategySignal)
    else:
        cerebro.addstrategy(StrategySignal)  # fallback

    # Analyzer 추가(에쿼티 시계열, 트레이드 로그)
    cerebro.addanalyzer(EquityCurveAnalyzer, _name="equity")
    cerebro.addanalyzer(TradeLogger, _name="trades")

    # 실행
    results = cerebro.run()
    strat = results[0]  # 단일 전략

    final_cap = cerebro.broker.getvalue()
    pnl = final_cap - start_cash

    # analyzer 결과
    eq_curve = strat.analyzers.equity.get_analysis()["equity_curve"]
    trade_logs = strat.analyzers.trades.get_analysis()["trade_logs"]

    # 옵션으로 trade_logs CSV 저장
    if result_csv_path:
        os.makedirs(os.path.dirname(result_csv_path), exist_ok=True)
        with open(result_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["datetime", "pnl", "pnlcomm", "size"])
            writer.writeheader()
            for t in trade_logs:
                writer.writerow(t)

    return {
        "start_cap": start_cash,
        "final_cap": final_cap,
        "pnl": pnl,
        "equity_curve": eq_curve,
        "trade_logs": trade_logs,
    }


if __name__ == "__main__":
    """
    간단 테스트 예시:
    """
    import pandas as pd

    # 샘플 데이터: 5개 봉
    data = {
        "open": [100, 102, 101, 105, 110],
        "high": [101, 103, 103, 108, 112],
        "low": [99, 100, 100, 104, 108],
        "close": [100, 101, 102, 107, 111],
        "volume": [10, 12, 9, 15, 14],
        # 시그널>0 => 매수, <0 => 매도(숏 허용), 0 => 포지션 청산
        "signal": [0, 1, 0, -1, 1],
    }
    df_test = pd.DataFrame(data)

    result = run_backtest(df_test, result_csv_path=None, strategy_name="StrategySignal", start_cash=100000)
    print("=== 백테스트 결과 ===")
    print("초기 자본:", result["start_cap"])
    print("최종 자본:", result["final_cap"])
    print("PnL:", result["pnl"])
    print("에쿼티 시계열:", result["equity_curve"])
    print("체결 로그:", result["trade_logs"])
