# module/strategies/buy_and_hold.py

import backtrader as bt

class BuyAndHoldStrategy(bt.Strategy):
    """
    단순 Buy & Hold 전략
     - 첫 번째 캔들에서 전량 매수 (시작 시점에 바로 매수)
     - 이후 전혀 청산하지 않음
     - 수동으로 중간에 더 매수/매도 로직도 없음
    """
    def __init__(self):
        self.has_bought = False  # 한 번만 매수할 플래그

    def next(self):
        # 아직 매수 안 했으면 즉시 매수
        if not self.has_bought:
            # 전 재산으로 매수: 1회성
            self.buy()
            self.has_bought = True
        # 이후 아무 것도 안 함(홀딩)
