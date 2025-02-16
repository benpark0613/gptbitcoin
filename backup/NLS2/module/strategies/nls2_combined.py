# module/strategies/nls2_combined.py

import backtrader as bt

class BBWidth(bt.Indicator):
    """
    논문에서 사용하는 Bollinger Band Width: (Top - Bot) / Mid
    VLI(Volatility-Level-Indicator) 계산에 쓰임.
    """
    lines = ('bbw',)
    params = (('period', 20), ('devfactor', 2.0),)

    def __init__(self):
        bb = bt.indicators.BollingerBands(
            self.data,
            period=self.p.period,
            devfactor=self.p.devfactor
        )
        self.lines.bbw = (bb.top - bb.bot) / bb.mid


class VLI(bt.Indicator):
    """
    Volatility-Level-Indicator(VLI):
      - vli_fast = SMA(BBWidth, period=fast)
      - vli_slow = SMA(BBWidth, period=slow)
      - vli_top  = vli_slow + 2 * StdDev(BBWidth, period=slow)
    """
    lines = ('vli_fast', 'vli_slow', 'vli_top',)
    params = (('fast', 200), ('slow', 1000),)

    def __init__(self):
        self.vli_fast_ma = bt.indicators.SMA(self.data, period=self.p.fast)
        self.vli_slow_ma = bt.indicators.SMA(self.data, period=self.p.slow)
        self.stdev_slow = bt.indicators.StdDev(self.data, period=self.p.slow)

    def next(self):
        self.lines.vli_fast[0] = self.vli_fast_ma[0]
        self.lines.vli_slow[0] = self.vli_slow_ma[0]
        self.lines.vli_top[0] = self.vli_slow_ma[0] + 2.0 * self.stdev_slow[0]


class NLS2Combined(bt.Strategy):
    """
    논문의 'New Long'과 'New Short' 전략을 모두 합친 종합 전략.
    - 롱/숏 각각 진입 조건과 청산 조건을 독립적으로 판단
    - 한 번에 하나의 포지션만 유지(기본 가정)
      (동시에 롱/숏 포지션을 잡으려면 거래소/브로커가 헤징 모드를 지원해야 하며,
       여기서는 논문 구조에 충실해 '단방향'만 가정)
    ----------------------------------------------------------------------------
    [LONG 로직] (논문 NLS2Long 요약)
      - 진입: Bollinger 상단 CrossDown + 거래량 증가 + 현재가> SMA fast + BBW < vli_top 등
              저/고 변동성에 따라 sma_mid>veryslow / sma_slow>veryslow
      - 청산: Bollinger 하단 CrossDown + 거래량 증가
      - 손절: 진입 시점 캔들 low * (1 - L_slippage_factor)
      - 다단계 익절(StopWin):
         3% → 1%,  20% → 15%, 25% → 20%, 30% → 25%, ...
    ----------------------------------------------------------------------------
    [SHORT 로직] (논문 NLS2Short 요약)
      - 진입: Bollinger 하단 CrossUp + 거래량 증가 + BBW < vli_top 등
              저/고 변동성 구분하여 sma_mid<veryslow / sma_slow<veryslow
      - 청산: Bollinger 상단 CrossUp + 거래량 증가
      - 손절: 진입 시점 캔들 high * (1 + S_slippage_factor)
      - 다단계 익절(StopWin):
         3% → 1%,  10% → 5%, 15% → 10%, 20% → 15%, ...
    ----------------------------------------------------------------------------
    파라미터는 롱(L_) / 숏(S_) 접두로 구분:
      - L_slippage_factor, L_profit_steps, L_minor_trigger, L_minor_stop ...
      - S_slippage_factor, S_profit_steps, S_minor_trigger, S_minor_stop ...
    """

    params = dict(
        # 공통 Bollinger+VLI
        bb_period=20,
        bb_dev=2.0,
        vol_fast=10,
        vol_slow=50,
        sma_fast=20,
        sma_mid=50,
        sma_slow=100,
        sma_veryslow=200,
        vli_fast=200,
        vli_slow=1000,

        # LONG 관련
        L_slippage_factor=0.05,  # 진입 캔들 low -5%
        L_minor_trigger=0.03,    # 3% 수익시 StopWin=1% 설정
        L_minor_stop=0.01,
        L_profit_steps=[         # (수익률, stopwin)
            (0.20, 0.15),
            (0.25, 0.20),
            (0.30, 0.25),
            (0.35, 0.30),
            (0.40, 0.35),
        ],

        # SHORT 관련
        S_slippage_factor=0.05,  # 진입 캔들 high +5%
        S_minor_trigger=0.03,    # 3% 수익시 StopWin=1%
        S_minor_stop=0.01,
        S_profit_steps=[
            (0.10, 0.05),
            (0.15, 0.10),
            (0.20, 0.15),
            (0.25, 0.20),
            (0.30, 0.25),
        ],
    )

    def __init__(self):
        # Bollinger
        self.bb = bt.indicators.BollingerBands(
            self.data.close,
            period=self.p.bb_period,
            devfactor=self.p.bb_dev
        )
        # BBWidth
        self.bbw = BBWidth(
            self.data.close,
            period=self.p.bb_period,
            devfactor=self.p.bb_dev
        )
        # VLI
        self.vli = VLI(
            self.bbw,
            fast=self.p.vli_fast,
            slow=self.p.vli_slow
        )

        # 거래량 SMA
        self.vol_fast_sma = bt.indicators.SMA(self.data.volume, period=self.p.vol_fast)
        self.vol_slow_sma = bt.indicators.SMA(self.data.volume, period=self.p.vol_slow)

        # 이동평균
        self.sma_fast = bt.indicators.SMA(self.data.close, period=self.p.sma_fast)
        self.sma_mid = bt.indicators.SMA(self.data.close, period=self.p.sma_mid)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=self.p.sma_slow)
        self.sma_veryslow = bt.indicators.SMA(self.data.close, period=self.p.sma_veryslow)

        # LONG signals
        self.cross_top_down = bt.indicators.CrossDown(self.data.close, self.bb.top)
        self.cross_bot_down = bt.indicators.CrossDown(self.data.close, self.bb.bot)
        # SHORT signals
        self.cross_bot_up = bt.indicators.CrossUp(self.data.close, self.bb.bot)
        self.cross_top_up = bt.indicators.CrossUp(self.data.close, self.bb.top)

        # 주문/포지션 추적
        self.order = None
        self.stop_order = None
        self.entry_price = None
        self.entry_low = None
        self.entry_high = None

        # 현재 포지션이 롱인지 숏인지 구분
        self.is_long_position = False
        self.is_short_position = False

    def vol_condition(self):
        return self.vol_fast_sma[0] > self.vol_slow_sma[0]

    def is_extreme_vol(self):
        return self.bbw.bbw[0] > self.vli.vli_top[0]

    def is_low_vol(self):
        return self.vli.vli_fast[0] < self.vli.vli_slow[0]

    ####################
    #   LONG Logic
    ####################
    def check_long_entry(self):
        """
        논문 'New Long' 진입:
          - Bollinger 상단 CrossDown
          - 거래량 증가
          - close > sma_fast
          - not extreme vol => low vol이면 sma_mid>veryslow, high vol이면 sma_slow>veryslow
          - else => sma_slow>veryslow
        """
        if self.cross_top_down[0] == 1 and self.vol_condition():
            if self.data.close[0] > self.sma_fast[0]:
                if self.bbw.bbw[0] < self.vli.vli_top[0]:
                    if self.is_low_vol():
                        if self.sma_mid[0] > self.sma_veryslow[0]:
                            return True
                    else:
                        if self.sma_slow[0] > self.sma_veryslow[0]:
                            return True
                else:
                    # extreme vol 시에도 sma_slow>veryslow 시 진입
                    if self.sma_slow[0] > self.sma_veryslow[0]:
                        return True
        return False

    def check_long_exit(self):
        """
        논문 'New Long' 청산: Bollinger 하단 CrossDown + 거래량 증가
        """
        if self.cross_bot_down[0] == 1 and self.vol_condition():
            return True
        return False

    def long_stop_price(self):
        """
        진입 캔들의 최저가에서 slippage_factor% 아래
        """
        return self.entry_low * (1.0 - self.p.L_slippage_factor)

    def long_update_stopwin(self):
        """
        다단계 익절 로직(논문 예시):
          3% → 1%, 20% → 15%, 25% → 20%, 30% → 25%, 35% → 30%, 40% → 35% ...
        """
        if not self.position or not self.is_long_position or not self.stop_order:
            return

        current_price = self.data.close[0]
        pnl_pct = (current_price - self.entry_price) / self.entry_price

        best_stop_pct = None

        # 3% 이익 → 1%
        if pnl_pct >= self.p.L_minor_trigger:
            best_stop_pct = self.p.L_minor_stop

        for (thr, sw) in self.p.L_profit_steps:
            if pnl_pct >= thr:
                best_stop_pct = sw

        if best_stop_pct is not None:
            new_stop_price = self.entry_price * (1.0 + best_stop_pct)
            # 기존 stop_order price
            old_stop_price = None
            if hasattr(self.stop_order, 'created'):
                old_stop_price = getattr(self.stop_order.created, 'price', None)

            # 롱 포지션: stop_price가 더 높을수록 수익 보호
            if old_stop_price is None or new_stop_price > old_stop_price:
                self.cancel(self.stop_order)
                self.stop_order = self.sell(
                    exectype=bt.Order.Stop,
                    price=new_stop_price
                )

    ####################
    #   SHORT Logic
    ####################
    def check_short_entry(self):
        """
        논문 'New Short' 진입:
          - Bollinger 하단 CrossUp
          - 거래량 증가
          - not extreme vol => low vol이면 sma_mid<sma_veryslow, high vol이면 sma_slow<sma_veryslow
        """
        if self.cross_bot_up[0] == 1 and self.vol_condition():
            if not self.is_extreme_vol():
                if self.is_low_vol():
                    if self.sma_mid[0] < self.sma_veryslow[0]:
                        return True
                else:
                    if self.sma_slow[0] < self.sma_veryslow[0]:
                        return True
        return False

    def check_short_exit(self):
        """
        논문 'New Short' 청산:
          - Bollinger 상단 CrossUp + 거래량 증가
        """
        if self.cross_top_up[0] == 1 and self.vol_condition():
            return True
        return False

    def short_stop_price(self):
        """
        진입 캔들의 최고가 * (1 + S_slippage_factor)
        """
        return self.entry_high * (1.0 + self.p.S_slippage_factor)

    def short_update_stopwin(self):
        """
        논문 'New Short' 다단계 수익률:
          3% → 1%, 10% → 5%, 15% → 10%, 20% → 15% ...
        """
        if not self.position or not self.is_short_position or not self.stop_order:
            return

        current_price = self.data.close[0]
        pnl_pct = (self.entry_price - current_price) / self.entry_price  # 숏 수익률

        best_stop_pct = None

        # 3% → 1%
        if pnl_pct >= self.p.S_minor_trigger:
            best_stop_pct = self.p.S_minor_stop

        for (thr, sw) in self.p.S_profit_steps:
            if pnl_pct >= thr:
                best_stop_pct = sw

        if best_stop_pct is not None:
            new_stop_price = self.entry_price * (1.0 - best_stop_pct)
            old_stop_price = None
            if hasattr(self.stop_order, 'created'):
                old_stop_price = getattr(self.stop_order.created, 'price', None)

            # 숏 포지션: buy_stop 가격이 더 높을수록 손실 제한 감소(수익 보호)
            if old_stop_price is None or new_stop_price > old_stop_price:
                self.cancel(self.stop_order)
                self.stop_order = self.buy(
                    exectype=bt.Order.Stop,
                    price=new_stop_price
                )

    ####################
    #   Backtrader Hooks
    ####################
    def notify_order(self, order):
        if order.status in [order.Completed]:
            self.order = None
            if order.isbuy():
                # Buy 체결 == Short 포지션이 손절 or 청산을 위한 주문이 체결됐을 수도 있음
                # 진입 시점이면 self.is_short_position = True (아래)
                # 하지만 Backtrader에선 buy()가 단순 '숏 포지션 진입'이 아니라 '롱/숏 청산'이 될 수도 있으므로
                # 아래 next()에서 상황을 다시 확인.
                pass

            elif order.issell():
                # Sell 체결 == 롱 포지션 진입 or 청산
                pass

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if self.order:
            return

        # 현재 보유 포지션 확인
        pos_size = self.position.size

        # 포지션이 없으면 => 롱 혹은 숏 진입
        if pos_size == 0:
            self.is_long_position = False
            self.is_short_position = False

            if self.check_long_entry():
                self.is_long_position = True
                self.entry_price = self.data.close[0]
                self.entry_low = self.data.low[0]
                self.order = self.buy()
            elif self.check_short_entry():
                self.is_short_position = True
                self.entry_price = self.data.close[0]
                self.entry_high = self.data.high[0]
                self.order = self.sell()

        # 롱 포지션
        elif pos_size > 0:
            if self.check_long_exit():
                self.order = self.close()  # 전량청산
            else:
                self.long_update_stopwin()

        # 숏 포지션
        elif pos_size < 0:
            if self.check_short_exit():
                self.order = self.close()
            else:
                self.short_update_stopwin()

    def notify_trade(self, trade):
        """
        체결 후 손절 주문 설정
         - 롱 체결 시 -> low*(1 - factor)
         - 숏 체결 시 -> high*(1 + factor)
        """
        if trade.isopen:
            return

        if trade.isclosed:
            # 포지션이 청산되면 stop_order 등 리셋
            self.stop_order = None
            return

        # Backtrader에서 isopen / isclosed가 시점에 따라 다르게 호출되므로
        # 실제 체결 콜백은 notify_order에서 하는 편이 더 확실함.
        # 여기서는 예시로 적음.
        if trade.justopened:
            if trade.size > 0:  # 롱
                sl_price = self.long_stop_price()
                if sl_price < self.data.close[0]:
                    self.stop_order = self.sell(
                        exectype=bt.Order.Stop,
                        price=sl_price
                    )
            elif trade.size < 0:  # 숏
                sl_price = self.short_stop_price()
                if sl_price > self.data.close[0]:
                    self.stop_order = self.buy(
                        exectype=bt.Order.Stop,
                        price=sl_price
                    )
