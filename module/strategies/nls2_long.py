# module/strategies/nls2_long.py

import backtrader as bt

class BBWidth(bt.Indicator):
    """
    Bollinger Band Width: (Top - Bot) / Mid
    논문의 VLI(Volatility-Level-Indicator) 계산을 위해 사용.
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
    Volatility-Level-Indicator(VLI)
      - vli_fast = SMA(BBW, period=fast)
      - vli_slow = SMA(BBW, period=slow)
      - vli_top  = vli_slow + 2 * StdDev(BBW, period=slow)
    논문에서 정의된 변동성 레벨(저/고/극단)을 구분하기 위해 사용.
    """
    lines = ('vli_fast', 'vli_slow', 'vli_top',)
    params = (('fast', 200), ('slow', 1000),)

    def __init__(self):
        # BBWidth를 입력으로 받음
        self.vli_fast_ma = bt.indicators.SMA(self.data, period=self.p.fast)
        self.vli_slow_ma = bt.indicators.SMA(self.data, period=self.p.slow)
        self.stdev_slow = bt.indicators.StdDev(self.data, period=self.p.slow)

    def next(self):
        self.lines.vli_fast[0] = self.vli_fast_ma[0]
        self.lines.vli_slow[0] = self.vli_slow_ma[0]
        self.lines.vli_top[0] = self.vli_slow_ma[0] + 2.0 * self.stdev_slow[0]


class NLS2Long(bt.Strategy):
    """
    논문에 제시된 'New Long' (최종 롱 전략)을 최대한 충실히 반영.
    ---------------------------------------------------------
    1) 진입조건 (CrossDown Bollinger 상단 + 거래량 증가 + 추가 확인):
       - BollingerBands(period=bb_period, devfactor=bb_dev)
         상단선을 가격이 위->아래로 교차(CrossDown)
       - fast/slow 거래량 SMA(vol_fast_sma > vol_slow_sma)
       - 현재가가 sma_fast 위
       - BBWidth < vli_top 인 경우 extreme volatility 아님
         * low vol (vli_fast < vli_slow) 시 => sma_mid > sma_veryslow
         * high vol 시 => long(또는 sma_slow > sma_veryslow 검사)
       - 그 외 else 조건 (논문 예시): sma_slow > sma_veryslow 시 롱
    2) 청산조건 (CrossDown Bollinger 하단 + 거래량 증가)
    3) 손절: (논문 예) "진입 캔들의 low - 5%" 가격에 Stop 주문
    4) 익절/트레일링 로직 (논문 예) 다단계:
       - 3% 초과 시, stopwin=1%
       - 20% 초과 시 stopwin=15%, 25% 초과 => 20%, 30% => 25%, 35% => 30%, 40% => 35%
       => 수익률이 높아질수록 stopwin 상향
    ---------------------------------------------------------
    파라미터:
      - bb_period, bb_dev: 볼린저 밴드 계산
      - vol_fast, vol_slow: 거래량 SMA
      - sma_fast, sma_mid, sma_slow, sma_veryslow: 가격 이동평균
      - vli_fast, vli_slow: VLI 계산
      - slippage_factor: 손절가(Entry candle의 low * (1 - slippage_factor))
      - profit_steps: [ (이익률, 스탑윈) ... ] 형태로 여러 단계
      - minor_stop (3%/1%)처럼 작은 트레일 스탑
    """

    params = dict(
        bb_period=20,        # Bollinger period
        bb_dev=2.0,          # Bollinger devfactor
        vol_fast=10,         # 거래량 fast SMA
        vol_slow=50,         # 거래량 slow SMA
        sma_fast=20,         # 가격 SMA fast
        sma_mid=50,
        sma_slow=100,
        sma_veryslow=200,

        vli_fast=200,        # BBWidth의 SMA fast
        vli_slow=1000,       # BBWidth의 SMA slow

        slippage_factor=0.05,   # 진입 캔들의 최저가에서 -5%
        minor_trigger=0.03,     # 3% 수익시 미니멈 stopwin=1%
        minor_stop=0.01,        # 1%

        # 논문에 언급된 다단계 수익률-스탑윈 목록(상승여력)
        profit_steps=[
            (0.20, 0.15),
            (0.25, 0.20),
            (0.30, 0.25),
            (0.35, 0.30),
            (0.40, 0.35),
        ],
    )

    def __init__(self):
        # BollingerBands
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

        # VLI(Volatility Level Indicator)
        self.vli = VLI(
            self.bbw,
            fast=self.p.vli_fast,
            slow=self.p.vli_slow
        )

        # 거래량 SMA
        self.vol_fast_sma = bt.indicators.SMA(self.data.volume, period=self.p.vol_fast)
        self.vol_slow_sma = bt.indicators.SMA(self.data.volume, period=self.p.vol_slow)

        # 가격 이동평균
        self.sma_fast = bt.indicators.SMA(self.data.close, period=self.p.sma_fast)
        self.sma_mid = bt.indicators.SMA(self.data.close, period=self.p.sma_mid)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=self.p.sma_slow)
        self.sma_veryslow = bt.indicators.SMA(self.data.close, period=self.p.sma_veryslow)

        # Bollinger 상단 교차(down) -> 진입시그널
        self.cross_top_down = bt.indicators.CrossDown(self.data.close, self.bb.top)
        # Bollinger 하단 교차(down) -> 청산시그널
        self.cross_bot_down = bt.indicators.CrossDown(self.data.close, self.bb.bot)

        self.order = None       # 최신 주문 추적
        self.stop_order = None  # 손절/익절 겸용 stop
        self.entry_price = None
        self.entry_low = None   # 진입 당시 캔들의 저가

    def vol_condition(self):
        """거래량 증가."""
        return self.vol_fast_sma[0] > self.vol_slow_sma[0]

    def is_low_vol(self):
        """VLI fast < VLI slow => 저변동성."""
        return self.vli.vli_fast[0] < self.vli.vli_slow[0]

    def check_entry_signal(self):
        """
        논문(최종 New Long) 진입 조건:
         1) Bollinger 상단 CrossDown
         2) 거래량 증가(vol_condition)
         3) 현재가 > sma_fast
         4) BBW < VLI_top (극단적 변동성 X)
            - if 저변동성 => sma_mid > sma_veryslow
            - else(고변동성) => 그냥 진입 or sma_slow > sma_veryslow
         5) 그 외 else if => sma_slow > sma_veryslow
        """
        # (1) + (2)
        if self.cross_top_down[0] == 1 and self.vol_condition():
            # (3)
            if self.data.close[0] > self.sma_fast[0]:
                # (4) extreme volatility 피하기
                if self.bbw.bbw[0] < self.vli.vli_top[0]:
                    # 저변동성
                    if self.is_low_vol():
                        if self.sma_mid[0] > self.sma_veryslow[0]:
                            return True
                    else:
                        # 고변동성 -> (논문 예시) 그냥 롱 or sma_slow>veryslow
                        if self.sma_slow[0] > self.sma_veryslow[0]:
                            return True
                else:
                    # vli_top 초과 시 => if sma_slow>veryslow => 롱
                    if self.sma_slow[0] > self.sma_veryslow[0]:
                        return True
        return False

    def check_exit_signal(self):
        """
        논문: Bollinger 하단 CrossDown + 거래량 증가 => 청산.
        """
        if self.cross_bot_down[0] == 1 and self.vol_condition():
            return True
        return False

    def notify_order(self, order):
        """
        주문 체결 상태.
        - 매수 체결 후 => 손절(5% below low of entry candle) 지정
        - stopwin은 별도 로직(next)에 따라 업데이트
        """
        if order.status in [order.Completed]:
            self.order = None
            if order.isbuy():
                # 진입 체결: 손절 주문
                if self.entry_low is not None:
                    sl_price = self.entry_low * (1.0 - self.p.slippage_factor)
                    # 현재가보다 유효하게 낮은지 확인
                    if sl_price < self.data.close[0]:
                        # Stop 주문(롱 포지션 → sell)
                        self.stop_order = self.sell(
                            exectype=bt.Order.Stop,
                            price=sl_price
                        )
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def update_stopwin(self):
        """
        다단계 익절/트레일링 스탑 로직.
        (논문: 3%, 20%, 25%, 30%, 35%, 40% 구간별 stopwin 상향)
        """
        if not self.position or not self.stop_order:
            return

        # 현재 수익률
        current_price = self.data.close[0]
        pnl_pct = (current_price - self.entry_price) / self.entry_price

        # 3% 이상 시 -> stopwin=1%
        # 이후 20% ~ 40% 까지 5% 단위로 stopwin 상향
        # ex) 20% 수익 → stopwin=15%, 25% 수익 → stopwin=20%, ...
        best_stopwin_pct = None

        if pnl_pct >= self.p.minor_trigger:
            best_stopwin_pct = self.p.minor_stop  # 1%

        for (thr, sw) in self.p.profit_steps:
            if pnl_pct >= thr:
                best_stopwin_pct = sw  # ex) 0.15 for 20%, 0.20 for 25% 등

        if best_stopwin_pct is not None:
            # 새 stop 가격 = entry_price * (1 + best_stopwin_pct)
            new_stop_price = self.entry_price * (1.0 + best_stopwin_pct)
            # 현재 stop_order보다 더 높은가? => Update
            # (기존 stop_order 정보를 가져오기 위해서는 backtrader 문법상:
            #   stop_order.created.price 등을 참조)
            old_stop_price = None
            if hasattr(self.stop_order, 'created'):
                old_stop_price = getattr(self.stop_order.created, 'price', None)

            # 기존 stop_price보다 더 높은 stop_price만 업데이트
            if old_stop_price is None or (new_stop_price > old_stop_price):
                # 기존 stop_order 취소 후 새 stop 발행
                self.cancel(self.stop_order)
                self.stop_order = self.sell(exectype=bt.Order.Stop, price=new_stop_price)

    def next(self):
        if self.order:
            return  # 아직 주문이 체결 안 되었으면 대기

        # 포지션이 없는 경우 → 진입 검사
        if not self.position:
            if self.check_entry_signal():
                self.entry_price = self.data.close[0]
                self.entry_low = self.data.low[0]
                self.order = self.buy()
        else:
            # 포지션 보유 중 → 청산 검사
            if self.check_exit_signal():
                self.order = self.close()  # 전량 청산
            else:
                # 매 초마다 stopwin 갱신
                self.update_stopwin()
