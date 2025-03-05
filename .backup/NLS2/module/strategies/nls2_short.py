# module/strategies/nls2_short.py

import backtrader as bt

class BBWidth(bt.Indicator):
    """
    논문에서 VLI(Volatility-Level-Indicator) 계산에 사용되는
    Bollinger Band Width: (Top - Bot) / Mid
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
      - vli_fast = SMA(BBWidth, period=fast)
      - vli_slow = SMA(BBWidth, period=slow)
      - vli_top  = vli_slow + 2 * StdDev(BBWidth, period=slow)
    논문에 따라 저/고/극단 변동성 구간을 구분하기 위해 사용.
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


class NLS2Short(bt.Strategy):
    """
    논문에 제시된 'New Short' 전략(최종 숏 로직)을 최대한 충실히 구현.

    ----------------------------------------------------------------
    [ 숏 진입 조건 (새로운 Short) - 논문 내용 요약 ]
      1) Bollinger Band 하단선 CrossUp (가격이 Bot 아래->위로 돌파)
      2) 거래량 증가 (fast/slow SMA 비교)
      3) 극단적 변동성(BBW > vli_top)은 피하기
         - low vol (vli_fast < vli_slow) → sma_mid < sma_veryslow (하락장)
         - high vol → sma_slow < sma_veryslow (좀 더 강한 하락 추세)
         * (논문 예시에서는 구체적으로 low vol / high vol에서 다른 필터 사용)
      4) 그 외 else-if 로 나머지 조건 설정 가능
    ----------------------------------------------------------------
    [ 청산 조건 ]
      - Bollinger Band 상단선 CrossUp + 거래량 증가

    ----------------------------------------------------------------
    [ 손절 로직 ]
      - 논문 예시: '진입 캔들의 high + 일정 퍼센트'로 Stop 설정
        예) high * (1 + slippage_factor)

    ----------------------------------------------------------------
    [ 익절/트레일링 스탑 로직 (단계별 수익률) ]
      - 3% 초과 시 => stopwin=1% (소폭 익절 보장)
      - 이후 (10%→5%), (15%→10%), (20%→15%), (25%→20%), (30%→25%) ...
        등 여러 구간별로 숏에 맞게 stopwin 상향.
      - 숏 포지션의 stopwin은 '매수가보다 위'에 buy stop 주문 형태.

    ----------------------------------------------------------------
    파라미터:
      - bb_period, bb_dev: 볼린저 밴드 계산
      - vol_fast, vol_slow: 거래량 SMA
      - sma_fast, sma_mid, sma_slow, sma_veryslow: 가격 이동평균 (필요하면 추가 사용)
      - vli_fast, vli_slow: VLI 계산
      - slippage_factor: 손절가(진입 캔들 high * (1 + slippage_factor))
      - minor_trigger, minor_stop: (3%, 1%)와 같이 '소규모 트레일 스탑'
      - profit_steps: 리스트( (수익률임계치, stopwin) )로 단계별 설정
    """

    params = dict(
        # Bollinger
        bb_period=20,
        bb_dev=2.0,

        # 거래량
        vol_fast=10,
        vol_slow=50,

        # 가격 이동평균
        sma_fast=20,
        sma_mid=50,
        sma_slow=100,
        sma_veryslow=200,

        # VLI 파라미터
        vli_fast=200,
        vli_slow=1000,

        # 손절
        slippage_factor=0.05,   # 진입 캔들 high + 5%
        # 소규모 트레일링: 3% 이득이면 1%로 stopwin
        minor_trigger=0.03,
        minor_stop=0.01,

        # 주 단계별 수익률
        # (10% 수익 → stopwin 5%), (15%→10%), (20%→15%), ...
        profit_steps=[
            (0.10, 0.05),
            (0.15, 0.10),
            (0.20, 0.15),
            (0.25, 0.20),
            (0.30, 0.25),
        ],
    )

    def __init__(self):
        # Bollinger + BBWidth + VLI
        self.bb = bt.indicators.BollingerBands(
            self.data.close,
            period=self.p.bb_period,
            devfactor=self.p.bb_dev
        )
        self.bbw = BBWidth(
            self.data.close,
            period=self.p.bb_period,
            devfactor=self.p.bb_dev
        )
        self.vli = VLI(
            self.bbw,
            fast=self.p.vli_fast,
            slow=self.p.vli_slow
        )

        # 거래량 SMA
        self.vol_fast_sma = bt.indicators.SMA(self.data.volume, period=self.p.vol_fast)
        self.vol_slow_sma = bt.indicators.SMA(self.data.volume, period=self.p.vol_slow)

        # 가격 이동평균(하락 추세 판단용)
        self.sma_fast = bt.indicators.SMA(self.data.close, period=self.p.sma_fast)
        self.sma_mid = bt.indicators.SMA(self.data.close, period=self.p.sma_mid)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=self.p.sma_slow)
        self.sma_veryslow = bt.indicators.SMA(self.data.close, period=self.p.sma_veryslow)

        # Bollinger band cross: 하단선 CrossUp => 숏 진입
        self.cross_bot_up = bt.indicators.CrossUp(self.data.close, self.bb.bot)
        # 상단선 CrossUp => 청산
        self.cross_top_up = bt.indicators.CrossUp(self.data.close, self.bb.top)

        self.order = None
        self.stop_order = None
        self.entry_price = None
        self.entry_high = None  # 숏 진입 당시의 캔들 high

    def vol_condition(self):
        """거래량 증가(논문에서도 언급)."""
        return self.vol_fast_sma[0] > self.vol_slow_sma[0]

    def is_low_vol(self):
        """(vli_fast < vli_slow) => 저변동성 구간."""
        return self.vli.vli_fast[0] < self.vli.vli_slow[0]

    def is_extreme_vol(self):
        """BBW > vli_top => 극단적 변동성."""
        return self.bbw.bbw[0] > self.vli.vli_top[0]

    def check_entry_signal(self):
        """
        [숏 진입 로직]
          1) Bollinger 하단선 CrossUp
          2) 거래량 증가
          3) 극단 변동성 피하기(BBW <= vli_top)
          4) low vol => sma_mid < sma_veryslow
          5) high vol => sma_slow < sma_veryslow
        """
        if self.cross_bot_up[0] == 1 and self.vol_condition():
            if not self.is_extreme_vol():
                if self.is_low_vol():
                    # 저변동성 => sma_mid < sma_veryslow => 하락장
                    if self.sma_mid[0] < self.sma_veryslow[0]:
                        return True
                else:
                    # 고변동성 => sma_slow < sma_veryslow
                    if self.sma_slow[0] < self.sma_veryslow[0]:
                        return True
        return False

    def check_exit_signal(self):
        """
        [숏 청산 로직]
          - Bollinger 상단선 CrossUp + 거래량 증가 => 숏 청산
        """
        if self.cross_top_up[0] == 1 and self.vol_condition():
            return True
        return False

    def notify_order(self, order):
        """
        체결 이후 => 손절 주문 설정
        (진입 캔들의 high + 5% 예시)
        """
        if order.status in [order.Completed]:
            self.order = None
            if order.issell():  # 숏 체결
                if self.entry_high is not None:
                    stop_price = self.entry_high * (1.0 + self.p.slippage_factor)
                    if stop_price > self.data.close[0]:
                        # Buy Stop (숏 포지션 손절)
                        self.stop_order = self.buy(exectype=bt.Order.Stop, price=stop_price)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order = None

    def next(self):
        if self.order:
            return  # 이전 주문이 체결 대기 중이면 대기

        if not self.position:
            # 포지션 없으면 -> 숏 진입 체크
            if self.check_entry_signal():
                self.entry_price = self.data.close[0]
                self.entry_high = self.data.high[0]
                self.order = self.sell()
        else:
            # 포지션 중 -> 청산 조건
            if self.check_exit_signal():
                self.order = self.close()
            else:
                # 트레일링 스탑 갱신
                self.update_stopwin()

    def update_stopwin(self):
        """
        논문의 다단계 수익률 관리.
        (10%, 15%, 20%, 25%, 30% ... 구간별로 stopwin 상향)
        또한 3% 넘으면 1% 정도 소규모 stopwin 설정.
        (숏 포지션에서는 'buy stop'을 위로 끌어올리는 형태)
        """
        if not self.position or not self.stop_order:
            return

        current_price = self.data.close[0]
        # 숏의 수익률 = (진입가 - 현재가) / 진입가
        pnl_pct = (self.entry_price - current_price) / self.entry_price

        best_stopwin_pct = None

        # 3% 넘으면 1% stopwin
        if pnl_pct >= self.p.minor_trigger:
            best_stopwin_pct = self.p.minor_stop

        # profit_steps 예: [(0.10, 0.05), (0.15, 0.10), ...]
        for (thr, sw) in self.p.profit_steps:
            if pnl_pct >= thr:
                best_stopwin_pct = sw

        if best_stopwin_pct is not None:
            # stop price = entry_price - (entry_price * stopwin_pct)
            new_stop_price = self.entry_price * (1.0 - best_stopwin_pct)

            # 기존 stop_order 가격보다 더 낮은지(숏은 buy stop이 '낮아야' 더 이득) 확인
            # Backtrader에서 stop_order.created.price 로 기존 가격 확인
            old_stop_price = None
            if hasattr(self.stop_order, 'created'):
                old_stop_price = getattr(self.stop_order.created, 'price', None)

            # 숏 포지션에서는 'buy stop'을 아래로 조정하는 것(수익 잠금),
            # 하지만 실제론 *위*로 조정하면 손실 한계가 줄어드는 형태.
            # 여기서는 '수익률 증가 -> stopwin 가격을 내림'으로 해석
            # => new_stop_price가 기존보다 높으면(=재산출) 역전 개념이 되므로 주의.
            #
            # 만약 "수익률 증가 -> buy stop을 (현재가보다 더 아래)로 내려 수익 보존" 식이면
            # 숏 전략 로직과는 약간 충돌할 수 있음.
            # -- 논문에 따라, "기존 stop보다 더 위로" 등으로 조정해야 할 수도 있음.
            # => 아래는 'buy stop'을 "진입가 - (진입가 * stopwin_pct)"로 낮추는 예시 구현.

            # 실제 트레일링 관점에서 "stop이 더 위"가 숏 입장에선 더 안전
            # -> 그럼 old_stop_price 와 new_stop_price 비교는 "if new_stop_price > old_stop_price:" 처럼 해야 됨.
            # 숏일 때는 'stop price'가 올라갈수록 (=가격이 더 높은 쪽) 손해 제한이 줄어듦(익절폭 확대).
            # 아래 로직은 논문 구현 예시에 맞춰 "stopwin을 한단계씩 위로 끌어올린다"로 해석.

            if old_stop_price is not None:
                # 숏에서는 buy_stop이 높아질수록(=가격 상승) 손실 제한이 더 작아지는 = 수익 잠금 확대
                if new_stop_price > old_stop_price:
                    # 더 위쪽 stop 주문을 갱신
                    self.cancel(self.stop_order)
                    self.stop_order = self.buy(
                        exectype=bt.Order.Stop,
                        price=new_stop_price
                    )
            else:
                # 기존 stop_price가 없거나 못 가져온 경우 -> 그냥 주문 갱신
                self.cancel(self.stop_order)
                self.stop_order = self.buy(
                    exectype=bt.Order.Stop,
                    price=new_stop_price
                )
