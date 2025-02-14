import os
import shutil
import pandas as pd
from datetime import datetime, timezone
from binance.client import Client
from dotenv import load_dotenv
import backtrader as bt


# -----------------------------------
# Helper: interval 문자열에 따른 timeframe/ compression 결정
# -----------------------------------
def get_timeframe_and_compression(interval: str):
    """
    Binance interval 문자열에 따른 Backtrader timeframe과 compression 값을 반환.
    예) '5m' -> (bt.TimeFrame.Minutes, 5), '1h' -> (bt.TimeFrame.Minutes, 60), '1d' -> (bt.TimeFrame.Days, 1)
    """
    if interval.endswith('m'):
        minutes = int(interval[:-1])
        return (bt.TimeFrame.Minutes, minutes)
    elif interval.endswith('h'):
        hours = int(interval[:-1])
        return (bt.TimeFrame.Minutes, hours * 60)
    elif interval.endswith('d'):
        days = int(interval[:-1])
        return (bt.TimeFrame.Days, days)
    else:
        raise ValueError("지원하지 않는 interval 형식입니다: " + interval)


# -----------------------------------
# 바이낸스 데이터 가져오기 관련 함수들
# -----------------------------------
def datetime_to_milliseconds(dt_str):
    if len(dt_str.strip()) == 10:
        dt_str += " 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms, limit=1500):
    """
    바이낸스 선물 OHLCV 데이터를 한 번에 limit개씩 가져옵니다.
    """
    all_data = []
    current_start = start_ms
    while True:
        klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            startTime=current_start,
            endTime=end_ms,
            limit=limit
        )
        if not klines:
            break
        all_data.extend(klines)
        last_open_time = klines[-1][0]
        next_start = last_open_time + 1
        if next_start > end_ms:
            break
        current_start = next_start
    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    return df


def fetch_and_save_data(symbol="BTCUSDT", interval="1h", start_date="2024-01-01", end_date="2025-01-01",
                        save_folder="test_result"):
    """
    바이낸스에서 지정한 기간 및 타임프레임의 데이터를 가져와 CSV로 저장합니다.
    """
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    client = Client(api_key, api_secret)

    start_ms = datetime_to_milliseconds(start_date)
    end_ms = datetime_to_milliseconds(end_date)

    if os.path.exists(save_folder):
        shutil.rmtree(save_folder)
    os.makedirs(save_folder, exist_ok=True)

    df = fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms)

    # ✅ open_time (밀리초 → 날짜/시간 변환)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms").dt.strftime('%Y-%m-%d %H:%M:%S')

    # ✅ 컬럼 정리: 필요한 데이터만 저장
    df = df[["open_time", "open", "high", "low", "close", "volume"]]

    file_name = f"{symbol}_{interval}.csv"
    file_path = os.path.join(save_folder, file_name)

    # ✅ 인덱스를 CSV에 저장하지 않음 (index=False)
    df.to_csv(file_path, encoding="utf-8", index=False)

    print(f"[{symbol} - {interval}] : {len(df)} 건 데이터 저장 완료 → {file_path}")
    return file_path

# -----------------------------------
# Backtrader용 Heikin Ashi 인디케이터
# -----------------------------------
class HeikinAshi(bt.Indicator):
    lines = ('ha_open', 'ha_high', 'ha_low', 'ha_close',)
    plotinfo = dict(subplot=False)
    plotlines = dict(ha_open=dict(ls='--', color='blue'),
                     ha_close=dict(ls='-', color='blue'),
                     ha_high=dict(ls='--', color='gray'),
                     ha_low=dict(ls='--', color='gray'))

    def __init__(self):
        self.addminperiod(1)

    def next(self):
        self.lines.ha_close[0] = (self.data.open[0] + self.data.high[0] +
                                  self.data.low[0] + self.data.close[0]) / 4.0
        if len(self) == 1:
            self.lines.ha_open[0] = (self.data.open[0] + self.data.close[0]) / 2.0
        else:
            self.lines.ha_open[0] = (self.lines.ha_open[-1] + self.lines.ha_close[-1]) / 2.0
        self.lines.ha_high[0] = max(self.data.high[0], self.lines.ha_open[0], self.lines.ha_close[0])
        self.lines.ha_low[0] = min(self.data.low[0], self.lines.ha_open[0], self.lines.ha_close[0])


# -----------------------------------
# EMA + Heikin Ashi + Parabolic SAR 전략
# -----------------------------------
class EMAHeikinAshiParabolicSARStrategy(bt.Strategy):
    params = dict(
        atr_period=14,  # ATR 계산 기간
        ema_period=200,  # EMA 기간
        stop_loss_multiplier=2  # 손절 조건 배수
    )

    def __init__(self):
        self.ema = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.p.ema_period)
        self.psar = bt.indicators.ParabolicSAR(self.data)
        self.atr = bt.indicators.ATR(self.data, period=self.p.atr_period)
        self.ha = HeikinAshi(self.data)
        self.order = None
        self.entry_price = None

    def next(self):
        if not self.position:
            if (self.data.close[0] > self.ema[0] and
                    self.ha.ha_close[0] > self.ha.ha_open[0] and
                    self.psar[0] < self.ha.ha_low[0]):
                self.order = self.buy()
                self.entry_price = self.data.close[0]
            elif (self.data.close[0] < self.ema[0] and
                  self.ha.ha_close[0] < self.ha.ha_open[0] and
                  self.psar[0] > self.ha.ha_high[0]):
                self.order = self.sell()
                self.entry_price = self.data.close[0]
        else:
            if self.position.size > 0:
                if (self.psar[0] > self.ha.ha_high[0] and self.ha.ha_close[0] < self.ha.ha_open[0]):
                    self.close()
                elif self.data.close[0] < self.entry_price - self.p.stop_loss_multiplier * self.atr[0]:
                    self.close()
            elif self.position.size < 0:
                if (self.psar[0] < self.ha.ha_low[0] and self.ha.ha_close[0] > self.ha.ha_open[0]):
                    self.close()
                elif self.data.close[0] > self.entry_price + self.p.stop_loss_multiplier * self.atr[0]:
                    self.close()


# -----------------------------------
# main 함수: 사용자가 설정값만 입력하면 실행되도록 함
# -----------------------------------
def main(symbol="BTCUSDT", interval="1h", start_date="2024-01-01", end_date="2025-01-01",
         initial_cash=100000.0, stake=1, save_folder="test_result",
         atr_period=14, ema_period=200, stop_loss_multiplier=2):
    # 1. 데이터 수집 및 CSV 저장
    csv_path = fetch_and_save_data(symbol, interval, start_date, end_date, save_folder)

    # 2. interval 에 따른 timeframe과 compression 결정
    timeframe, compression = get_timeframe_and_compression(interval)

    # 3. Cerebro 및 전략, Analyzer 설정
    cerebro = bt.Cerebro()
    cerebro.addstrategy(
        EMAHeikinAshiParabolicSARStrategy,
        atr_period=atr_period,
        ema_period=ema_period,
        stop_loss_multiplier=stop_loss_multiplier
    )
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns_analyzer')

    # 4. CSV 데이터를 Backtrader 데이터 피드로 로드
    data = bt.feeds.GenericCSVData(
        dataname=csv_path,
        dtformat='%Y-%m-%d %H:%M:%S',
        datetime=0,  # CSV 첫 열 (open_time_dt)
        open=1,
        high=2,
        low=3,
        close=4,
        volume=5,
        openinterest=-1,
        timeframe=timeframe,
        compression=compression
    )
    cerebro.adddata(data)

    # 5. 초기 자본 및 포지션 사이즈 설정
    cerebro.broker.setcash(initial_cash)
    cerebro.addsizer(bt.sizers.FixedSize, stake=stake)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    strat = results[0]

    trade_analysis = strat.analyzers.trade_analyzer.get_analysis()
    returns_analysis = strat.analyzers.returns_analyzer.get_analysis()

    final_value = cerebro.broker.getvalue()
    pnl = final_value - initial_cash
    rate_of_return = (pnl / initial_cash) * 100
    total_closed_trades = trade_analysis.total.closed if 'closed' in trade_analysis.total else 0

    print('Final Portfolio Value: %.2f' % final_value)
    print(f"총 거래 건수(Closed Trades): {total_closed_trades}")
    print(f"최종 수익률(%)      : {rate_of_return:.2f}%")
    print(f"최종 계좌 잔고($)  : {final_value:.2f}")
    if 'rtot' in returns_analysis:
        print(f"누적수익률(%) : {returns_analysis['rtot'] * 100:.2f}%")
    if 'rnorm100' in returns_analysis:
        print(f"연평균화 수익률(%) : {returns_analysis['rnorm100']:.2f}%")

    cerebro.plot(style='candlestick')


if __name__ == '__main__':
    # 사용자가 아래 설정값만 수정하면 됩니다.
    main(
        symbol="BTCUSDT",
        interval="15m",  # 예: "5m", "15m", "1h", "1d", "30m" 등
        start_date="2024-12-01",
        end_date="2025-02-13",
        initial_cash=100_000_000.0,
        stake=1,
        save_folder="test_result",
        atr_period=14,  # ATR 기간 (기본 14)
        ema_period=200,  # EMA 기간 (기본 200)
        stop_loss_multiplier=2  # 손절 조건 배수 (기본 2)
    )
