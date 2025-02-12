"""
MTF전략(4H EMA+ADX, 15m RSI/Boll/ATR) + StopLoss(ATR×배수) + 부분익절(1.5R) + 최종익절(2R) + (옵션)트레일링
+ 파라미터 최적화(optstrategy)

(1) 바이낸스에서 15m/4h 데이터를 가져와 CSV 저장
(2) Backtrader로 DataFeed 로드
(3) Strategy는 전역 스코프(top-level)에 정의 -> Windows 멀티프로세싱 pickle 이슈 완화
(4) cerebro.optstrategy(...)로 여러 파라미터 조합 최적화
"""

import os
import time
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from binance.client import Client
import backtrader as bt
from datetime import datetime

#────────────────────────────────────────────────────────────────────────
# (A) 바이낸스 15m/4h 데이터 수집 + CSV
#────────────────────────────────────────────────────────────────────────
def fetch_data_all(
    symbol: str,
    interval: str,
    api_key: str,
    api_secret: str,
    start_time: int,
    end_time: int,
    limit_per_call=1500
) -> pd.DataFrame:
    """
    바이낸스 USDT-M 선물 klines:
      - start_time~end_time 구간(밀리초)
      - limit=1500씩 반복 호출 -> concat
      - pandas DataFrame
    """
    client = Client(api_key, api_secret)
    result_list = []
    current_start = start_time

    while True:
        klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            limit=limit_per_call,
            startTime=current_start,
            endTime=end_time
        )
        if not klines:
            break

        df_part = pd.DataFrame(klines)
        result_list.append(df_part)

        last_close_time = klines[-1][6]
        current_start = last_close_time + 1
        if current_start >= end_time:
            break

        time.sleep(0.2)

    if not result_list:
        return pd.DataFrame()

    df_all = pd.concat(result_list, ignore_index=True)

    # 열이름 변환
    df_all.rename(columns={
        0:"open_time",
        1:"Open",
        2:"High",
        3:"Low",
        4:"Close",
        5:"Volume",
        6:"close_time",
        7:"quote_asset_volume",
        8:"trades",
        9:"taker_base_vol",
        10:"taker_quote_vol",
        11:"ignore"
    }, inplace=True)

    # 중복 제거
    df_all.drop_duplicates(subset=["open_time","close_time"], keep="first", inplace=True)

    # 숫자 변환
    for c in ["Open","High","Low","Close","Volume"]:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce")

    df_all["open_time"] = pd.to_datetime(df_all["open_time"], unit="ms")
    df_all.set_index("open_time", inplace=True)
    df_all.sort_index(inplace=True)
    return df_all

#────────────────────────────────────────────────────────────────────────
# (B) CSV -> Backtrader DataFeed
#────────────────────────────────────────────────────────────────────────
def load_csv_as_btdata(csv_file, timeframe=bt.TimeFrame.Minutes, compression=15):
    df = pd.read_csv(csv_file, parse_dates=True, index_col="open_time", encoding="utf-8-sig")
    data = bt.feeds.PandasData(
        dataname=df,
        timeframe=timeframe,
        compression=compression,
        openinterest=None
    )
    return data

#────────────────────────────────────────────────────────────────────────
# (C) Strategy 클래스(전역 스코프)
#────────────────────────────────────────────────────────────────────────
class MtfResearchStrategy(bt.Strategy):
    """
    (연구 보고서 로직)
    4H: EMA50 + ADX>25 => 추세 필터
    15m: RSI/Boll/ATR => 진입 시점
    StopLoss=ATR×stop_atr_mult
    부분익절= (진입가 ± R×partial_rr), 최종익절= (± R×rr_ratio)
    (옵션) 트레일링 스탑
    데이 트레이딩(옵션)
    """
    params = dict(
        adx_thres=25,       # ADX 임계값
        rsi_os=30,          # RSI 과매도
        rsi_ob=70,          # RSI 과매수
        stop_atr_mult=1.5,  # 손절 ATR 배수
        partial_rr=1.5,     # 부분익절 R배수
        rr_ratio=2.0,       # 최종익절 R배수
        daytrade_close=True,
        use_trail=False,
        trail_amount=200.0
    )

    def __init__(self):
        # 4H 지표
        self.ema4h = bt.indicators.ExponentialMovingAverage(self.datas[1].close, period=50)
        self.adx4h = bt.indicators.AverageDirectionalMovementIndex(self.datas[1], period=14)

        # 15m 지표
        self.rsi  = bt.indicators.RSI_Safe(self.datas[0], period=14)
        self.boll = bt.indicators.BollingerBands(self.datas[0], period=20, devfactor=2.0)
        self.atr  = bt.indicators.ATR(self.datas[0], period=14)

        self.current_day = None
        self.entry_price = None
        self.sl_price    = None
        self.risk        = None

    def next(self):
        # (1) 데이 트레이딩
        dt0 = self.datas[0].datetime.date(0)
        if self.p.daytrade_close:
            if self.current_day is None:
                self.current_day = dt0
            elif dt0 != self.current_day:
                if self.position:
                    self.close()
                self.current_day = dt0

        # (2) 4H 추세
        c4  = self.datas[1].close[0]
        ema = self.ema4h[0]
        adx = self.adx4h.adx[0]

        uptrend   = (c4 > ema) and (adx > self.p.adx_thres)
        downtrend = (c4 < ema) and (adx > self.p.adx_thres)

        # (3) 15m
        c15  = self.datas[0].close[0]
        rsi_v= self.rsi[0]
        b_low= self.boll.bot[0]
        b_up = self.boll.top[0]
        atrv = self.atr[0]

        if not self.position:
            # 매수
            if uptrend:
                rsi_prev = self.rsi[-1]
                c_prev   = self.datas[0].close[-1]
                b_prev   = self.boll.bot[-1]

                was_oversold = (rsi_prev < self.p.rsi_os) and (c_prev < b_prev)
                now_recover  = (rsi_v > self.p.rsi_os) or (c15 > b_low)

                if was_oversold and now_recover:
                    self.buy(size=1)
                    self.sl_price = c15 - (atrv*self.p.stop_atr_mult)
                    self.risk     = c15 - self.sl_price
                    self.entry_price = c15

            # 매도
            elif downtrend:
                rsi_prev = self.rsi[-1]
                c_prev   = self.datas[0].close[-1]
                b_prev   = self.boll.top[-1]

                was_overbought= (rsi_prev > self.p.rsi_ob) and (c_prev > b_prev)
                now_recover   = (rsi_v < self.p.rsi_ob) or (c15 < b_up)

                if was_overbought and now_recover:
                    self.sell(size=1)
                    self.sl_price = c15 + (atrv*self.p.stop_atr_mult)
                    self.risk     = self.sl_price - c15
                    self.entry_price = c15

        else:
            pass

    def notify_order(self, order):
        if order.status in [order.Completed]:
            # --- BUY 체결 ---
            if order.isbuy() and order.exectype == 0:
                print(f"[BUY EXEC] price={order.executed.price:.2f}")
                if self.entry_price and self.risk:
                    part_tp  = self.entry_price + (self.risk*self.p.partial_rr)
                    final_tp = self.entry_price + (self.risk*self.p.rr_ratio)

                    # 부분익절(0.5)
                    self.partial_sell = self.sell(size=0.5, exectype=bt.Order.Limit, price=part_tp)
                    # 최종익절(0.5)
                    self.final_sell   = self.sell(size=0.5, exectype=bt.Order.Limit, price=final_tp)
                    # 스탑로스
                    self.stop_sell    = self.sell(size=1.0, exectype=bt.Order.Stop, price=self.sl_price)

                    if self.p.use_trail:
                        if self.stop_sell.alive():
                            self.cancel(self.stop_sell)
                        self.trail_sell = self.sell(size=0.5, exectype=bt.Order.StopTrail, trailamount=self.p.trail_amount)

            # --- SELL 체결 ---
            elif not order.isbuy() and order.exectype == 0:
                print(f"[SELL EXEC] price={order.executed.price:.2f}")
                if self.entry_price and self.risk:
                    part_tp  = self.entry_price - (self.risk*self.p.partial_rr)
                    final_tp = self.entry_price - (self.risk*self.p.rr_ratio)

                    self.partial_buy = self.buy(size=0.5, exectype=bt.Order.Limit, price=part_tp)
                    self.final_buy   = self.buy(size=0.5, exectype=bt.Order.Limit, price=final_tp)
                    self.stop_buy    = self.buy(size=1.0, exectype=bt.Order.Stop, price=self.sl_price)

                    if self.p.use_trail:
                        if self.stop_buy.alive():
                            self.cancel(self.stop_buy)
                        self.trail_buy = self.buy(size=0.5, exectype=bt.Order.StopTrail, trailamount=self.p.trail_amount)

            else:
                print(f"[ORDER EXECUTED] exectype={order.exectype}, price={order.executed.price:.2f}")
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f"[ORDER FAILED] status={order.status}")

    def notify_trade(self, trade):
        if trade.isclosed:
            print(f"[TRADE CLOSED] PnL={trade.pnl:.2f}")
            self.entry_price = None
            self.sl_price    = None
            self.risk        = None

#────────────────────────────────────────────────────────────────────────
# (D) 최적화 함수
#────────────────────────────────────────────────────────────────────────
def run_optimization(symbol="BTCUSDT", start_dt="2024-01-01", end_dt="2025-02-01", cash=1_000_000):
    """
    Backtrader optstrategy 예시:
      - adx_thres ∈ {20,25,30}
      - stop_atr_mult ∈ {1.0,1.2,1.5}
      - partial_rr ∈ {1.2,1.5,2.0}
      - daytrade_close ∈ {False,True}
      - use_trail ∈ {False, True}
    """
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY","")
    api_secret= os.getenv("BINANCE_SECRET_KEY","")

    start_ms = int(datetime.strptime(start_dt, "%Y-%m-%d").timestamp()*1000)
    end_ms   = int(datetime.strptime(end_dt,   "%Y-%m-%d").timestamp()*1000)

    print("=== Fetching 15m data => 'BTCUSDT_15m.csv' ===")
    df_15m = fetch_data_all(symbol, Client.KLINE_INTERVAL_15MINUTE, api_key, api_secret, start_ms, end_ms)
    df_15m.to_csv("BTCUSDT_15m.csv", encoding="utf-8-sig")

    print("=== Fetching 4h data => 'BTCUSDT_4h.csv' ===")
    df_4h = fetch_data_all(symbol, Client.KLINE_INTERVAL_4HOUR, api_key, api_secret, start_ms, end_ms)
    df_4h.to_csv("BTCUSDT_4h.csv", encoding="utf-8-sig")

    cerebro = bt.Cerebro()

    data_15m = load_csv_as_btdata("BTCUSDT_15m.csv", timeframe=bt.TimeFrame.Minutes, compression=15)
    data_4h  = load_csv_as_btdata("BTCUSDT_4h.csv",  timeframe=bt.TimeFrame.Minutes, compression=240)

    cerebro.adddata(data_15m, name="15m")
    cerebro.adddata(data_4h,  name="4h")

    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=0.0004)

    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio,   _name='sharpe')

    # 여기서 멀티프로세싱 pickle 문제가 나면, maxcpus=1 로 바꿔보세요
    # optreturn=True => (Strategy,) 형태로 결과 반환
    cerebro.optstrategy(
        MtfResearchStrategy,
        adx_thres=[20,25,30],
        stop_atr_mult=[1.0,1.2,1.5],
        partial_rr=[1.2,1.5,2.0],
        daytrade_close=[False, True],
        use_trail=[False, True]
    )

    results = cerebro.run(optreturn=True, maxcpus=1)  # , maxcpus=1) # 병렬 off 시에는 maxcpus=1
    final_list = []
    best_profit = -999999999
    best_params = None

    for run in results:
        strat = run[0]
        params_dict = strat.p._getkwargs()

        trades_ana = strat.analyzers.trades.get_analysis()
        sharpe_ana = strat.analyzers.sharpe.get_analysis()

        closed = trades_ana.total.closed if ('total' in trades_ana and 'closed' in trades_ana.total) else 0
        won = trades_ana.won.total if closed>0 else 0
        winrate = (won/closed*100) if closed>0 else 0

        final_val = strat.broker.getvalue()
        profit = final_val - cash

        final_list.append((params_dict, closed, winrate, profit))
        if profit > best_profit:
            best_profit = profit
            best_params = params_dict

    # 정렬(수익 내림차순)
    final_list.sort(key=lambda x: x[3], reverse=True)

    print("=== Top 5 results ===")
    for i in range(min(5, len(final_list))):
        pdic, c, wr, pf = final_list[i]
        print(f"{i+1}) Profit={pf:.2f}, Trades={c}, WinRate={wr:.2f}%, Params={pdic}")

    print("\n--- BEST PARAMS (Profit) ---")
    print(best_params)

#────────────────────────────────────────────────────────────────────────
# (E) 메인
#────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    예)
    python this_file.py
    -> 파라미터 최적화 돌려서 best 조합 찾음
    """
    run_optimization(
        symbol="BTCUSDT",
        start_dt="2025-01-01",
        end_dt="2025-02-01",
        cash=1_000_000
    )
