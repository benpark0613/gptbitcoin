import pyupbit
import pandas as pd
import numpy as np
import ta
from ta.utils import dropna
from datetime import datetime, timedelta

def calculate_atr_threshold(
    ticker="KRW-BTC",
    days=180,            # 6개월(약 180일)
    atr_window=8,
    percentile=0.85,     # 상위 85% 분위수를 기준으로
    interval="minute60"
):
    """
    과거 일정 기간의 1시간봉 데이터를 가져와 ATR(atr_window)을 계산한 뒤,
    상위 percentile 분위수를 임계값(threshold)으로 반환한다.
    """
    # 최대 요청 가능한 분봉 데이터 수 제한을 고려해야 하지만,
    # 여기서는 간단히 count=24*days 정도로 요청.
    # PyUpbit는 한 번에 최대 200개 봉까지만 받으므로,
    # 6개월치를 모두 받으려면 여러 번 호출해야 한다는 점에 주의.
    # 여기서는 예시로 보여주기 위해 24*180=4320봉을 한 번에 가져온다고 가정.
    df = pyupbit.get_ohlcv(ticker, interval=interval, count=24 * days)

    if df is None or len(df) == 0:
        # 데이터 불러오기에 실패하면 None 반환
        return None

    df = dropna(df)

    # ATR(8) 계산
    atr_indicator = ta.volatility.AverageTrueRange(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=atr_window
    )
    df["atr"] = atr_indicator.average_true_range()

    # 분위수 계산
    threshold_value = df["atr"].quantile(percentile)

    return threshold_value

def add_indicators_hourly_auto_threshold(df, threshold):
    """
    1시간봉에서 ATR 임계값을 외부로부터 주입받아 사용.
    threshold는 과거 데이터 기반으로 자동 계산된 값.
    """
    df["atr_temp"] = ta.volatility.AverageTrueRange(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=8
    ).average_true_range()

    # 현재 최신 봉의 ATR 값
    current_atr = df["atr_temp"].iloc[-1]

    if current_atr > threshold:
        # 변동성이 높을 때: 더욱 빠른 신호 감지
        macd_slow, macd_fast, macd_sign = 17, 8, 5
        bb_window, bb_dev = 16, 2.1
        rsi_window = 7
        stoch_window = 8
    else:
        # 변동성이 낮을 때: 기본 공격적 세팅
        macd_slow, macd_fast, macd_sign = 20, 9, 7
        bb_window, bb_dev = 16, 2.0
        rsi_window = 7
        stoch_window = 8

    # Bollinger Bands
    bb = ta.volatility.BollingerBands(
        close=df["close"],
        window=bb_window,
        window_dev=bb_dev
    )
    df["bb_bbm"] = bb.bollinger_mavg()
    df["bb_bbh"] = bb.bollinger_hband()
    df["bb_bbl"] = bb.bollinger_lband()

    # RSI
    df["rsi"] = ta.momentum.RSIIndicator(
        close=df["close"],
        window=rsi_window
    ).rsi()

    # MACD
    macd = ta.trend.MACD(
        close=df["close"],
        window_slow=macd_slow,
        window_fast=macd_fast,
        window_sign=macd_sign
    )
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_diff"] = macd.macd_diff()

    # SMA(16) & EMA(8)
    df["sma_16"] = ta.trend.SMAIndicator(
        close=df["close"],
        window=16
    ).sma_indicator()
    df["ema_8"] = ta.trend.EMAIndicator(
        close=df["close"],
        window=8
    ).ema_indicator()

    # Stochastic
    stoch = ta.momentum.StochasticOscillator(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=stoch_window,
        smooth_window=3
    )
    df["stoch_k"] = stoch.stoch()
    df["stoch_d"] = stoch.stoch_signal()

    # 최종 ATR에 반영
    df["atr"] = df["atr_temp"]
    df.drop(columns=["atr_temp"], inplace=True)

    # OBV
    df["obv"] = ta.volume.OnBalanceVolumeIndicator(
        close=df["close"],
        volume=df["volume"]
    ).on_balance_volume()

    return df


if __name__ == "__main__":
    # 1) 과거 6개월 데이터로부터 ATR 임계값 계산
    computed_threshold = calculate_atr_threshold(
        ticker="KRW-BTC",
        days=180,         # 6개월 정도
        atr_window=8,
        percentile=0.85   # 상위 85% 분위수
    )

    print("Computed ATR threshold (85th percentile):", computed_threshold)

    # 2) 실시간(혹은 최근) 1시간봉 데이터를 받아 인디케이터 계산
    df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=48)
    df_hourly = dropna(df_hourly)

    # 3) 자동 계산된 임계값으로 보조지표 생성
    if computed_threshold:
        df_hourly = add_indicators_hourly_auto_threshold(
            df_hourly,
            threshold=computed_threshold
        )
        print(df_hourly.tail(5))
    else:
        print("Error: Could not compute threshold.")
