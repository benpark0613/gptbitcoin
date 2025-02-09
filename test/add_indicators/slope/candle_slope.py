import csv
import os
import datetime

import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

# interval 별로 1봉이 몇 시간(hours)에 해당하는지 매핑
INTERVAL_HOURS = {
    Client.KLINE_INTERVAL_1MINUTE: 1.0 / 60,
    Client.KLINE_INTERVAL_3MINUTE: 3.0 / 60,
    Client.KLINE_INTERVAL_5MINUTE: 5.0 / 60,
    Client.KLINE_INTERVAL_15MINUTE: 15.0 / 60,
    Client.KLINE_INTERVAL_30MINUTE: 30.0 / 60,
    Client.KLINE_INTERVAL_1HOUR: 1.0,
    Client.KLINE_INTERVAL_2HOUR: 2.0,
    Client.KLINE_INTERVAL_4HOUR: 4.0,
    Client.KLINE_INTERVAL_6HOUR: 6.0,
    Client.KLINE_INTERVAL_8HOUR: 8.0,
    Client.KLINE_INTERVAL_12HOUR: 12.0,
    Client.KLINE_INTERVAL_1DAY: 24.0,
    Client.KLINE_INTERVAL_3DAY: 72.0,
    Client.KLINE_INTERVAL_1WEEK: 24.0 * 7,
    Client.KLINE_INTERVAL_1MONTH: 24.0 * 30,  # 대략 값
}

def load_binance_client():
    """Binance API Client를 로드합니다."""
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    return Client(api_key, api_secret)

def fetch_futures_klines(client, symbol, interval, start_ms, end_ms):
    """Binance 선물 Kline 데이터를 millisecond 단위 timestamp 범위로 조회"""
    klines = client.futures_klines(
        symbol=symbol,
        interval=interval,
        startTime=start_ms,
        endTime=end_ms
    )
    return klines

def klines_to_dataframe(klines):
    """
    Kline 리스트를 Pandas DataFrame으로 변환,
    open_time, close_time을 KST로 변경,
    숫자형 컬럼 변환
    """
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])

    numeric_cols = [
        "open", "high", "low", "close", "volume",
        "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)

    # UTC→KST 변환
    df["open_time_kst"] = pd.to_datetime(df["open_time"], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')
    df["close_time_kst"] = pd.to_datetime(df["close_time"], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

    return df

def calculate_ma_slopes(df):
    """
    5, 20, 60 이동평균선 계산 + Slope(직전 봉 대비 차이) 컬럼 추가
    """
    # 5, 20, 60 이동평균
    df["MA_5"] = df["close"].rolling(window=5).mean()
    df["MA_20"] = df["close"].rolling(window=20).mean()
    df["MA_60"] = df["close"].rolling(window=60).mean()

    # Slope = diff() -> 직전 값과의 차
    df["Slope_5"] = df["MA_5"].diff()
    df["Slope_20"] = df["MA_20"].diff()
    df["Slope_60"] = df["MA_60"].diff()

    return df

def save_to_csv(df, filename):
    """
    CSV 저장. 기존 시세 컬럼 + MA & Slope 컬럼을 모두 포함.
    float_format='%.2f' 로 소수 둘째자리까지 반올림 출력.
    """
    output_cols = [
        "open_time_kst", "open", "high", "low", "close", "volume",
        "close_time_kst", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume",
        "MA_5", "MA_20", "MA_60",
        "Slope_5", "Slope_20", "Slope_60"
    ]
    # 실제 존재하는 컬럼만 필터
    output_cols = [c for c in output_cols if c in df.columns]

    # to_csv에서 float_format 파라미터로 소수점 둘째자리까지만 출력
    df[output_cols].to_csv(filename, index=False, encoding="utf-8", float_format='%.2f')

def main():
    # 사용자 지정 파라미터
    symbol = 'BTCUSDT'
    interval = Client.KLINE_INTERVAL_1MINUTE   # 1시간봉
    start_str = "2025-01-01"
    end_str = "2025-02-01"

    # 이동평균 중 가장 긴 것이 60봉이므로 60봉만큼 추가 조회
    largest_ma = 60

    # 날짜 변환
    start_dt = datetime.datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_str, "%Y-%m-%d")

    # interval별로 1봉이 몇 시간이 걸리는지
    hours_per_candle = INTERVAL_HOURS[interval]
    extra_hours = largest_ma * hours_per_candle

    # 실제 조회 시작 시간을 (start_dt - extra_hours)로 조정
    start_dt_for_fetch = start_dt - datetime.timedelta(hours=extra_hours)

    start_ms = int(start_dt_for_fetch.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # 바이낸스 클라이언트
    client = load_binance_client()

    # 데이터 수집
    klines = fetch_futures_klines(client, symbol, interval, start_ms, end_ms)
    df = klines_to_dataframe(klines)

    # 5, 20, 60 MA & Slope 계산
    df = calculate_ma_slopes(df)

    # 초기 NaN 제거 (rolling(60)로 인해 초반 59개는 NaN)
    df.dropna(subset=["MA_60"], inplace=True)

    # 기간 필터링
    mask = (df["open_time_kst"] >= pd.Timestamp(start_str, tz="Asia/Seoul")) & \
           (df["open_time_kst"] < pd.Timestamp(end_str, tz="Asia/Seoul"))
    df_filtered = df[mask].copy()

    # CSV 저장 (소수 둘째자리)
    save_to_csv(df_filtered, "btc_usdt_futures_klines_with_MAs_and_Slopes.csv")

if __name__ == "__main__":
    main()
