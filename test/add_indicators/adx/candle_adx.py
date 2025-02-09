import csv
import os
import datetime

import pandas as pd
import pandas_ta as ta

from binance.client import Client
from dotenv import load_dotenv


# interval마다 1캔들이 몇 분인지 매핑
INTERVAL_MINUTES_MAPPING = {
    Client.KLINE_INTERVAL_1MINUTE: 1,
    Client.KLINE_INTERVAL_3MINUTE: 3,
    Client.KLINE_INTERVAL_5MINUTE: 5,
    Client.KLINE_INTERVAL_15MINUTE: 15,
    Client.KLINE_INTERVAL_30MINUTE: 30,
    Client.KLINE_INTERVAL_1HOUR: 60,
    Client.KLINE_INTERVAL_2HOUR: 120,
    Client.KLINE_INTERVAL_4HOUR: 240,
    Client.KLINE_INTERVAL_6HOUR: 360,
    Client.KLINE_INTERVAL_8HOUR: 480,
    Client.KLINE_INTERVAL_12HOUR: 720,
    Client.KLINE_INTERVAL_1DAY: 1440,
    Client.KLINE_INTERVAL_3DAY: 4320,
    Client.KLINE_INTERVAL_1WEEK: 10080,   # 7일 × 24시간 × 60분
    Client.KLINE_INTERVAL_1MONTH: 43200,  # (대략) 30일 × 24 × 60
}


def load_binance_client():
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    return Client(api_key, api_secret)


def fetch_futures_klines(client, symbol, interval, start_time, end_time):
    """Binance 선물 Kline 데이터를 조회합니다."""
    return client.futures_klines(
        symbol=symbol,
        interval=interval,
        startTime=start_time,
        endTime=end_time
    )


def klines_to_dataframe(klines):
    """
    Kline 리스트를 Pandas DataFrame으로 변환 후,
    날짜(UTC) → KST 변환 및 컬럼명 지정.
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

    # Timestamp → Datetime 변환 (KST)
    df["open_time_kst"] = pd.to_datetime(df["open_time"], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')
    df["close_time_kst"] = pd.to_datetime(df["close_time"], unit='ms', utc=True).dt.tz_convert('Asia/Seoul')

    return df


def calculate_adx_with_pandas_ta(df, length=14):
    """
    pandas_ta를 사용하여 ADX 지표를 계산.
    high, low, close 컬럼 기준으로 ADX 컬럼을 DataFrame에 추가.
    """
    df.ta.adx(high="high", low="low", close="close", length=length, append=True)
    # append=True 옵션으로 df에 ["ADX_14", "DMP_14", "DMN_14"] 컬럼이 생성됨
    return df


def save_to_csv(df, filename):
    """DataFrame을 CSV로 저장합니다. 열 순서 및 헤더를 지정."""
    output_cols = [
        "open_time_kst", "open", "high", "low", "close", "volume",
        "close_time_kst", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume",
        "ADX_14", "DMP_14", "DMN_14"
    ]
    # 실제 존재하는 컬럼만 필터
    output_cols = [col for col in output_cols if col in df.columns]

    # float_format="%.2f" → CSV에서 모든 실수를 소수점 둘째자리까지 출력
    df[output_cols].to_csv(filename, index=False, encoding="utf-8", float_format="%.2f")


def main():
    # ---------------------
    # 사용자 지정 파라미터
    # ---------------------
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_1MINUTE  # 예: 1분봉
    adx_lookback = 14
    start_str = "2025-01-01"
    end_str = "2025-02-01"

    # 문자열 → datetime
    start_dt = datetime.datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_str, "%Y-%m-%d")

    # interval마다 몇 분인지 조회
    interval_minutes = INTERVAL_MINUTES_MAPPING[interval]

    # ADX 계산 위해 (adx_lookback)개 봉 * (interval_minutes)분만큼 추가 조회
    extra_minutes = adx_lookback * interval_minutes

    # 조회 시작 시간을 extra_minutes만큼 앞당김
    start_dt_for_fetch = start_dt - datetime.timedelta(minutes=extra_minutes)

    # datetime → timestamp(ms)
    start_ts_for_fetch = int(start_dt_for_fetch.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    # Binance API Client
    client = load_binance_client()

    # Kline 데이터 조회
    klines = fetch_futures_klines(
        client=client,
        symbol=symbol,
        interval=interval,
        start_time=start_ts_for_fetch,
        end_time=end_ts
    )

    # DataFrame 변환
    df = klines_to_dataframe(klines)

    # ADX 계산
    df = calculate_adx_with_pandas_ta(df, length=adx_lookback)

    # --------------------------------------
    # 1) 초기 NaN (지표 계산 전 봉) 제거
    # 2) 사용자 지정 기간 (start_str ~ end_str) 필터링
    # --------------------------------------

    # ADX_14 열에서 NaN을 가진 행 제거
    df.dropna(subset=["ADX_14"], inplace=True)

    # 기간 필터링
    mask = (df["open_time_kst"] >= pd.Timestamp(start_str, tz="Asia/Seoul")) & \
           (df["open_time_kst"] < pd.Timestamp(end_str, tz="Asia/Seoul"))
    df_filtered = df[mask].copy()

    # CSV 저장 (소수 둘째자리까지 반올림)
    filename = "btc_usdt_futures_klines_pandas_ta.csv"
    save_to_csv(df_filtered, filename)


if __name__ == "__main__":
    main()
