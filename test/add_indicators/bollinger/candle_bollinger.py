import csv
import os
import datetime
import numpy as np
import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

# (1) 환경 변수 로드
load_dotenv()
api_key = os.getenv("BINANCE_ACCESS_KEY", "")
api_secret = os.getenv("BINANCE_SECRET_KEY", "")

client = Client(api_key, api_secret)

# (2) 바이낸스 interval 문자열별 밀리초 환산 딕셔너리
INTERVAL_TO_MILLISECONDS = {
    Client.KLINE_INTERVAL_1MINUTE: 1 * 60 * 1000,
    Client.KLINE_INTERVAL_3MINUTE: 3 * 60 * 1000,
    Client.KLINE_INTERVAL_5MINUTE: 5 * 60 * 1000,
    Client.KLINE_INTERVAL_15MINUTE: 15 * 60 * 1000,
    Client.KLINE_INTERVAL_30MINUTE: 30 * 60 * 1000,
    Client.KLINE_INTERVAL_1HOUR: 1 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_2HOUR: 2 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_4HOUR: 4 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_6HOUR: 6 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_8HOUR: 8 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_12HOUR: 12 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_1DAY: 24 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_3DAY: 3 * 24 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_1WEEK: 7 * 24 * 60 * 60 * 1000,
    Client.KLINE_INTERVAL_1MONTH: 30 * 24 * 60 * 60 * 1000
}

# (3) 날짜 범위 설정
START_DATE = "2025-01-01"
END_DATE = "2025-02-01"

def fetch_futures_klines(symbol, interval, start_date, end_date, extra_candles=20):
    """
    바이낸스 선물 시장 데이터를 가져옵니다.
    필요한 보조지표 계산을 위해 extra_candles만큼 이전 캔들을 추가로 가져옵니다.
    """
    # 날짜 -> timestamp 변환
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    # interval(예: '1h')에 따른 밀리초 계산
    interval_ms = INTERVAL_TO_MILLISECONDS.get(interval, None)
    if interval_ms is None:
        raise ValueError(f"Unsupported interval: {interval}")

    # 메인 구간 캔들 조회
    klines = client.futures_klines(
        symbol=symbol,
        interval=interval,
        startTime=start_ts,
        endTime=end_ts
    )

    # 보조지표 계산에 필요한 추가 캔들 (extra_candles) 만큼 앞서 가져옴
    if extra_candles > 0:
        extra_start_ts = start_ts - extra_candles * interval_ms
        extra_klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            startTime=extra_start_ts,
            endTime=start_ts
        )
        # 앞쪽에 추가 캔들 병합
        klines = extra_klines + klines

    return klines

def calculate_bollinger_bands(df, window=20, std_dev=2):
    """
    불린저 밴드를 계산하여 DataFrame에 추가합니다.
    - BBL_20_2.0 (Lower Band)
    - BBM_20_2.0 (Middle Band, 20-period SMA)
    - BBU_20_2.0 (Upper Band)
    """
    df["Close"] = df["Close"].astype(float)

    # 중심선(중앙밴드)
    df["BBM_20_2.0"] = df["Close"].rolling(window=window).mean()
    # 표준편차
    df["STD"] = df["Close"].rolling(window=window).std()

    # 상단/하단 밴드
    df["BBU_20_2.0"] = df["BBM_20_2.0"] + (df["STD"] * std_dev)
    df["BBL_20_2.0"] = df["BBM_20_2.0"] - (df["STD"] * std_dev)

    return df

def round_numeric_columns(df, columns, decimals=2):
    """
    주어진 컬럼들에 대해 float 변환 후 소수점 둘째 자리까지(round(2)) 반올림합니다.
    """
    for col in columns:
        df[col] = df[col].astype(float).round(decimals)

def save_to_csv(df, filename="btc_usdt_futures_klines.csv"):
    """
    DataFrame을 CSV 파일로 저장합니다.
    - BBL_20_2.0, BBM_20_2.0, BBU_20_2.0 컬럼 포함
    - 숫자 데이터는 모두 소수점 둘째 자리까지 표기
    """
    # 한국 표준시(KST)
    kst = datetime.timezone(datetime.timedelta(hours=9))

    with open(filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)

        # CSV 헤더
        writer.writerow([
            "Open Time (KST)", "Open", "High", "Low", "Close", "Volume",
            "Close Time (KST)", "Quote Asset Volume", "Number of Trades",
            "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
            "BBL_20_2.0", "BBM_20_2.0", "BBU_20_2.0"
        ])

        # 각 행 쓰기
        for _, row in df.iterrows():
            open_time_kst = datetime.datetime.fromtimestamp(
                float(row["Open Time"]) / 1000, tz=kst
            ).strftime('%Y-%m-%d %H:%M:%S')
            close_time_kst = datetime.datetime.fromtimestamp(
                float(row["Close Time"]) / 1000, tz=kst
            ).strftime('%Y-%m-%d %H:%M:%S')

            writer.writerow([
                open_time_kst,
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row["Volume"],
                close_time_kst,
                row["Quote Asset Volume"],
                row["Number of Trades"],  # 정수형(거래 횟수)이므로 소수점 처리 생략
                row["Taker Buy Base Asset Volume"],
                row["Taker Buy Quote Asset Volume"],
                row["BBL_20_2.0"],
                row["BBM_20_2.0"],
                row["BBU_20_2.0"]
            ])

def main():
    """
    메인 실행부:
    1. 데이터 조회
    2. 보조지표(볼린저 밴드) 계산
    3. 초기 extra_candles 제거
    4. 소수점 둘째 자리까지 반올림
    5. CSV 저장
    """
    symbol = "BTCUSDT"
    # 원하는 프레임으로 수정 가능: 예) Client.KLINE_INTERVAL_1MINUTE, Client.KLINE_INTERVAL_4HOUR, Client.KLINE_INTERVAL_1DAY 등
    interval = Client.KLINE_INTERVAL_1HOUR

    # (1) 데이터 조회 (보조지표 계산을 위해 extra_candles=20)
    raw_klines = fetch_futures_klines(symbol, interval, START_DATE, END_DATE, extra_candles=20)

    # (2) DataFrame 변환
    df = pd.DataFrame(raw_klines, columns=[
        "Open Time", "Open", "High", "Low", "Close", "Volume",
        "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"
    ])

    # (3) 볼린저 밴드 계산
    df = calculate_bollinger_bands(df, window=20, std_dev=2)

    # (4) 초반 extra_candles(20개) 제거
    df = df.iloc[20:].reset_index(drop=True)

    # (5) 필요한 숫자 컬럼들만 소수점 둘째 자리로 반올림
    float_cols = [
        "Open", "High", "Low", "Close", "Volume",
        "Quote Asset Volume", "Taker Buy Base Asset Volume",
        "Taker Buy Quote Asset Volume",
        "BBL_20_2.0", "BBM_20_2.0", "BBU_20_2.0"
    ]
    round_numeric_columns(df, float_cols, decimals=2)

    # (6) CSV로 저장
    save_to_csv(df)

if __name__ == "__main__":
    main()
