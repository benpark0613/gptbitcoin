# get_binance_ohlcv.py

import os
import shutil
import pandas as pd
from datetime import datetime, timezone
from binance.client import Client
from dotenv import load_dotenv


def datetime_to_milliseconds(dt_str):
    """
    날짜 문자열(YYYY-MM-DD 혹은 YYYY-MM-DD HH:MM:SS)을
    UTC 타임존 기준 밀리초 단위로 변환합니다.
    """
    if len(dt_str.strip()) == 10:
        dt_str += " 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms, limit=1500):
    """
    바이낸스 선물 OHLCV 데이터를, 한 번에 최대 limit개씩 끊어가며
    (start_ms ~ end_ms) 구간 전체를 모두 수집합니다.
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

    # 수집된 데이터를 DataFrame으로 변환
    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    return df


def clean_klines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kline 단위의 이상치(Outlier)나 엉뚱한 데이터(0,음수 가격/거래량 등)를
    간단히 필터링하거나 제거하는 예시 함수.

    - 실제로는 tick 데이터 기반으로 Brownlees & Gallo(2006)같은
      정교한 알고리즘을 적용 가능하지만,
      여기서는 기본적인 검증 로직만 구현.
    """
    # 1) 음수 혹은 0 가격/거래량 제거
    #    (원래 Kline 데이터에서 0이 나오긴 어려우나 혹시 모를 예외 처리)
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df = df[df[col] > 0]

    # 2) 이상한 High/Low 관계 정리
    #    통상적으로 low <= open/close/high, high >= open/close/low
    #    open > high 혹은 open < low이면 비정상으로 보고 제거
    #    (Kline에서는 이론상 잘 발생하지 않지만 혹시 모를 데이터 오류)
    df = df[df["high"] >= df["low"]]
    df = df[df["high"] >= df["open"]]
    df = df[df["high"] >= df["close"]]
    df = df[df["low"] <= df["open"]]
    df = df[df["low"] <= df["close"]]

    # 3) (선택) 극단치 제거
    #    예: volume이 너무 큰 행을 제거한다든지
    #    아래는 단순 예시로 전체 volume의 상위 0.1% 행을 제거
    #    실제론 더 정교한 방법(rollingsigma 등) 사용할 수 있음
    volume_threshold = df["volume"].quantile(0.999)  # 상위 0.1%
    df = df[df["volume"] <= volume_threshold]

    # 필요하다면 open-close 스프레드가 지나치게 큰 행도 제거 가능
    # (예: 1분 Kline인데 몇 천 % 변동 등)
    # spread = abs(df["close"] - df["open"]) / df["open"]
    # extreme_spread_threshold = 5.0  # 예: 500% 이상 변동이면 제거
    # df = df[spread < extreme_spread_threshold]

    return df


def main(
        symbol="BTCUSDT",
        intervals=None,
        start_date="2024-01-01",
        end_date="2025-01-01",
        save_folder="test_result",
        return_data=False  # True이면 메모리 내 DataFrame을 dictionary로 반환
):
    """
    1) 환경 변수 로드 및 Binance Client 초기화
    2) 날짜 범위를 밀리초로 변환
    3) 타임프레임별로 데이터 가져와 -> clean_klines()로 이상치 제거
    4) CSV 저장하거나(return_data=False), 메모리에 반환(return_data=True)
    """
    if intervals is None:
        intervals = ["15m"]
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    client = Client(api_key, api_secret)

    start_ms = datetime_to_milliseconds(start_date)
    end_ms = datetime_to_milliseconds(end_date)

    result_dict = {}
    if not return_data:
        if os.path.exists(save_folder):
            shutil.rmtree(save_folder)
        os.makedirs(save_folder, exist_ok=True)

    for interval in intervals:
        # (1) Kline 수집
        df = fetch_futures_ohlcv(client, symbol, interval, start_ms, end_ms)

        # (2) open_time을 DateTime으로 변환 후 인덱스로 설정
        df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time_dt", inplace=True)

        # (3) 불필요한 컬럼 제거
        drop_cols = [
            "open_time", "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ]
        df.drop(columns=drop_cols, inplace=True, errors="ignore")
        # (4) 컬럼 순서 정리
        df = df[["open", "high", "low", "close", "volume"]]

        # (5) 숫자형 변환
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].apply(
            pd.to_numeric, errors='coerce')

        # (6) 이상치(Outlier) 제거
        df = clean_klines(df)

        if return_data:
            result_dict[interval] = df
        else:
            file_name = f"{symbol}_{interval}.csv"
            file_path = os.path.join(save_folder, file_name)
            df.to_csv(file_path, encoding="utf-8", index=True)
            print(f"[{symbol} - {interval}] : {len(df)}건 데이터 저장(정제 후) → {file_path}")

    if return_data:
        return result_dict


if __name__ == "__main__":
    # 예시: 1개월 치 15분봉 데이터 수집 + 이상치 처리 → CSV 저장
    main(
        symbol="BTCUSDT",
        intervals=["15m"],
        start_date="2024-01-01",
        end_date="2024-02-01",
        save_folder="test_result"
    )
