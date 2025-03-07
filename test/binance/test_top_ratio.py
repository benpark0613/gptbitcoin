"""
test_top_ratio.py

기능 요약:
1) 바이낸스 선물 API를 통해
   - Top Trader Long/Short Ratio (Positions)
   - BTCUSDT 15분봉 종가(Kline)
   데이터를 각각 조회 후 CSV 저장
2) 저장된 CSV 2개를 로드해, 한 화면 안에
   - 왼쪽 Y축: 종가 (파란색)
   - 오른쪽 Y축: 롱숏비율 (빨간색)
   으로 겹쳐 그린다.
"""

import requests
import csv
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt


def get_top_long_short_position_ratio(symbol, period, limit=30):
    """
    Top Trader Long/Short Ratio (Positions) API
    """
    url = "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit
    }
    resp = requests.get(url, params=params)
    data = resp.json()

    # timestamp -> KST
    for d in data:
        utc_dt = datetime.utcfromtimestamp(d["timestamp"] / 1000.0)
        kst_dt = utc_dt + timedelta(hours=9)
        d["timestamp_kst"] = kst_dt.strftime("%Y-%m-%d %H:%M:%S")

    return data


def get_futures_klines(symbol, interval, limit=30):
    """
    BTCUSDT 15분봉 선물 Kline 조회
    (close price 위주)
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    resp = requests.get(url, params=params)
    data = resp.json()

    result = []
    for item in data:
        open_time = int(item[0])          # UTC 기준 ms
        close_price = float(item[4])      # 종가
        utc_dt = datetime.utcfromtimestamp(open_time / 1000.0)
        kst_dt = utc_dt + timedelta(hours=9)

        result.append({
            "open_time": open_time,
            "open_time_kst": kst_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "close": close_price
        })

    return result


def save_to_csv(filename, data):
    """
    딕셔너리 리스트 -> CSV
    """
    if not data:
        print(f"No data to save for {filename}")
        return

    fieldnames = list(data[0].keys())
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    print(f"{filename} saved.")


def plot_combined(ratio_csv, kline_csv):
    """
    하나의 화면(figure)에
    - 왼쪽 Y축: 종가(파란색)
    - 오른쪽 Y축: 롱숏비율(빨간색)
    """
    # 1) 롱숏비 CSV
    df_ratio = pd.read_csv(ratio_csv)
    df_ratio['timestamp_kst'] = pd.to_datetime(df_ratio['timestamp_kst'])
    df_ratio.sort_values('timestamp_kst', inplace=True)
    df_ratio['longShortRatio'] = pd.to_numeric(df_ratio['longShortRatio'], errors='coerce')

    # 2) 종가 CSV
    df_kline = pd.read_csv(kline_csv)
    df_kline['open_time_kst'] = pd.to_datetime(df_kline['open_time_kst'])
    df_kline.sort_values('open_time_kst', inplace=True)
    df_kline['close'] = pd.to_numeric(df_kline['close'], errors='coerce')

    # 3) Figure & Axes
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # 종가 (왼쪽 Y축)
    ax1.set_xlabel("Timestamp (KST)")
    ax1.set_ylabel("Close Price", color='blue')
    ax1.plot(df_kline['open_time_kst'], df_kline['close'], color='blue', label="Close Price")
    ax1.tick_params(axis='y', labelcolor='blue')

    # 롱숏비율 (오른쪽 Y축)
    ax2 = ax1.twinx()
    ax2.set_ylabel("Long/Short Ratio", color='red')
    ax2.plot(df_ratio['timestamp_kst'], df_ratio['longShortRatio'], color='red', label="Long/Short Ratio")
    ax2.tick_params(axis='y', labelcolor='red')

    # 타이틀 및 레이아웃
    plt.title("BTCUSDT Close Price vs. Top Trader Long/Short Ratio")
    fig.tight_layout()
    plt.show()


def main():
    symbol = "BTCUSDT"
    period = "4h"
    limit = 500

    # 1) 롱숏비 (Positions)
    ratio_data = get_top_long_short_position_ratio(symbol, period, limit)
    ratio_csv = "topLongShortPositionRatio_15m.csv"
    save_to_csv(ratio_csv, ratio_data)

    # 2) 15분봉 종가
    kline_data = get_futures_klines(symbol, period, limit)
    kline_csv = "btcusdt_futures_kline_15m.csv"
    save_to_csv(kline_csv, kline_data)

    # 3) 단일 figure에 2축으로 그래프 그리기
    plot_combined(ratio_csv, kline_csv)


if __name__ == "__main__":
    main()
