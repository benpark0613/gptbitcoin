import time
import datetime
import sys

from data_manager.tf_data_updater import update_csv


def main():
    # 간단한 테스트를 위해 심볼, 인터벌, 기간(UTC ms) 지정
    symbol = "BTCUSDT"
    interval = "15m"

    # 예: 현재 시각 기준으로 1일 전부터 지금까지
    end_ts = int(time.time() * 1000)  # 현재 UTC ms
    start_ts = end_ts - (24 * 60 * 60 * 1000)  # 1일 전

    csv_path = f"./data_fetcher/{symbol}_{interval}.csv"

    try:
        df = update_csv(symbol, interval, start_ts, end_ts, csv_path)
        print(f"Fetched/updated data_fetcher count: {len(df)}")
        if not df.empty:
            first_ts = df.iloc[0]['open_time']
            last_ts = df.iloc[-1]['open_time']
            print(f"Data range: {first_ts} ~ {last_ts}")
        else:
            print("No data_fetcher returned within the specified range.")
    except Exception as e:
        print(f"Error during update_csv: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
