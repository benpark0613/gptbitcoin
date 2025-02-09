import csv
import os

from binance.client import Client
import datetime

from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("BINANCE_ACCESS_KEY", "")
api_secret = os.getenv("BINANCE_SECRET_KEY", "")

api_key = api_key
api_secret = api_secret
client = Client(api_key, api_secret)

# 조회할 날짜 범위 설정 (YYYY-MM-DD)
start_str = "2025-01-01"
end_str = "2025-02-01"

# 날짜 문자열을 datetime 객체로 변환
start_dt = datetime.datetime.strptime(start_str, "%Y-%m-%d")
end_dt = datetime.datetime.strptime(end_str, "%Y-%m-%d")

# Binance API에서는 timestamp를 밀리초 단위로 요구합니다.
start_ts = int(start_dt.timestamp() * 1000)
end_ts = int(end_dt.timestamp() * 1000)

# futures_klines 함수를 사용해 1시간 간격 데이터 조회
klines = client.futures_klines(
    symbol='BTCUSDT',
    interval=Client.KLINE_INTERVAL_1HOUR,
    startTime=start_ts,
    endTime=end_ts
)

# 한국 표준시(KST, UTC+9) 타임존 정의
kst = datetime.timezone(datetime.timedelta(hours=9))

# CSV 파일로 저장 (파일명: btc_usdt_futures_klines.csv)
with open("../binance_backtest/btc_usdt_futures_klines.csv", mode="w", newline="", encoding="utf-8") as csv_file:
    writer = csv.writer(csv_file)

    # CSV 파일 헤더 작성
    writer.writerow([
        "Open Time (KST)", "Open", "High", "Low", "Close", "Volume",
        "Close Time (KST)", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"
    ])

    # 각 캔들스틱 데이터에 대해 시간 변환 후 CSV에 작성
    for kline in klines:
        # kline 데이터 구조:
        # [0] Open time, [1] Open, [2] High, [3] Low, [4] Close, [5] Volume,
        # [6] Close time, [7] Quote Asset Volume, [8] Number of Trades,
        # [9] Taker Buy Base Asset Volume, [10] Taker Buy Quote Asset Volume, [11] Ignore

        # 밀리초 단위의 시간을 초 단위로 변환하여 한국 시간으로 변경
        open_time_kst = datetime.datetime.fromtimestamp(kline[0] / 1000, tz=kst).strftime('%Y-%m-%d %H:%M:%S')
        close_time_kst = datetime.datetime.fromtimestamp(kline[6] / 1000, tz=kst).strftime('%Y-%m-%d %H:%M:%S')

        # CSV에 기록할 행 구성
        row = [open_time_kst] + kline[1:6] + [close_time_kst] + kline[7:12]
        writer.writerow(row)