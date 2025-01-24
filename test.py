import logging
import os
import json
import csv
import requests
import re
import pytz
import pandas as pd

from bs4 import BeautifulSoup
from ta.utils import dropna
from datetime import datetime, timedelta
from dotenv import load_dotenv

# python-binance
from binance.client import Client

# ============== (1) 설정 및 준비 ==============
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

access = os.getenv("BINANCE_ACCESS_KEY")
secret = os.getenv("BINANCE_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")

client = Client(access, secret)

# ============== (2) 폴더 생성 함수 ==============
def create_folders(base_folder_name="futures_BTCUSDT_report"):
    if not os.path.exists(base_folder_name):
        os.makedirs(base_folder_name)
    else:
        for file in os.listdir(base_folder_name):
            file_path = os.path.join(base_folder_name, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
    return base_folder_name

# ============== (3) CSV 저장 함수들 ==============
def save_dataframe_to_csv(dataframe, full_filename, folder_path):
    file_path = os.path.join(folder_path, full_filename)
    dataframe.to_csv(file_path, index=True, index_label="timestamp")
    logger.info(f"{full_filename} saved to {folder_path}")

def save_fng_to_csv(fear_greed_index, folder_path):
    if not fear_greed_index:
        logger.warning("No fear_greed_index data available to save.")
        return

    timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")
    filename_with_prefix = f"{timestamp_prefix}_fear_greed_index.csv"
    csv_filename = os.path.join(folder_path, filename_with_prefix)

    fieldnames = ["value", "value_classification", "timestamp", "time_until_update"]
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in fear_greed_index:
            writer.writerow(row)
    logger.info(f"{filename_with_prefix} saved to {folder_path}")

# ============== (4) 날짜 파싱 함수 (뉴스용) ==============
def parse_date_str(date_str):
    if not date_str:
        return datetime.now()

    date_str = date_str.strip().lower()
    match = re.match(r"(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago", date_str)
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        now = datetime.now()

        if "minute" in unit:
            return now - timedelta(minutes=amount)
        elif "hour" in unit:
            return now - timedelta(hours=amount)
        elif "day" in unit:
            return now - timedelta(days=amount)

    try:
        return datetime.strptime(date_str, "%b %d, %Y")
    except ValueError:
        pass

    return datetime.now()

# ============== (5) 구글 뉴스 크롤링 ==============
def generate_url(query, start=0, date_filter=None):
    base_url = "https://www.google.com/search"
    params = {"q": query, "gl": "us", "tbm": "nws", "start": start}
    if date_filter:
        params["tbs"] = f"qdr:{date_filter}"
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{query_string}"

def get_news_on_page(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    articles = soup.select("div.SoaBEf")
    page_results = []
    for el in articles:
        title_el = el.select_one("div.MBeuO")
        snippet_el = el.select_one(".GI74Re")
        date_el = el.select_one(".LfVVr")
        source_el = el.select_one(".NUnG9d span")

        title = title_el.get_text(strip=True) if title_el else "N/A"
        snippet = snippet_el.get_text(strip=True) if snippet_el else "N/A"
        date_str = date_el.get_text(strip=True) if date_el else "N/A"
        source = source_el.get_text(strip=True) if source_el else "N/A"

        page_results.append({
            "title": title,
            "snippet": snippet,
            "date": date_str,
            "source": source
        })
    return page_results

def get_news_data(query, num_results=10, date_filter=None):
    collected_results = []
    start = 0
    while len(collected_results) < num_results:
        url = generate_url(query, start, date_filter=date_filter)
        page_data = get_news_on_page(url)
        if not page_data:
            break
        collected_results.extend(page_data)
        start += 10
    return collected_results[:num_results]

def save_news_to_csv(data, folder_path):
    timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")
    filename_with_prefix = f"{timestamp_prefix}_google_news.csv"
    csv_path = os.path.join(folder_path, filename_with_prefix)

    for item in data:
        if "parsed_dt" in item:
            del item["parsed_dt"]

    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["title", "snippet", "date", "source"])
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"{filename_with_prefix} saved to {folder_path}")

def retrieve_and_save_google_news(output_folder, query="Bitcoin", total_results=50, top_n=10):
    news_data_raw = get_news_data(query, num_results=total_results)
    for item in news_data_raw:
        item["parsed_dt"] = parse_date_str(item["date"])
    news_sorted = sorted(news_data_raw, key=lambda x: x["parsed_dt"], reverse=True)
    latest_n_news = news_sorted[:top_n]
    save_news_to_csv(latest_n_news, output_folder)

# ============== (6) 바이낸스 선물: 잔고 & 오더북 조회 함수 예시 ==============
def fetch_futures_balance():
    try:
        futures_balance = client.futures_account_balance()
        logger.info("선물 잔고 정보를 성공적으로 조회했습니다.")
        return futures_balance
    except Exception as e:
        logger.error(f"잔고 조회 에러: {e}")
        return []

def fetch_futures_orderbook(symbol="BTCUSDT", limit=20):
    try:
        orderbook = client.futures_order_book(symbol=symbol, limit=limit)
        logger.info(f"'{symbol}' 선물 오더북을 성공적으로 조회했습니다.")
        return orderbook
    except Exception as e:
        logger.error(f"오더북 조회 에러: {e}")
        return {}

# ============== (7) [통합] 잔고와 오더북 저장 함수 ==============
def save_balance_and_orderbook(balances, orderbook, folder_path):
    timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    balances_filename = f"{timestamp_prefix}_balances.txt"
    balances_file = os.path.join(folder_path, balances_filename)
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 바이낸스 선물 잔고 ===\n")
        json.dump(balances, bf, ensure_ascii=False, indent=4)

    orderbook_filename = f"{timestamp_prefix}_orderbook.txt"
    orderbook_file = os.path.join(folder_path, orderbook_filename)
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 바이낸스 선물 오더북 ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)

    logger.info(f"{balances_filename} / {orderbook_filename} saved to {folder_path}")

# ============== (7-1) OHLCV 여러 interval에 대해 한꺼번에 조회 + CSV 저장 (Binance 예시) ==============
def fetch_and_save_ohlcv(symbol, output_folder, intervals):
    """
    Binance 선물 K라인을 KST로 변환하여 저장합니다.
    intervals 예시: [{"interval": "1m", "limit": 50}, ...]
    """
    import pandas as pd
    import pytz

    kst = pytz.timezone('Asia/Seoul')

    for setting in intervals:
        interval_name = setting.get("interval", "1m")
        limit_value = setting.get("limit", 500)

        # YYYYMMDD_interval.csv 예: 20250124_1m.csv
        today_str = datetime.now().strftime("%Y%m%d")
        csv_filename = f"{today_str}_{interval_name}.csv"

        try:
            klines = client.futures_klines(symbol=symbol, interval=interval_name, limit=limit_value)
            # klines 예시:
            # [
            #   [ 1499040000000, "0.01634790", "0.80000000", ... ],
            #   ...
            # ]

            df = pd.DataFrame(klines, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_base_volume", "taker_quote_volume", "ignore"
            ])

            # 1) UTC 기준 datetime 변환
            #    => to_datetime(..., utc=True)를 사용해 'UTC'를 명시
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

            # 2) UTC → KST 변환
            df["open_time"] = df["open_time"].dt.tz_convert(kst)

            # 3) 인덱스로 설정
            df.set_index("open_time", inplace=True)

            # 4) 정렬: 최신이 위로 오게
            df.sort_index(ascending=False, inplace=True)

            # CSV 저장
            save_dataframe_to_csv(df, csv_filename, output_folder)

        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol} - {interval_name}: {e}")

# ============== (8) 메인 실행부 ==============
if __name__ == "__main__":
    output_folder = create_folders("futures_BTCUSDT_report")

    # 1) 잔고 & 오더북 수집 및 저장
    futures_balance = fetch_futures_balance()
    orderbook = fetch_futures_orderbook(symbol="BTCUSDT", limit=20)
    save_balance_and_orderbook(futures_balance, orderbook, output_folder)

    # 2) OHLCV intervals 정의
    my_intervals = [
        {"interval": Client.KLINE_INTERVAL_1MINUTE, "limit": 50},  # 1분봉
        {"interval": Client.KLINE_INTERVAL_5MINUTE, "limit": 50},  # 5분봉
        {"interval": Client.KLINE_INTERVAL_15MINUTE, "limit": 50},  # 15분봉
        {"interval": Client.KLINE_INTERVAL_1HOUR, "limit": 50},  # 1시간봉
        {"interval": Client.KLINE_INTERVAL_4HOUR, "limit": 50},  # 4시간봉
        {"interval": Client.KLINE_INTERVAL_1DAY, "limit": 50}  # 1일봉
    ]
    # 3) OHLCV 수집 + CSV 저장 (KST 변환 반영)
    fetch_and_save_ohlcv("BTCUSDT", output_folder, my_intervals)

    # 4) 구글 뉴스 크롤링
    retrieve_and_save_google_news(
        output_folder,
        query="Bitcoin",
        total_results=30,
        top_n=10
    )

    # 5) 공포 탐욕 지수 조회 및 저장
    fear_greed_index = requests.get("https://api.alternative.me/fng/?limit=7").json().get("data", [])
    save_fng_to_csv(fear_greed_index, output_folder)

    logger.info("바이낸스 선물 리포트 스크립트가 정상적으로 완료되었습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")
