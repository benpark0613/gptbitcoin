import logging
import os
import json
import csv
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from binance.client import Client
from dotenv import load_dotenv

# ============== (1) 설정 및 준비 ==============
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_key = os.getenv("BINANCE_ACCESS_KEY")
api_secret = os.getenv("BINANCE_SECRET_KEY")
if not api_key or not api_secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")

binance_client = Client(api_key, api_secret)

# ============== (2) 폴더 생성 함수 ==============
def create_base_folder():
    """
    futures_BTCUSDT_report 폴더를 생성.
    """
    base_folder = "futures_BTCUSDT_report"
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    return base_folder

# ============== (3) 폴더 비우기(기존 파일 삭제) 함수 ==============
def clear_folder(folder_path):
    """
    해당 폴더 내 모든 파일을 삭제.
    하위 폴더는 없다고 가정.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            logger.info(f"Deleted existing file: {file_path}")

# ============== (4) OHLCV 데이터 저장 함수 ==============
def save_ohlcv_to_csv(symbol, interval, folder_path, limit=500):
    prefix = datetime.now().strftime("%y%m%d%H%M")
    klines = binance_client.futures_klines(symbol=symbol, interval=interval, limit=limit)

    data = []
    kst_timezone = pytz.timezone('Asia/Seoul')  # KST 시간대 정의

    for row in klines:
        # row[0] -> 밀리초 단위의 타임스탬프
        utc_timestamp = datetime.utcfromtimestamp(row[0] / 1000).replace(tzinfo=pytz.utc)  # UTC 기준
        kst_timestamp = utc_timestamp.astimezone(kst_timezone)  # KST 변환
        timestamp_str = kst_timestamp.strftime('%Y-%m-%d %H:%M:%S')  # KST 시간 문자열

        data.append([
            timestamp_str,
            row[1],  # open
            row[2],  # high
            row[3],  # low
            row[4],  # close
            row[5]   # volume
        ])

    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.sort_values(by="timestamp", ascending=False)

    csv_filename = f"{prefix}_{symbol}_{interval}.csv"
    csv_filepath = os.path.join(folder_path, csv_filename)
    df.to_csv(csv_filepath, index=False)
    logger.info(f"{symbol} OHLCV data saved to {csv_filepath}")

# ============== (5) 구글 뉴스 크롤링 관련 함수 ==============
def generate_url(query, start=0, date_filter='m'):
    base_url = "https://www.google.com/search"
    params = {
        "q": query,
        "gl": "us",
        "tbm": "nws",
        "start": start,
        "tbs": f"qdr:{date_filter}"
    }
    return f"{base_url}?{'&'.join([f'{key}={value}' for key, value in params.items()])}"

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
        link_el = el.find("a")
        link = link_el.get("href", "") if link_el else "N/A"

        title_el = el.select_one("div.MBeuO")
        title = title_el.get_text(strip=True) if title_el else "N/A"

        snippet_el = el.select_one(".GI74Re")
        snippet = snippet_el.get_text(strip=True) if snippet_el else "N/A"

        date_el = el.select_one(".LfVVr")
        date = date_el.get_text(strip=True) if date_el else "N/A"

        source_el = el.select_one(".NUnG9d span")
        source = source_el.get_text(strip=True) if source_el else "N/A"

        page_results.append({
            "link": link,
            "title": title,
            "snippet": snippet,
            "date": date,
            "source": source
        })
    return page_results

def get_news_data(query, num_results=10, date_filter='m'):
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
    prefix = datetime.now().strftime("%y%m%d%H%M")
    csv_filename = f"{prefix}_google_news.csv"
    csv_path = os.path.join(folder_path, csv_filename)

    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["title", "link", "snippet", "date", "source"])
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Google News data saved to {csv_path}")

# ============== (6) 공포 탐욕 지수(Fear & Greed Index) CSV 저장 함수 ==============
def save_fng_to_csv(fear_greed_index, folder_path):
    prefix = datetime.now().strftime("%y%m%d%H%M")
    csv_filename = f"{prefix}_fear_greed_index.csv"
    csv_filepath = os.path.join(folder_path, csv_filename)

    if not fear_greed_index:
        logger.warning("No fear_greed_index data available to save.")
        return

    fieldnames = ["value", "value_classification", "timestamp", "time_until_update"]
    with open(csv_filepath, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in fear_greed_index:
            writer.writerow(row)
    logger.info(f"Fear and Greed Index saved to {csv_filepath}")

# ============== (7) 메인 실행부 ==============
if __name__ == "__main__":
    # 1) 폴더 생성 (없는 경우)
    base_folder = create_base_folder()

    # 2) 기존 파일 삭제
    clear_folder(base_folder)

    # 3) 접두사(YYMMDDHHMM) 미리 정의
    prefix = datetime.now().strftime("%y%m%d%H%M")

    # 4) 계정/오더북/차트 등 수집
    account_info = binance_client.futures_account_balance()
    balances = account_info
    orderbook = binance_client.futures_order_book(symbol="BTCUSDT")

    # 5) OHLCV 데이터 저장
    # === 워뇨띠 스타일에 맞춰 1분봉 추가 ===
    save_ohlcv_to_csv("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, base_folder, limit=1000)  # 1분봉
    save_ohlcv_to_csv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, base_folder, limit=1000)
    save_ohlcv_to_csv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, base_folder, limit=1000)
    save_ohlcv_to_csv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, base_folder, limit=1000)
    save_ohlcv_to_csv("BTCUSDT", Client.KLINE_INTERVAL_4HOUR, base_folder, limit=1000)
    save_ohlcv_to_csv("BTCUSDT", Client.KLINE_INTERVAL_1DAY, base_folder, limit=1000)

    # 6) Balances, Orderbook 저장 (JSON)
    balances_file = os.path.join(base_folder, f"{prefix}_balances.json")
    with open(balances_file, "w", encoding="utf-8") as bf:
        json.dump(balances, bf, ensure_ascii=False, indent=4)
    logger.info(f"Futures balances saved to {balances_file}")

    orderbook_file = os.path.join(base_folder, f"{prefix}_orderbook.json")
    with open(orderbook_file, "w", encoding="utf-8") as of:
        json.dump(orderbook, of, ensure_ascii=False, indent=4)
    logger.info(f"Futures orderbook saved to {orderbook_file}")

    # 7) 구글 뉴스
    query = "Bitcoin"
    num_results = 10
    date_filter = 'w'
    news_data = get_news_data(query, num_results, date_filter)
    if news_data:
        save_news_to_csv(news_data, base_folder)
    else:
        logger.warning("No Google News data available.")

    # 8) 공포 탐욕 지수
    fng_response = requests.get("https://api.alternative.me/fng/?limit=7").json()
    fear_greed_index = fng_response.get('data', [])
    save_fng_to_csv(fear_greed_index, base_folder)

    print("BTCUSDT 선물 리포트가 정상적으로 완료되었습니다.")