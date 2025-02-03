import logging
import os
import json
import csv
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from mbinance.client import Client
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
def create_folders():
    base_folder = "official_trump_coin_report"
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    today_date = datetime.now().strftime("%Y%m%d")
    date_folder = os.path.join(base_folder, today_date)
    if not os.path.exists(date_folder):
        os.makedirs(date_folder)

    current_time = datetime.now().strftime("%H%M%S")
    time_folder = os.path.join(date_folder, current_time)
    if not os.path.exists(time_folder):
        os.makedirs(time_folder)

    return time_folder

# ============== (3) CSV 저장 함수 ==============
def save_dataframe_to_csv(dataframe, filename, folder_path):
    file_path = os.path.join(folder_path, filename)
    if os.path.exists(file_path):
        logger.info(f"{filename} already exists. Skipping save.")
        return
    dataframe.to_csv(file_path, index=True)
    logger.info(f"{filename} saved to {folder_path}")

# ============== (4) OHLCV 데이터 저장 함수 ==============
def save_ohlcv_to_csv(symbol, interval, folder_path, limit=500):
    klines = binance_client.get_klines(symbol=symbol, interval=interval, limit=limit)

    data = []
    for row in klines:
        open_time = datetime.utcfromtimestamp(row[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        data.append([open_time, row[1], row[2], row[3], row[4], row[5]])

    df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume"])
    df = df.sort_values(by="open_time", ascending=False)

    csv_filename = os.path.join(folder_path, f"{symbol}_{interval}.csv")
    df.to_csv(csv_filename, index=False)
    logger.info(f"{symbol} OHLCV data saved to {csv_filename}")

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
    csv_path = os.path.join(folder_path, "google_news.csv")
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["title", "link", "snippet", "date", "source"])
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Google News data saved to {csv_path}")

# ============== (6) 공포 탐욕 지수(Fear & Greed Index) CSV 저장 함수 ==============
def save_fng_to_csv(fear_greed_index, folder_path):
    if not fear_greed_index:
        logger.warning("No fear_greed_index data available to save.")
        return

    csv_filename = os.path.join(folder_path, "fear_greed_index.csv")
    fieldnames = ["value", "value_classification", "timestamp", "time_until_update"]

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in fear_greed_index:
            writer.writerow(row)
    logger.info(f"Fear and Greed Index saved to {csv_filename}")

# ============== (7) 메인 실행부 ==============
if __name__ == "__main__":
    output_folder = create_folders()

    # Binance 잔고 및 호가 정보 수집
    account_info = binance_client.get_account()
    balances = [balance for balance in account_info["balances"] if float(balance["free"]) > 0 or float(balance["locked"]) > 0]
    orderbook = binance_client.get_order_book(symbol="TRUMPUSDT")

    # OHLCV 데이터 수집 및 저장
    save_ohlcv_to_csv("TRUMPUSDT", Client.KLINE_INTERVAL_5MINUTE, output_folder, limit=1000)
    save_ohlcv_to_csv("TRUMPUSDT", Client.KLINE_INTERVAL_15MINUTE, output_folder, limit=1000)
    save_ohlcv_to_csv("TRUMPUSDT", Client.KLINE_INTERVAL_1HOUR, output_folder, limit=1000)
    save_ohlcv_to_csv("TRUMPUSDT", Client.KLINE_INTERVAL_4HOUR, output_folder, limit=1000)
    save_ohlcv_to_csv("TRUMPUSDT", Client.KLINE_INTERVAL_1DAY, output_folder, limit=1000)

    # Balances, Orderbook 분리 저장
    balances_file = os.path.join(output_folder, "balances.json")
    orderbook_file = os.path.join(output_folder, "orderbook.json")

    with open(balances_file, "w", encoding="utf-8") as bf:
        json.dump(balances, bf, ensure_ascii=False, indent=4)
        logger.info(f"Balances saved to {balances_file}")

    with open(orderbook_file, "w", encoding="utf-8") as of:
        json.dump(orderbook, of, ensure_ascii=False, indent=4)
        logger.info(f"Orderbook saved to {orderbook_file}")

    # Google 뉴스 크롤링 및 저장
    query = "OFFICIAL TRUMP Coin"
    num_results = 10
    date_filter = 'w'
    news_data = get_news_data(query, num_results, date_filter)
    if news_data:
        save_news_to_csv(news_data, output_folder)
    else:
        logger.warning("No Google News data available.")

    # 공포 탐욕 지수 조회 및 저장
    fng_response = requests.get("https://api.alternative.me/fng/?limit=7").json()
    fear_greed_index = fng_response.get('data', [])
    save_fng_to_csv(fear_greed_index, output_folder)

    print("OFFICIAL TRUMP 코인 리포트가 정상적으로 완료되었습니다.")
