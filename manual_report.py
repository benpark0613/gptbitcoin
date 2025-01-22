import logging
import os
import json
import csv
import requests
import pyupbit
from bs4 import BeautifulSoup
from ta.utils import dropna
from datetime import datetime
from dotenv import load_dotenv

# ============== (1) 설정 및 준비 ==============
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")
upbit = pyupbit.Upbit(access, secret)


# ============== (2) 폴더 생성 함수 ==============
def create_folders():
    base_folder = "trade_report"
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


# ============== (3) 구글 뉴스 크롤링 관련 함수들 ==============
def generate_url(query, start=0, date_filter='m'):
    base_url = "https://www.google.com/search"
    params = {
        "q": query,
        "gl": "us",     # 미국 기반 검색
        "tbm": "nws",   # 뉴스 검색
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


# ============== (4) 뉴스 CSV 저장 함수 ==============
def save_news_to_csv(data, folder_path):
    csv_path = os.path.join(folder_path, "news_results.csv")
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["title", "link", "snippet", "date", "source"]
        )
        writer.writeheader()
        writer.writerows(data)


# ============== (5) 메인 실행부 ==============
if __name__ == "__main__":
    # 1) 폴더 생성
    output_folder = create_folders()
    print(f"결과 폴더가 생성되었습니다: {output_folder}")

    # 2) 구글 뉴스 데이터 수집 (BTC OR Bitcoin, 지난 1주)
    news_data = get_news_data(query="BTC OR Bitcoin", num_results=10, date_filter='w')
    save_news_to_csv(news_data, output_folder)
    print(f"뉴스 크롤링 데이터가 저장되었습니다: {os.path.join(output_folder, 'news_results.csv')}")

    # 3) Upbit 잔고 및 호가 정보 수집
    all_balances = upbit.get_balances()
    orderbook = pyupbit.get_orderbook("KRW-BTC")

    from ta.utils import dropna

    # 4) OHLCV 데이터 (15분봉, 1시간봉, 4시간봉)
    df_15min = pyupbit.get_ohlcv("KRW-BTC", interval="minute15", count=2880)
    df_15min = dropna(df_15min).sort_index(ascending=False)

    df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=10000)
    df_hourly = dropna(df_hourly).sort_index(ascending=False)

    df_4hour = pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=2500)
    df_4hour = dropna(df_4hour).sort_index(ascending=False)

    # 5) 타임스탬프
    timestamp = datetime.now().strftime("%Y%m%d%H%M")

    # 6) CSV 저장 (시세 데이터)
    csv_15min = os.path.join(output_folder, f"{timestamp}_15min.csv")
    df_15min.to_csv(csv_15min, index=True)

    csv_hourly = os.path.join(output_folder, f"{timestamp}_hourly.csv")
    df_hourly.to_csv(csv_hourly, index=True)

    csv_4hour = os.path.join(output_folder, f"{timestamp}_4hour.csv")
    df_4hour.to_csv(csv_4hour, index=True)

    print(f"시세 CSV 파일들이 저장되었습니다: {csv_15min}, {csv_hourly}, {csv_4hour}")

    # 7) Balances, Orderbook 분리 저장
    balances_file = os.path.join(output_folder, f"{timestamp}_balances.txt")
    orderbook_file = os.path.join(output_folder, f"{timestamp}_orderbook.txt")

    # Balances 파일
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 현재 투자 상태 (Balances) ===\n")
        json.dump(all_balances, bf, ensure_ascii=False, indent=4)

    # Orderbook 파일
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 오더북 데이터 (Orderbook) ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)

    print(f"Balances와 Orderbook 데이터가 각각 분리되어 저장되었습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")