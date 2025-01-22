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
def create_folders(base_folder_name="upbit_trade_report"):
    """
    기본 베이스 폴더(trade_report) / 날짜 폴더 / 시간 폴더 구조를 생성하고
    최종적으로 저장할 time_folder 경로를 반환
    """
    base_folder = base_folder_name
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
    """
    데이터프레임을 CSV 파일로 저장.
    """
    file_path = os.path.join(folder_path, filename)
    dataframe.to_csv(file_path, index=True, index_label="timestamp")  # 인덱스에 제목 추가
    logger.info(f"{filename} saved to {folder_path}")

# ============== (4) 공포 탐욕 지수 저장 함수 ==============
def save_fng_to_csv(fear_greed_index, folder_path):
    """
    공포 탐욕지수 리스트를 CSV 파일로 저장
    """
    if not fear_greed_index:
        logger.warning("No fear_greed_index data available to save.")
        return

    csv_filename = os.path.join(folder_path, "fear_greed_index.csv")

    # CSV 필드명: value, value_classification, timestamp, time_until_update 등
    fieldnames = ["value", "value_classification", "timestamp", "time_until_update"]

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in fear_greed_index:
            writer.writerow(row)

# ============== (4) 구글 뉴스 크롤링 관련 함수 ==============
def generate_url(query, start=0, date_filter='m'):
    """
    Google 뉴스 검색 URL 생성 함수.
    date_filter='m'일 경우 지난 1개월, 'w'는 1주, 'd'는 24시간.
    """
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
    """
    최대 num_results개를 모을 때까지 페이지를 넘겨가며 뉴스를 수집.
    지난 date_filter 기간을 제한(qdr:m => 1개월).
    """
    collected_results = []
    start = 0

    while len(collected_results) < num_results:
        url = generate_url(query, start, date_filter=date_filter)
        page_data = get_news_on_page(url)
        if not page_data:
            # 더 이상 결과가 없으면 중단
            break
        collected_results.extend(page_data)
        start += 10  # 다음 페이지로 이동

    return collected_results[:num_results]


def save_news_to_csv(data, folder_path):
    """
    구글 뉴스 데이터를 CSV로 저장
    """
    csv_path = os.path.join(folder_path, "google_news.csv")
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["title", "link", "snippet", "date", "source"]
        )
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Google News data saved to {csv_path}")


# ============== (5) 메인 실행부 ==============
if __name__ == "__main__":
    # 1) 폴더 생성
    output_folder = create_folders()

    # 2) Upbit 잔고 및 호가 정보 수집
    all_balances = upbit.get_balances()
    orderbook = pyupbit.get_orderbook("KRW-BTC")

    # 3) OHLCV 데이터 수집
    df_15min = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="minute15", count=2880)).sort_index(ascending=False)
    df_hourly = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=5000)).sort_index(ascending=False)
    df_4hour = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=2500)).sort_index(ascending=False)
    df_daily = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="day", count=1460)).sort_index(ascending=False)
    df_weekly = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="week", count=208)).sort_index(ascending=False)

    # 4) CSV 저장 (시세 데이터)
    save_dataframe_to_csv(df_15min, "15m.csv", output_folder)
    save_dataframe_to_csv(df_hourly, "1h.csv", output_folder)
    save_dataframe_to_csv(df_4hour, "4h.csv", output_folder)
    save_dataframe_to_csv(df_daily, "1d.csv", output_folder)
    save_dataframe_to_csv(df_weekly, "1w.csv", output_folder)

    # 5) Balances, Orderbook 분리 저장
    balances_file = os.path.join(output_folder, "balances.txt")
    orderbook_file = os.path.join(output_folder, "orderbook.txt")

    # Balances 파일 저장
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 현재 투자 상태 (Balances) ===\n")
        json.dump(all_balances, bf, ensure_ascii=False, indent=4)
        logger.info(f"Balances saved to {balances_file}")

    # Orderbook 파일 저장
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 오더북 데이터 (Orderbook) ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)
        logger.info(f"Orderbook saved to {orderbook_file}")

    # 6) 구글 뉴스 크롤링
    query = "BTC OR Bitcoin"
    num_results = 10
    date_filter = 'w'  # 'w' => 지난 1주간
    news_data = get_news_data(query, num_results, date_filter)
    save_news_to_csv(news_data, output_folder)

    # 7) 공포 탐욕지수 조회 및 저장
    fear_greed_index = requests.get("https://api.alternative.me/fng/?limit=7").json().get('data', [])
    save_fng_to_csv(fear_greed_index, output_folder)

    print("스크립트가 정상적으로 완료되었습니다.")