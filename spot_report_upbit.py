import logging
import os
import json
import csv
import requests
import pyupbit
from bs4 import BeautifulSoup
from ta.utils import dropna
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re

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
def create_folders(base_folder_name="spot_BTCKRW_report"):
    """
    base_folder_name 폴더가 없으면 생성하고,
    파일 저장 전에 해당 폴더 내 기존 파일(또는 비어있는 폴더)을 삭제합니다.
    """
    if not os.path.exists(base_folder_name):
        os.makedirs(base_folder_name)
    else:
        for file in os.listdir(base_folder_name):
            file_path = os.path.join(base_folder_name, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)  # 폴더가 비어있을 경우만 삭제 가능
    return base_folder_name


# ============== (3) CSV 저장 함수들 ==============
def save_dataframe_to_csv(dataframe, full_filename, folder_path):
    """
    DataFrame을 CSV 파일로 저장합니다.
    full_filename은 이미 완성된 파일명을 전달받아 접두사를 추가하지 않습니다.
    """
    file_path = os.path.join(folder_path, full_filename)
    dataframe.to_csv(file_path, index=True, index_label="timestamp")
    logger.info(f"{full_filename} saved to {folder_path}")


def save_fng_to_csv(fear_greed_index, folder_path, timestamp_prefix=None):
    """
    공포 탐욕 지수 리스트(fear_greed_index)를 CSV 파일로 저장합니다.
    timestamp_prefix 매개변수를 통해 동일한 타임스탬프를 모든 파일명에 적용할 수 있습니다.
    """
    if not fear_greed_index:
        logger.warning("No fear_greed_index data available to save.")
        return

    # timestamp_prefix가 없다면 현재 시·분을 생성 (YYYYMMDDHHmm 형태)
    if not timestamp_prefix:
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


# ============== (4) 날짜 파싱 함수 ==============
def parse_date_str(date_str):
    """
    구글 뉴스에서 흔히 볼 수 있는 날짜 문자열 예시:
    '3 hours ago', '2 days ago', 'Jan 22, 2025' 등을 단순 처리합니다.
    """
    if not date_str:
        return datetime.now()

    date_str = date_str.strip().lower()

    # (4-1) X minutes/hours/days ago
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

    # (4-2) 'Jan 22, 2025' 형식 가정
    try:
        return datetime.strptime(date_str, "%b %d, %Y")
    except ValueError:
        pass

    # (4-3) 파싱 안 되면 현재 시각 반환
    return datetime.now()


# ============== (5) 구글 뉴스 크롤링 ==============
def generate_url(query, start=0, date_filter=None):
    base_url = "https://www.google.com/search"
    params = {
        "q": query,
        "gl": "us",
        "tbm": "nws",
        "start": start
    }
    if date_filter:
        params["tbs"] = f"qdr:{date_filter}"
    query_string = "&".join([f"{key}={value}" for key, value in params.items()])
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


def save_news_to_csv(data, folder_path, timestamp_prefix=None):
    """
    구글 뉴스 정보를 CSV로 저장합니다.
    timestamp_prefix 매개변수를 통해 동일한 타임스탬프를 적용할 수 있습니다.
    """
    if not timestamp_prefix:
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


def retrieve_and_save_google_news(output_folder, query="Bitcoin", total_results=10, top_n=10, timestamp_prefix=None):
    """
    구글 뉴스에서 데이터를 수집하고, 상위 top_n건을 CSV로 저장합니다.
    """
    news_data_raw = get_news_data(query, num_results=total_results)
    for item in news_data_raw:
        item["parsed_dt"] = parse_date_str(item["date"])

    # 날짜 기준으로 내림차순 정렬
    news_sorted = sorted(news_data_raw, key=lambda x: x["parsed_dt"], reverse=True)
    latest_n_news = news_sorted[:top_n]

    save_news_to_csv(latest_n_news, output_folder, timestamp_prefix=timestamp_prefix)


# ============== (6) 미체결 주문 조회 함수 ==============
def save_open_orders_to_txt(folder_path, timestamp_prefix=None):
    """
    KRW-BTC 종목 미체결 주문을 조회해 TXT로 저장합니다.
    """
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    try:
        open_orders = upbit.get_order("KRW-BTC")
        if not open_orders:
            logger.info("No open orders found.")
            return

        filename_with_prefix = f"{timestamp_prefix}_open_orders.txt"
        orders_file = os.path.join(folder_path, filename_with_prefix)

        with open(orders_file, "w", encoding="utf-8") as file:
            file.write("=== 미체결 주문 (Open Orders) ===\n")
            json.dump(open_orders, file, ensure_ascii=False, indent=4)

        logger.info(f"{filename_with_prefix} saved to {folder_path}")
    except Exception as e:
        logger.error(f"Error fetching or saving open orders: {e}")


# ============== (7) [통합] 잔고와 오더북 저장 함수 ==============
def save_balance_and_orderbook(balances, orderbook, folder_path, timestamp_prefix=None):
    """
    Upbit 잔고(balances)와 오더북(orderbook)을 한 번에 저장합니다.
    """
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    # 잔고 저장
    balances_filename = f"{timestamp_prefix}_balances.txt"
    balances_file = os.path.join(folder_path, balances_filename)
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 현재 투자 상태 (Balances) ===\n")
        json.dump(balances, bf, ensure_ascii=False, indent=4)

    # 오더북 저장
    orderbook_filename = f"{timestamp_prefix}_orderbook.txt"
    orderbook_file = os.path.join(folder_path, orderbook_filename)
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 오더북 데이터 (Orderbook) ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)

    logger.info(f"{balances_filename} / {orderbook_filename} saved to {folder_path}")


# ============== (7-1) OHLCV 여러 interval에 대해 한꺼번에 조회 + CSV 저장 함수 ==============
def fetch_and_save_ohlcv(symbol, output_folder, intervals, timestamp_prefix=None):
    """
    intervals 예시:
      [
        {"interval": "minute15",  "count": 5000},
        {"interval": "minute60",  "count": 5000},
        {"interval": "minute240", "count": 2500},
        {"interval": "day",       "count": 1460}
      ]
    """
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%Y%m%d%H%M")

    for setting in intervals:
        interval_name = setting.get("interval", "day")
        count_value = setting.get("count", 200)

        csv_filename = f"{timestamp_prefix}_{interval_name}.csv"

        # PyUpbit로 OHLCV 조회
        df = dropna(
            pyupbit.get_ohlcv(symbol, interval=interval_name, count=count_value)
        ).sort_index(ascending=False)

        # CSV 저장
        save_dataframe_to_csv(df, csv_filename, output_folder)


# ============== (8) 메인 실행부 ==============
if __name__ == "__main__":
    # (8-1) 공통 타임스탬프 설정 (예: 202501241630 형태)
    common_timestamp_prefix = datetime.now().strftime("%Y%m%d%H%M")

    # (8-2) 폴더 생성
    output_folder = create_folders("spot_BTCKRW_report")

    # (8-3) Upbit 잔고 및 호가 정보 수집 + 파일 저장(통합 함수 사용)
    all_balances = upbit.get_balances()
    orderbook = pyupbit.get_orderbook("KRW-BTC")
    save_balance_and_orderbook(
        all_balances,
        orderbook,
        output_folder,
        timestamp_prefix=common_timestamp_prefix
    )

    # (8-4) 원하는 interval 정의
    my_intervals = [
        {"interval": "minute15",  "count": 2500},
        {"interval": "minute60",  "count": 5000},
        {"interval": "minute240", "count": 2500},
        {"interval": "day",       "count": 1460}
    ]

    # (8-5) OHLCV 데이터 수집 + CSV 저장 (여러 interval 한 번에)
    fetch_and_save_ohlcv(
        "KRW-BTC",
        output_folder,
        my_intervals,
        timestamp_prefix=common_timestamp_prefix
    )

    # (8-6) 미체결 주문 조회 및 저장
    save_open_orders_to_txt(
        output_folder,
        timestamp_prefix=common_timestamp_prefix
    )

    # (8-7) 구글 뉴스 크롤링 & CSV 저장
    retrieve_and_save_google_news(
        output_folder,
        query="Bitcoin",
        total_results=50,
        top_n=10,
        timestamp_prefix=common_timestamp_prefix
    )

    # (8-8) 공포 탐욕 지수 조회 및 저장
    fear_greed_index = requests.get("https://api.alternative.me/fng/?limit=7").json().get("data", [])
    save_fng_to_csv(
        fear_greed_index,
        output_folder,
        timestamp_prefix=common_timestamp_prefix
    )

    logger.info("스팟 리포트 스크립트가 정상적으로 완료되었습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")
