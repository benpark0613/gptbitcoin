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
    base_folder_name 하위에 바로 저장되도록 하고,
    파일 저장 전에 기존 파일들을 삭제합니다.
    """
    base_folder = base_folder_name
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    else:
        # 기존 파일 삭제
        for file in os.listdir(base_folder):
            file_path = os.path.join(base_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)  # 폴더가 비어있을 경우만 삭제 가능
    return base_folder


# ============== (3) CSV 저장 함수들 ==============
def save_dataframe_to_csv(dataframe, filename, folder_path):
    """
    데이터프레임을 CSV 파일로 저장.
    """
    timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")
    filename_with_prefix = f"{timestamp_prefix}_{filename}"
    file_path = os.path.join(folder_path, filename_with_prefix)

    dataframe.to_csv(file_path, index=True, index_label="timestamp")
    logger.info(f"{filename_with_prefix} saved to {folder_path}")


def save_fng_to_csv(fear_greed_index, folder_path):
    """
    공포 탐욕 지수 리스트를 CSV 파일로 저장.
    """
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


# ============== (4) 날짜 파싱 함수 ==============
def parse_date_str(date_str):
    """
    구글 뉴스에서 흔히 볼 수 있는 날짜 문자열 예시:
    - '3 hours ago'
    - '2 days ago'
    - 'Jan 22, 2025'
    등등을 단순 처리합니다.
    """
    if not date_str:
        return datetime.now()

    date_str = date_str.strip().lower()

    # (4-1) X minutes/hours/days ago 패턴
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

    # (4-3) 파싱이 안 되면 현재 시각 반환 (혹은 None)
    return datetime.now()


# ============== (5) 구글 뉴스 크롤링 ==============
def generate_url(query, start=0, date_filter=None):
    """
    Google 뉴스 검색 URL 생성 함수.
    date_filter가 주어지면 'tbs' 파라미터(qdr:...)를 생성하고,
    주어지지 않으면 (None 또는 빈 문자열) tbs를 넣지 않아
    기간 제한을 적용하지 않습니다.
    """
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
            "date": date_str,     # 원본 문자열
            "source": source
        })
    return page_results


def get_news_data(query, num_results=10, date_filter=None):
    """
    최대 num_results개를 모을 때까지 페이지를 넘겨가며 뉴스를 수집.
    date_filter 인자가 주어졌을 경우에만 'qdr:{date_filter}' 형태로 기간 제한.
    """
    collected_results = []
    start = 0

    while len(collected_results) < num_results:
        url = generate_url(query, start, date_filter=date_filter)
        page_data = get_news_on_page(url)
        if not page_data:
            break

        collected_results.extend(page_data)
        start += 10

    # 필요한 개수만 잘라서 반환
    return collected_results[:num_results]


def save_news_to_csv(data, folder_path):
    """
    뉴스 데이터(제목, 스니펫, 날짜, 소스)를 CSV로 저장.
    'parsed_dt' 필드는 CSV에서 제외합니다.
    """
    timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")
    filename_with_prefix = f"{timestamp_prefix}_google_news.csv"
    csv_path = os.path.join(folder_path, filename_with_prefix)

    # CSV에 기록하기 전에 parsed_dt 필드 제거
    for item in data:
        if "parsed_dt" in item:
            del item["parsed_dt"]

    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["title", "snippet", "date", "source"]
        )
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"{filename_with_prefix} saved to {folder_path}")


# ============== (6) 미체결 주문 조회 함수 ==============
def save_open_orders_to_txt(folder_path):
    """
    업비트 미체결 주문 조회 후 텍스트 파일로 저장.
    """
    try:
        open_orders = upbit.get_order("KRW-BTC")  # BTC 마켓의 미체결 주문
        if not open_orders:
            logger.info("No open orders found.")
            return

        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")
        filename_with_prefix = f"{timestamp_prefix}_open_orders.txt"
        orders_file = os.path.join(folder_path, filename_with_prefix)

        with open(orders_file, "w", encoding="utf-8") as file:
            file.write("=== 미체결 주문 (Open Orders) ===\n")
            json.dump(open_orders, file, ensure_ascii=False, indent=4)
        logger.info(f"{filename_with_prefix} saved to {folder_path}")
    except Exception as e:
        logger.error(f"Error fetching or saving open orders: {e}")


# ============== (7) 메인 실행부 ==============
if __name__ == "__main__":
    # (7-1) 폴더 생성
    output_folder = create_folders("spot_BTCKRW_report")

    # (7-2) Upbit 잔고 및 호가 정보 수집
    all_balances = upbit.get_balances()
    orderbook = pyupbit.get_orderbook("KRW-BTC")

    # (7-3) OHLCV 데이터 수집
    df_15min = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="minute15", count=5000)).sort_index(ascending=False)
    df_hourly = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=5000)).sort_index(ascending=False)
    df_4hour = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=2500)).sort_index(ascending=False)
    df_daily = dropna(pyupbit.get_ohlcv("KRW-BTC", interval="day", count=1460)).sort_index(ascending=False)

    # (7-4) CSV 저장 (시세 데이터)
    save_dataframe_to_csv(df_15min, "15min.csv", output_folder)
    save_dataframe_to_csv(df_hourly, "1h.csv", output_folder)
    save_dataframe_to_csv(df_4hour, "4h.csv", output_folder)
    save_dataframe_to_csv(df_daily, "1d.csv", output_folder)

    # (7-5) Balances, Orderbook 분리 저장
    timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    balances_filename = f"{timestamp_prefix}_balances.txt"
    balances_file = os.path.join(output_folder, balances_filename)
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 현재 투자 상태 (Balances) ===\n")
        json.dump(all_balances, bf, ensure_ascii=False, indent=4)
    logger.info(f"{balances_filename} saved to {output_folder}")

    orderbook_filename = f"{timestamp_prefix}_orderbook.txt"
    orderbook_file = os.path.join(output_folder, orderbook_filename)
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 오더북 데이터 (Orderbook) ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)
    logger.info(f"{orderbook_filename} saved to {output_folder}")

    # (7-6) 미체결 주문 조회 및 저장
    save_open_orders_to_txt(output_folder)

    # (7-7) 구글 뉴스 크롤링 (50개 수집 후 날짜 파싱하여 최신순 정렬 → 상위 10개만 CSV 저장)
    query = "Bitcoin"
    news_data_raw = get_news_data(query, num_results=50)  # 50개 크롤링

    # 각 기사 날짜를 datetime으로 변환하여 parsed_dt 필드 추가
    for item in news_data_raw:
        item["parsed_dt"] = parse_date_str(item["date"])

    # parsed_dt 기준 내림차순 정렬 (최신 기사 우선)
    news_sorted = sorted(news_data_raw, key=lambda x: x["parsed_dt"], reverse=True)

    # 상위 10개만 추려서 CSV 저장
    latest_10_news = news_sorted[:10]
    # save_news_to_csv() 내부에서 'parsed_dt'를 제거하므로, 그대로 호출해도 문제 없음.
    save_news_to_csv(latest_10_news, output_folder)

    # (7-8) 공포 탐욕지수 조회 및 저장
    fear_greed_index = requests.get("https://api.alternative.me/fng/?limit=7").json().get('data', [])
    save_fng_to_csv(fear_greed_index, output_folder)

    print("스크립트가 정상적으로 완료되었습니다.")