import logging
import os
import json
import csv
import zipfile

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


def save_dataframe_to_csv(dataframe, full_filename, folder_path):
    file_path = os.path.join(folder_path, full_filename)
    # index=False를 사용하면 인덱스 컬럼(번호)이 CSV에 포함되지 않음
    dataframe.to_csv(file_path, index=False)
    logger.info(f"{full_filename} saved to {folder_path}")


def save_fng_to_csv(fear_greed_index, folder_path, timestamp_prefix=None):
    if not fear_greed_index:
        logger.warning("No fear_greed_index data available to save.")
        return

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


def save_news_to_csv(data, folder_path, timestamp_prefix=None):
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


def retrieve_and_save_google_news(output_folder, query="Bitcoin", total_results=50, top_n=10, timestamp_prefix=None):
    news_data_raw = get_news_data(query, num_results=total_results)
    for item in news_data_raw:
        item["parsed_dt"] = parse_date_str(item["date"])
    news_sorted = sorted(news_data_raw, key=lambda x: x["parsed_dt"], reverse=True)
    latest_n_news = news_sorted[:top_n]

    save_news_to_csv(latest_n_news, output_folder, timestamp_prefix=timestamp_prefix)


def fetch_futures_balance():
    """
    바이낸스 선물 계정의 잔고 정보를 가져온 뒤,
    balance가 0이 아닌 항목만 반환합니다.
    """
    try:
        futures_balance = client.futures_account_balance()
        logger.info("선물 잔고 정보를 성공적으로 조회했습니다.")

        # balance가 0이 아닌 항목만 필터링
        non_zero_balance = []
        for b in futures_balance:
            # balance 문자열을 float으로 변환 후 체크
            if float(b.get("balance", 0)) != 0:
                non_zero_balance.append(b)

        return non_zero_balance
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


def save_balance_and_orderbook(balances, orderbook, folder_path, timestamp_prefix=None):
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    balances_filename = f"{timestamp_prefix}_balances.txt"
    balances_file = os.path.join(folder_path, balances_filename)
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 바이낸스 선물 잔고 (0이 아닌 잔고만) ===\n")
        json.dump(balances, bf, ensure_ascii=False, indent=4)

    orderbook_filename = f"{timestamp_prefix}_orderbook.txt"
    orderbook_file = os.path.join(folder_path, orderbook_filename)
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 바이낸스 선물 오더북 ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)

    logger.info(f"{balances_filename} / {orderbook_filename} saved to {folder_path}")


def fetch_and_save_ohlcv(symbol, output_folder, intervals, timestamp_prefix=None):
    """
    OHLCV 데이터를 가져와 CSV로 저장하며,
    5분, 15분, 1시간, 1일 차트에 대해 ema_7, ema_25, ema_99 컬럼을 추가로 계산합니다.
    그리고 최종 CSV에는 timestamp, open, high, low, close, volume, ema_7, ema_25, ema_99만 남깁니다.
    """
    kst = pytz.timezone('Asia/Seoul')

    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    # EMA 계산 대상 interval
    ema_intervals = [
        Client.KLINE_INTERVAL_5MINUTE,
        Client.KLINE_INTERVAL_15MINUTE,
        Client.KLINE_INTERVAL_1HOUR,
        Client.KLINE_INTERVAL_1DAY
    ]

    for setting in intervals:
        interval_name = setting.get("interval", "1m")
        limit_value = setting.get("limit", 500)

        csv_filename = f"{timestamp_prefix}_{interval_name}.csv"

        try:
            klines = client.futures_klines(symbol=symbol, interval=interval_name, limit=limit_value)

            df = pd.DataFrame(klines, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_base_volume", "taker_quote_volume", "ignore"
            ])

            # 시간 변환 및 인덱스 설정
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["open_time"] = df["open_time"].dt.tz_convert(kst)
            df.set_index("open_time", inplace=True)

            # 시간 오름차순 정렬
            df.sort_index(ascending=True, inplace=True)

            # 필요한 컬럼 타입 변환
            df["close"] = df["close"].astype(float)

            # EMA 계산 (지정한 인터벌일 때만)
            if interval_name in ema_intervals:
                df["ema_7"] = df["close"].ewm(span=7).mean()
                df["ema_25"] = df["close"].ewm(span=25).mean()
                df["ema_99"] = df["close"].ewm(span=99).mean()
            else:
                # 해당 인터벌이 아니면 일단 빈 컬럼 생성
                df["ema_7"] = None
                df["ema_25"] = None
                df["ema_99"] = None

            # 최종으로 최근 데이터가 위로 오게 내림차순 정렬
            df.sort_index(ascending=False, inplace=True)

            # 인덱스를 컬럼으로 재변환
            df.reset_index(inplace=True)  # open_time -> 새 컬럼

            # open_time 컬럼명을 timestamp로 변경
            df.rename(columns={"open_time": "timestamp"}, inplace=True)

            # 필요 없는 컬럼 제거
            df.drop(["close_time", "quote_volume", "trades", "taker_base_volume", "taker_quote_volume", "ignore"],
                    axis=1, inplace=True)

            # 최종으로 필요한 컬럼만 순서대로 선택
            df = df[["timestamp", "open", "high", "low", "close", "volume", "ema_7", "ema_25", "ema_99"]]

            # CSV 저장
            save_dataframe_to_csv(df, csv_filename, output_folder)

        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol} - {interval_name}: {e}")


# ============== (새로 추가) 선물 포지션, 오픈 오더 조회 및 CSV 저장 ==============
def fetch_futures_positions():
    try:
        positions = client.futures_position_information()
        logger.info("선물 포지션 정보를 성공적으로 조회했습니다.")
        return positions
    except Exception as e:
        logger.error(f"포지션 조회 에러: {e}")
        return []


def fetch_futures_open_orders(symbol=None):
    try:
        open_orders = client.futures_get_open_orders(symbol=symbol) if symbol else client.futures_get_open_orders()
        logger.info("선물 오픈 오더를 성공적으로 조회했습니다.")
        return open_orders
    except Exception as e:
        logger.error(f"오픈 오더 조회 에러: {e}")
        return []


def save_positions_to_csv(positions, folder_path, timestamp_prefix=None):
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    if not positions:
        logger.warning("No positions to save.")
        return

    csv_filename = f"{timestamp_prefix}_futures_positions.csv"
    csv_path = os.path.join(folder_path, csv_filename)

    df = pd.DataFrame(positions)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"{csv_filename} saved to {folder_path}")


def save_open_orders_to_csv(open_orders, folder_path, timestamp_prefix=None):
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    if not open_orders:
        logger.warning("No open orders to save.")
        return

    csv_filename = f"{timestamp_prefix}_futures_open_orders.csv"
    csv_path = os.path.join(folder_path, csv_filename)

    df = pd.DataFrame(open_orders)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"{csv_filename} saved to {folder_path}")

def compress_files_in_folder(folder_path, zip_filename=None):
    """
    지정된 폴더 내의 모든 파일을 Zip(압축) 파일로 묶는다.
    zip_filename을 지정하지 않으면 "compressed_files.zip"으로 생성된다.
    """
    if not zip_filename:
        zip_filename = "compressed_files.zip"

    # 압축 파일도 같은 폴더에 생성
    zip_filepath = os.path.join(folder_path, zip_filename)

    with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
        # 폴더 내 모든 파일을 순회
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # 생성 중인 zip 파일 자체는 건너뛴다
                if file == zip_filename:
                    continue
                file_path = os.path.join(root, file)
                # zip 내부에서의 상대 경로(arcname)를 지정
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
    logger.info(f"폴더 내 파일이 모두 압축되었습니다: {zip_filepath}")

# ============== (8) 메인 실행부 ==============
if __name__ == "__main__":
    common_timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    output_folder = create_folders("futures_BTCUSDT_report")

    futures_balance = fetch_futures_balance()
    orderbook = fetch_futures_orderbook(symbol="BTCUSDT", limit=100)
    save_balance_and_orderbook(
        futures_balance,
        orderbook,
        output_folder,
        timestamp_prefix=common_timestamp_prefix
    )

    # 5분, 15분, 1시간, 1일 차트에 대해 EMA를 계산하고, 결과 CSV에는 필요한 열만 남김
    my_intervals = [
        {"interval": Client.KLINE_INTERVAL_5MINUTE, "limit": 200},
        {"interval": Client.KLINE_INTERVAL_15MINUTE, "limit": 150},
        {"interval": Client.KLINE_INTERVAL_1HOUR,   "limit": 200},
        {"interval": Client.KLINE_INTERVAL_1DAY,    "limit": 200}
    ]
    fetch_and_save_ohlcv("BTCUSDT", output_folder, my_intervals, timestamp_prefix=common_timestamp_prefix)

    retrieve_and_save_google_news(
        output_folder,
        query="Bitcoin",
        total_results=30,
        top_n=10,
        timestamp_prefix=common_timestamp_prefix
    )

    fear_greed_index = requests.get("https://api.alternative.me/fng/?limit=7").json().get("data", [])
    save_fng_to_csv(fear_greed_index, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 선물 포지션 / 오픈 오더 조회 및 CSV 저장
    positions = fetch_futures_positions()
    save_positions_to_csv(positions, output_folder, timestamp_prefix=common_timestamp_prefix)

    open_orders = fetch_futures_open_orders(symbol=None)  # 특정 심볼 지정 시 symbol="BTCUSDT"
    save_open_orders_to_csv(open_orders, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 마지막에 폴더 압축
    compress_files_in_folder(output_folder, f"{common_timestamp_prefix}_report.zip")
    logger.info("바이낸스 선물 리포트와 관련 파일들을 압축 완료했습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")

    logger.info("바이낸스 선물 리포트 스크립트가 정상적으로 완료되었습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")
