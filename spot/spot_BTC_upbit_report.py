import logging
import os
import json
import csv
# import requests
import pyupbit
import re
import time
import zipfile

import numpy as np
import pandas as pd
import pytz

# from bs4 import BeautifulSoup
from ta.utils import dropna
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys.")

upbit = pyupbit.Upbit(access, secret)


# ===================== 지표 계산 함수들 =====================
def calculate_rsi(df, period=14):
    df = df.copy()
    df["change"] = df["close"].diff()
    df["gain"] = np.where(df["change"] > 0, df["change"], 0)
    df["loss"] = np.where(df["change"] < 0, -df["change"], 0)

    df["avg_gain"] = df["gain"].rolling(window=period).mean()
    df["avg_loss"] = df["loss"].rolling(window=period).mean()

    df["rs"] = np.where(df["avg_loss"] == 0, np.nan, df["avg_gain"] / df["avg_loss"])
    df["rsi"] = 100 - (100 / (1.0 + df["rs"]))
    return df["rsi"]

def calculate_macd(df, short=12, long=26, signal=9):
    df = df.copy()
    df["ema_short"] = df["close"].ewm(span=short, adjust=False).mean()
    df["ema_long"] = df["close"].ewm(span=long, adjust=False).mean()
    df["macd"] = df["ema_short"] - df["ema_long"]
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    return df["macd"], df["macd_signal"], df["macd_hist"]

def calculate_bollinger_bands(df, period=20, num_std=2):
    df = df.copy()
    df["mbb"] = df["close"].rolling(window=period).mean()
    df["std"] = df["close"].rolling(window=period).std()
    df["upper_bb"] = df["mbb"] + (df["std"] * num_std)
    df["lower_bb"] = df["mbb"] - (df["std"] * num_std)
    return df["mbb"], df["upper_bb"], df["lower_bb"]

def calculate_fibonacci_levels(df, lookback=100):
    recent_df = df.tail(lookback)
    min_price = recent_df["close"].min()
    max_price = recent_df["close"].max()
    diff = max_price - min_price

    fib_levels = {
        "0%": max_price,
        "23.6%": max_price - diff * 0.236,
        "38.2%": max_price - diff * 0.382,
        "50%": max_price - diff * 0.5,
        "61.8%": max_price - diff * 0.618,
        "78.6%": max_price - diff * 0.786,
        "100%": min_price
    }
    return fib_levels

def calculate_obv(df):
    df = df.copy()
    df["obv"] = 0.0
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            df.at[df.index[i], "obv"] = df["obv"].iloc[i - 1] + df["volume"].iloc[i]
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            df.at[df.index[i], "obv"] = df["obv"].iloc[i - 1] - df["volume"].iloc[i]
        else:
            df.at[df.index[i], "obv"] = df["obv"].iloc[i - 1]
    return df["obv"]


# ===================== 폴더 생성 함수 =====================
def create_folders(base_folder_name="spot_BTCKRW_report"):
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


# ===================== CSV 저장 함수들 =====================
def save_dataframe_to_csv(dataframe, full_filename, folder_path):
    file_path = os.path.join(folder_path, full_filename)
    dataframe.to_csv(file_path, index=False)
    logger.info(f"{full_filename} saved to {folder_path}")

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


# ===================== 파일 압축 함수 =====================
def compress_files_in_folder(folder_path, zip_filename=None):
    if not zip_filename:
        zip_filename = "compressed_files.zip"

    zip_filepath = os.path.join(folder_path, zip_filename)
    with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file == zip_filename:
                    continue
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)

    logger.info(f"폴더 내 파일이 모두 압축되었습니다: {zip_filepath}")


# ===================== 날짜 파싱 함수 =====================
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


# ===================== (수정된) 구글 뉴스 크롤링: Selenium 사용 =====================
def generate_url(query, start=0, date_filter=None):
    """
    구글 뉴스 URL 생성 함수. date_filter가 있을 경우 qdr 옵션을 추가.
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

def get_news_data_selenium(query, num_results=10, date_filter=None):
    """
    Selenium을 사용하여 여러 페이지에서 뉴스를 수집.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    collected_results = []
    start = 0

    try:
        while len(collected_results) < num_results:
            url = generate_url(query, start, date_filter=date_filter)
            driver.get(url)

            # 페이지 로딩 및 동적 요소 처리를 위해 잠시 대기
            time.sleep(2)

            articles = driver.find_elements(By.CSS_SELECTOR, "div.SoaBEf")
            if not articles:
                break

            for el in articles:
                try:
                    title_el = el.find_element(By.CSS_SELECTOR, "div.MBeuO")
                    snippet_el = el.find_element(By.CSS_SELECTOR, ".GI74Re")
                    date_el = el.find_element(By.CSS_SELECTOR, ".LfVVr")
                    source_el = el.find_element(By.CSS_SELECTOR, ".NUnG9d span")

                    title = title_el.text.strip() if title_el else "N/A"
                    snippet = snippet_el.text.strip() if snippet_el else "N/A"
                    date_str = date_el.text.strip() if date_el else "N/A"
                    source = source_el.text.strip() if source_el else "N/A"

                    collected_results.append({
                        "title": title,
                        "snippet": snippet,
                        "date": date_str,
                        "source": source
                    })
                except:
                    # 개별 기사 요소가 없거나 파싱 실패 시 넘어감
                    pass

            start += 10
            if len(articles) == 0:
                break

    finally:
        driver.quit()

    # 필요한 만큼만 잘라서 반환
    return collected_results[:num_results]

def retrieve_and_save_google_news(output_folder, query="Bitcoin", total_results=10, top_n=10, timestamp_prefix=None):
    news_data_raw = get_news_data_selenium(query, num_results=total_results)
    for item in news_data_raw:
        item["parsed_dt"] = parse_date_str(item["date"])

    news_sorted = sorted(news_data_raw, key=lambda x: x["parsed_dt"], reverse=True)
    latest_n_news = news_sorted[:top_n]

    save_news_to_csv(latest_n_news, output_folder, timestamp_prefix=timestamp_prefix)


# ===================== 미체결 주문 조회 =====================
def save_open_orders_to_txt(folder_path, timestamp_prefix=None):
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


# ===================== 잔고와 오더북 저장 =====================
def save_balance_and_orderbook(balances, orderbook, folder_path, timestamp_prefix=None):
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    balances_filename = f"{timestamp_prefix}_spot_balances.txt"
    balances_file = os.path.join(folder_path, balances_filename)
    with open(balances_file, "w", encoding="utf-8") as bf:
        bf.write("=== 현재 투자 상태 (Balances) ===\n")
        json.dump(balances, bf, ensure_ascii=False, indent=4)

    orderbook_filename = f"{timestamp_prefix}_orderbook.txt"
    orderbook_file = os.path.join(folder_path, orderbook_filename)
    with open(orderbook_file, "w", encoding="utf-8") as of:
        of.write("=== 오더북 데이터 (Orderbook) ===\n")
        json.dump(orderbook, of, ensure_ascii=False, indent=4)

    logger.info(f"{balances_filename} / {orderbook_filename} saved to {folder_path}")


# ===================== OHLCV 여러 interval (지표 계산 포함) =====================
def fetch_and_save_ohlcv(symbol, output_folder, intervals, timestamp_prefix=None):
    """
    데이터를 더 넉넉히 가져온 후, 지표를 계산하고 결측치를 제거한 뒤,
    최종적으로 지정된 count만큼만 데이터 저장.
    """
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%Y%m%d%H%M")

    for setting in intervals:
        interval_name = setting.get("interval", "day")
        count_value = setting.get("count", 200)

        # 지정된 count보다 더 많은 데이터를 가져옴
        extended_count = count_value + 500  # 넉넉히 추가 확보 (필요에 따라 조정)

        logger.info(f"Fetching {extended_count} rows for interval '{interval_name}'")
        df = pyupbit.get_ohlcv(symbol, interval=interval_name, count=extended_count)

        if df is None or df.empty:
            logger.warning(f"No data retrieved for {symbol} with interval={interval_name}")
            continue

        # 1) 지표 계산용으로 시간 오름차순 정렬
        df.sort_index(ascending=True, inplace=True)

        # ========== 지표 계산 ==========
        df["rsi"] = calculate_rsi(df, period=14)

        macd, macd_signal, macd_hist = calculate_macd(df)
        df["macd"], df["macd_signal"], df["macd_hist"] = macd, macd_signal, macd_hist

        mbb, upper_bb, lower_bb = calculate_bollinger_bands(df)
        df["bb_mid"], df["bb_upper"], df["bb_lower"] = mbb, upper_bb, lower_bb

        # OBV
        df["obv"] = calculate_obv(df)

        # BB Width
        df["bb_width"] = ((df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]) * 100

        # EMA
        df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
        df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
        # df["ema_99"] = df["close"].ewm(span=99, adjust=False).mean()

        # Fibonacci Levels
        fib_levels = calculate_fibonacci_levels(df, lookback=100)
        for level, value in fib_levels.items():
            df[f"fib_{level}"] = value

        # 2) 결측치 제거
        df.dropna(inplace=True)

        # 3) 지정된 count만큼 데이터만 남김 (최신 기준으로 자름)
        if len(df) > count_value:
            df = df.iloc[-count_value:]

        # 4) 최신 데이터가 위로 오도록 내림차순 정렬
        df.sort_index(ascending=False, inplace=True)

        # 5) 인덱스를 컬럼으로 변환
        df.reset_index(inplace=True)
        df.rename(columns={"index": "timestamp"}, inplace=True)

        # 6) 최종 컬럼 순서 지정
        desired_columns = [
            "timestamp","open","high","low","close","volume","value",
            "rsi",
            "macd","macd_signal","macd_hist",
            "bb_mid","bb_upper","bb_lower","bb_width",
            # "obv",
            "ema_50","ema_200",
            # "fib_0%","fib_23.6%","fib_38.2%","fib_50%","fib_61.8%","fib_78.6%","fib_100%"
        ]

        # "value" 컬럼이 없으면 만들기
        if "value" not in df.columns:
            df["value"] = np.nan

        for col in desired_columns:
            if col not in df.columns:
                df[col] = np.nan

        df = df[desired_columns]

        # 7) CSV 저장
        csv_filename = f"{timestamp_prefix}_{interval_name}.csv"
        save_dataframe_to_csv(df, csv_filename, output_folder)

        logger.info(f"Data for interval '{interval_name}' saved with {len(df)} rows.")


# ===================== ubcindex.com 공포·탐욕 지수 =====================
def retrieve_and_save_fear_greed_ubcindex(folder_path, timestamp_prefix=None):
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    csv_filename = f"{timestamp_prefix}_fear_greed_ubcindex.csv"
    csv_path = os.path.join(folder_path, csv_filename)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.ubcindex.com/feargreed")
    time.sleep(5)

    score_element = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div/div/div/div[2]/div/div/div[1]/section/div/div/div[1]/div/div[2]"
    )
    today_score = score_element.text.strip().split("/")[0].strip()

    table_rows = driver.find_elements(By.CSS_SELECTOR, "table.historyTbl tbody tr")
    header_row = [td.text.strip() for td in table_rows[0].find_elements(By.TAG_NAME, "td")]
    data_row = [td.text.strip() for td in table_rows[1].find_elements(By.TAG_NAME, "td")]

    header_map = {
        "어제": "Yesterday",
        "일주일전": "1WeekAgo",
        "1개월전": "1MonthAgo",
        "3개월전": "3MonthsAgo",
        "6개월전": "6MonthsAgo",
        "1년전": "1YearAgo"
    }
    target_keys = ["어제", "일주일전", "1개월전", "3개월전", "6개월전", "1년전"]
    extracted_data = []
    for key in target_keys:
        if key in header_row:
            idx = header_row.index(key)
            eng_header = header_map[key]
            extracted_data.append((eng_header, data_row[idx]))
        else:
            extracted_data.append((header_map.get(key, key), ""))

    driver.quit()

    with open(csv_path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Time", "FearIndex"])
        writer.writerow(["Today", today_score])
        for eng_header, val in extracted_data:
            writer.writerow([eng_header, val])

    logger.info(f"{csv_filename} saved to {folder_path}")


# ===================== 메인 실행부 =====================
def main():
    common_timestamp_prefix = datetime.now().strftime("%Y%m%d%H%M")
    output_folder = create_folders("spot_BTCKRW_report")

    # 1) 잔고 & 오더북 저장
    all_balances = upbit.get_balances()
    orderbook = pyupbit.get_orderbook("KRW-BTC")
    save_balance_and_orderbook(all_balances, orderbook, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 2) 여러 구간별 OHLCV 저장 (최신 데이터가 맨 위)
    my_intervals = [
        # {"interval": "minute15", "count": 5760},  # 약 2개월
        {"interval": "minute240", "count": 1080},  # 약 6개월
        {"interval": "day", "count": 1095},  # 약 3년
        # {"interval": "week", "count": 260}  # 약 5년
    ]
    fetch_and_save_ohlcv("KRW-BTC", output_folder, my_intervals, timestamp_prefix=common_timestamp_prefix)

    # 3) 미체결 주문(오픈 오더) 조회 -> TXT
    save_open_orders_to_txt(output_folder, timestamp_prefix=common_timestamp_prefix)

    # 4) (수정된) 구글 뉴스 크롤링 -> CSV
    retrieve_and_save_google_news(
        output_folder,
        query="Bitcoin",
        total_results=50,
        top_n=10,
        timestamp_prefix=common_timestamp_prefix
    )

    # 5) ubcindex.com 공포·탐욕 지수 -> CSV
    retrieve_and_save_fear_greed_ubcindex(output_folder, timestamp_prefix=common_timestamp_prefix)

    # 6) 결과물을 ZIP으로 압축
    compress_files_in_folder(output_folder, zip_filename="upbit_spot_report.zip")

    logger.info("스팟 리포트 스크립트가 정상적으로 완료되었습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")


if __name__ == "__main__":
    main()
