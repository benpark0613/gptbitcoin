import logging
import os
import json
import csv
import zipfile
import requests
import re
import pytz
import pandas as pd
import numpy as np  # 지표 계산을 위해 NumPy 추가
import time  # Selenium으로 페이지 로딩 대기 등에 사용

# BeautifulSoup을 완전히 제거해도 되지만, 다른 부분에서 혹시 쓸 수 있으므로 유지하되 구글 뉴스 부분에서는 사용 안 함
# from bs4 import BeautifulSoup

from datetime import datetime, timedelta
from dotenv import load_dotenv

# python-mbinance
from mbinance.client import Client

# Selenium 관련
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

access = os.getenv("BINANCE_ACCESS_KEY")
secret = os.getenv("BINANCE_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")

client = Client(access, secret)


def create_folders(base_folder_name):
    """
    지정된 폴더가 없으면 새로 만들고, 이미 존재한다면 내부 파일/폴더들을 정리 후 리턴
    """
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
    """
    pandas DataFrame을 CSV로 저장
    """
    file_path = os.path.join(folder_path, full_filename)
    dataframe.to_csv(file_path, index=False)
    logger.info(f"{full_filename} saved to {folder_path}")


def save_fng_to_csv(fear_greed_index, folder_path, timestamp_prefix=None):
    """
    공포/탐욕 지수 데이터를 CSV로 저장
    """
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
    """
    구글 뉴스 항목 중 날짜 문자열을 파싱해 datetime 형식으로 변환
    """
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
    """
    구글 뉴스 검색 URL 생성
    """
    base_url = "https://www.google.com/search"
    params = {"q": query, "gl": "us", "tbm": "nws", "start": start}
    if date_filter:
        params["tbs"] = f"qdr:{date_filter}"
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{query_string}"


# ---------------------------------------------------------------------------------
# (수정된) Selenium을 이용해 구글 뉴스 기사를 가져오는 함수
# ---------------------------------------------------------------------------------
def fetch_google_news_data(query, total_results=30, date_filter=None):
    """
    구글 뉴스에서 원하는 개수(total_results)만큼 기사를 Selenium으로 크롤링
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    collected_results = []
    start = 0

    try:
        while len(collected_results) < total_results:
            url = generate_url(query, start, date_filter=date_filter)
            driver.get(url)

            # 페이지 로딩 시간을 위해 약간 대기
            time.sleep(2)

            articles = driver.find_elements(By.CSS_SELECTOR, "div.SoaBEf")
            if not articles:
                # 더 이상 기사가 없다고 판단되면 중단
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
                    # 각 기사 중 일부 요소가 없으면 건너뛰기
                    pass

            # 구글 뉴스는 1페이지당 대략 10개 기사씩 표시
            start += 10

    finally:
        driver.quit()

    # 최종적으로 total_results만큼 잘라서 반환
    return collected_results[:total_results]


def save_google_news_data(news_data, folder_path, top_n=10, timestamp_prefix=None):
    """
    수집된 뉴스 중 최신순(top_n개)만 CSV로 저장
    """
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    filename_with_prefix = f"{timestamp_prefix}_google_news.csv"
    csv_path = os.path.join(folder_path, filename_with_prefix)

    # 날짜 파싱
    for item in news_data:
        item["parsed_dt"] = parse_date_str(item["date"])

    # 최신순 정렬 후 상위 top_n만
    news_sorted = sorted(news_data, key=lambda x: x["parsed_dt"], reverse=True)
    latest_n_news = news_sorted[:top_n]

    # CSV에 저장할 때 'parsed_dt' 제거
    for item in latest_n_news:
        if "parsed_dt" in item:
            del item["parsed_dt"]

    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["title", "snippet", "date", "source"])
        writer.writeheader()
        writer.writerows(latest_n_news)

    logger.info(f"{filename_with_prefix} saved to {folder_path}")


def fetch_futures_balance():
    """
    선물 계정 잔고 조회
    """
    try:
        futures_balance = client.futures_account_balance()
        logger.info("선물 잔고 정보를 성공적으로 조회했습니다.")

        non_zero_balance = []
        for b in futures_balance:
            if float(b.get("balance", 0)) != 0:
                non_zero_balance.append(b)

        return non_zero_balance
    except Exception as e:
        logger.error(f"잔고 조회 에러: {e}")
        return []


def fetch_futures_orderbook(symbol="BTCUSDT", limit=20):
    """
    특정 심볼의 선물 오더북 조회
    """
    try:
        orderbook = client.futures_order_book(symbol=symbol, limit=limit)
        logger.info(f"'{symbol}' 선물 오더북을 성공적으로 조회했습니다.")
        return orderbook
    except Exception as e:
        logger.error(f"오더북 조회 에러: {e}")
        return {}


def save_balance_and_orderbook(balances, orderbook, folder_path, timestamp_prefix=None):
    """
    잔고와 오더북을 텍스트 파일로 저장
    """
    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    balances_filename = f"{timestamp_prefix}_futures_balances.txt"
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


def calculate_rsi(df, period=14):
    """
    RSI (Relative Strength Index) 계산
    """
    df = df.copy()
    df["change"] = df["close"].diff()
    df["gain"] = np.where(df["change"] > 0, df["change"], 0)
    df["loss"] = np.where(df["change"] < 0, -df["change"], 0)

    # 단순 이동평균
    df["avg_gain"] = df["gain"].rolling(window=period).mean()
    df["avg_loss"] = df["loss"].rolling(window=period).mean()

    df["rs"] = np.where(df["avg_loss"] == 0, np.nan, df["avg_gain"] / df["avg_loss"])
    df["rsi"] = 100 - (100 / (1.0 + df["rs"]))
    return df["rsi"]


def calculate_macd(df, short=12, long=26, signal=9):
    """
    MACD 계산
    """
    df = df.copy()
    df["ema_short"] = df["close"].ewm(span=short, adjust=False).mean()
    df["ema_long"] = df["close"].ewm(span=long, adjust=False).mean()
    df["macd"] = df["ema_short"] - df["ema_long"]
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    return df["macd"], df["macd_signal"], df["macd_hist"]


def calculate_bollinger_bands(df, period=20, num_std=2):
    """
    Bollinger Bands 계산
    """
    df = df.copy()
    df["mbb"] = df["close"].rolling(window=period).mean()
    df["std"] = df["close"].rolling(window=period).std()
    df["upper_bb"] = df["mbb"] + (df["std"] * num_std)
    df["lower_bb"] = df["mbb"] - (df["std"] * num_std)
    return df["mbb"], df["upper_bb"], df["lower_bb"]


def calculate_fibonacci_levels(df, lookback=100):
    """
    최근 특정 구간(lookback)에서의 고점/저점을 찾아 피보나치 되돌림 레벨 계산.
    """
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
    """
    OBV (On-Balance Volume) 계산
    """
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


def fetch_and_save_ohlcv(symbol, output_folder, intervals, timestamp_prefix=None):
    """
    바이낸스 선물 K라인 데이터(OHLCV)를 조회 후
    - extended_limit으로 더 많은 캔들 확보
    - 보조지표(RSI, MACD, Bollinger, OBV, EMA) + 피보나치 계산
    - dropna(subset=...)로 특정 지표만 결측치 제거
    - 최종 limit 개수 슬라이싱 후 CSV 저장
    """
    kst = pytz.timezone('Asia/Seoul')

    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    # EMA를 계산할 interval
    ema_intervals = [
        Client.KLINE_INTERVAL_5MINUTE,
        Client.KLINE_INTERVAL_15MINUTE,
        Client.KLINE_INTERVAL_30MINUTE,
    ]
    for setting in intervals:
        interval_name = setting.get("interval", "1m")
        limit_value = setting.get("limit", 500)

        extended_limit = limit_value + 500  # EMA 등 지표 계산 위해 추가 확보

        csv_filename = f"{timestamp_prefix}_{interval_name}.csv"

        try:
            klines = client.futures_klines(
                symbol=symbol,
                interval=interval_name,
                limit=extended_limit
            )

            df = pd.DataFrame(klines, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_base_volume", "taker_quote_volume", "ignore"
            ])

            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["open_time"] = df["open_time"].dt.tz_convert(kst)
            df.set_index("open_time", inplace=True)
            df.sort_index(ascending=True, inplace=True)

            # float 변환
            df["open"] = df["open"].astype(float)
            df["high"] = df["high"].astype(float)
            df["low"] = df["low"].astype(float)
            df["close"] = df["close"].astype(float)
            df["volume"] = df["volume"].astype(float)
            df["quote_volume"] = df["quote_volume"].astype(float)

            # EMA 계산 (해당 interval이면)
            if interval_name in ema_intervals:
                df["ema_9"] = df["close"].ewm(span=9).mean()
                df["ema_21"] = df["close"].ewm(span=21).mean()
                # df["ema_7"] = df["close"].ewm(span=7).mean()
                # df["ema_25"] = df["close"].ewm(span=25).mean()
                # df["ema_99"] = df["close"].ewm(span=99).mean()
            else:
                df["ema_9"] = np.nan
                df["ema_12"] = np.nan
                # df["ema_7"] = np.nan
                # df["ema_25"] = np.nan
                # df["ema_99"] = np.nan

            # RSI, MACD, Bollinger, OBV
            df["rsi"] = calculate_rsi(df, period=14)
            df["macd"], df["macd_signal"], df["macd_hist"] = calculate_macd(df)
            df["bb_mid"], df["bb_upper"], df["bb_lower"] = calculate_bollinger_bands(df)
            df["obv"] = calculate_obv(df)
            df["bb_width"] = df["bb_upper"] - df["bb_lower"]

            # 피보나치 레벨
            fibo_levels = calculate_fibonacci_levels(df)
            df["fib_0%"] = fibo_levels["0%"]
            df["fib_23.6%"] = fibo_levels["23.6%"]
            df["fib_38.2%"] = fibo_levels["38.2%"]
            df["fib_50%"] = fibo_levels["50%"]
            df["fib_61.8%"] = fibo_levels["61.8%"]
            df["fib_78.6%"] = fibo_levels["78.6%"]
            df["fib_100%"] = fibo_levels["100%"]

            # quote_volume을 value라는 이름으로 사용
            df["value"] = df["quote_volume"]

            before_dropna = len(df)
            logger.info(f"{interval_name} => before_dropna={before_dropna}")

            df.dropna(subset=["rsi", "macd", "macd_signal", "macd_hist", "obv"], inplace=True)

            after_dropna = len(df)
            logger.info(f"{interval_name} => after_dropna={after_dropna}")

            # 최종 limit 개수만 남김
            if len(df) > limit_value:
                df = df.iloc[-limit_value:]

            # 시간 내림차순 정렬
            df.sort_index(ascending=False, inplace=True)
            df.reset_index(inplace=True)
            df.rename(columns={"open_time": "timestamp"}, inplace=True)

            # 중간 계산용 컬럼 제거
            for col in ["change", "gain", "loss", "avg_gain", "avg_loss", "rs", "ema_short", "ema_long", "std"]:
                if col in df.columns:
                    df.drop(col, axis=1, inplace=True)

            # 필요 없는 컬럼도 제거
            df.drop([
                "close_time", "trades",
                "taker_base_volume", "taker_quote_volume", "ignore",
                "quote_volume"  # 이미 value로 사용함
            ], axis=1, errors="ignore", inplace=True)

            # 최종 컬럼 순서
            desired_columns = [
                "timestamp", "open", "high", "low", "close", "volume", "value",
                "rsi",
                # "macd", "macd_signal", "macd_hist",
                "bb_mid", "bb_upper", "bb_lower", "bb_width",
                # "obv",
                "ema_9", "ema_21"
                # "fib_0%", "fib_23.6%", "fib_38.2%", "fib_50%", "fib_61.8%", "fib_78.6%", "fib_100%"
            ]
            existing_cols = [col for col in desired_columns if col in df.columns]
            df = df[existing_cols]

            save_dataframe_to_csv(df, csv_filename, output_folder)

        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol} - {interval_name}: {e}")


def fetch_futures_positions():
    """
    선물 포지션 정보 조회
    """
    try:
        positions = client.futures_position_information()
        logger.info("선물 포지션 정보를 성공적으로 조회했습니다.")
        return positions
    except Exception as e:
        logger.error(f"포지션 조회 에러: {e}")
        return []


def fetch_futures_open_orders(symbol=None):
    """
    선물 오픈 오더 조회
    """
    try:
        open_orders = client.futures_get_open_orders(symbol=symbol) if symbol else client.futures_get_open_orders()
        logger.info("선물 오픈 오더를 성공적으로 조회했습니다.")
        return open_orders
    except Exception as e:
        logger.error(f"오픈 오더 조회 에러: {e}")
        return []


def save_positions_to_csv(positions, folder_path, timestamp_prefix=None):
    """
    조회된 포지션 정보를 CSV로 저장
    """
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
    """
    오픈 오더 정보를 CSV로 저장
    """
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
    폴더 내 모든 파일을 zip으로 압축
    """
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


def fetch_long_short_ratio(symbol="BTCUSDT", period="1h", limit=30):
    """
    바이낸스 선물 롱/숏 비율 조회
    (period: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d)
    """
    endpoint = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit
    }
    try:
        resp = requests.get(endpoint, params=params)
        data = resp.json()
        if isinstance(data, dict) and data.get("code"):
            raise Exception(data.get("msg", "API error"))
        logger.info("롱/숏 비율 데이터를 성공적으로 조회했습니다.")
        return data
    except Exception as e:
        logger.error(f"롱/숏 비율 조회 에러: {e}")
        return []


def save_long_short_ratio_to_csv(ratio_data, folder_path, timestamp_prefix=None):
    """
    롱/숏 비율 데이터를 CSV로 저장
    """
    if not ratio_data:
        logger.warning("No long/short ratio data to save.")
        return

    if not timestamp_prefix:
        timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    filename_with_prefix = f"{timestamp_prefix}_long_short_ratio.csv"
    csv_path = os.path.join(folder_path, filename_with_prefix)

    df = pd.DataFrame(ratio_data)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"{filename_with_prefix} saved to {folder_path}")


def main():
    common_timestamp_prefix = datetime.now().strftime("%y%m%d%H%M")

    # [사용자가 컨트롤할 수 있는 부분들]
    symbol = "BTCUSDT"  # 심볼
    orderbook_limit = 100  # 오더북 개수 제한
    google_query_keyword = "Bitcoin"  # 구글뉴스 검색어
    intervals_for_ohlcv = [
        {"interval": Client.KLINE_INTERVAL_5MINUTE, "limit": 576},
        {"interval": Client.KLINE_INTERVAL_15MINUTE, "limit": 672},
        {"interval": Client.KLINE_INTERVAL_1HOUR, "limit": 720},
        {"interval": Client.KLINE_INTERVAL_4HOUR, "limit": 480},
        {"interval": Client.KLINE_INTERVAL_1DAY, "limit": 720},
    ]

    # 보고서 폴더 생성
    output_folder = create_folders(f"futures_{symbol}_report")

    # 1) 선물 잔고 및 오더북
    futures_balance = fetch_futures_balance()
    orderbook = fetch_futures_orderbook(symbol=symbol, limit=orderbook_limit)
    save_balance_and_orderbook(futures_balance, orderbook, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 2) OHLCV 데이터 + 지표(RSI, MACD, Bollinger, OBV) + EMA + 피보나치 레벨
    # fetch_and_save_ohlcv(symbol, output_folder, intervals_for_ohlcv, timestamp_prefix=common_timestamp_prefix)

    # 3) (수정된) 구글 뉴스 스크래핑(셀레니움) 후 저장
    # google_news_data = fetch_google_news_data(query=google_query_keyword, total_results=30)
    # save_google_news_data(google_news_data, output_folder, top_n=10, timestamp_prefix=common_timestamp_prefix)

    # 4) 공포/탐욕 지수
    # fear_greed_index = requests.get("https://api.alternative.me/fng/?limit=7").json().get("data", [])
    # save_fng_to_csv(fear_greed_index, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 5) 포지션/오픈오더 조회 후 CSV
    positions = fetch_futures_positions()
    save_positions_to_csv(positions, output_folder, timestamp_prefix=common_timestamp_prefix)

    open_orders = fetch_futures_open_orders(symbol=None)
    save_open_orders_to_csv(open_orders, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 6) 롱/숏 비율 조회 후 CSV 저장
    long_short_data = fetch_long_short_ratio(symbol=symbol, period="1h", limit=30)
    save_long_short_ratio_to_csv(long_short_data, output_folder, timestamp_prefix=common_timestamp_prefix)

    # 7) 폴더 내 결과물을 압축
    compress_files_in_folder(output_folder, zip_filename="binance_futures_report.zip")

    logger.info("바이낸스 선물 리포트 스크립트가 정상적으로 완료되었습니다.")
    print("스크립트가 정상적으로 완료되었습니다.")

if __name__ == "__main__":
    main()