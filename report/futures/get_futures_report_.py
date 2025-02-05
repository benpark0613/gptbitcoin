import os
import csv
import time
import random
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo  # Python 3.9 이상 필요

import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from module.clear_folder import clear_folder
from module.mbinance.position_history import build_position_history
from module.get_googlenews import scrape_news  # 리팩토링된 구글 뉴스 모듈 사용


# ===============================================================
# 보조지표 추가 함수
# ===============================================================
def add_technical_indicators(df, key):
    """
    OHLCV DataFrame에 다음 보조지표를 추가합니다.
      1. RSI (14)
      2. MACD (EMA12, EMA26, Signal 9)
      3. Bollinger Bands (20, 표준편차 2)
      4. 이동평균선 (MA20, MA50)
    """
    # RSI (14)
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = df['MACD'] - df['MACD_signal']

    # Bollinger Bands (20, 2)
    df['BB_middle'] = df['Close'].rolling(window=20).mean()
    df['BB_std'] = df['Close'].rolling(window=20).std()
    df['BB_upper'] = df['BB_middle'] + (2 * df['BB_std'])
    df['BB_lower'] = df['BB_middle'] - (2 * df['BB_std'])
    # 필요 시 df.drop('BB_std', axis=1, inplace=True)

    # 이동평균선 (MA): 단기 차트에서는 MA20, MA50 사용
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA50'] = df['Close'].rolling(window=50).mean()

    return df


# ===============================================================
# 파일 및 폴더 관련 함수
# ===============================================================
def prepare_directories(report_path):
    """
    보고서(리포트) 저장 폴더를 초기화하고, 폴더가 없으면 생성.
    """
    clear_folder(report_path)
    if not os.path.exists(report_path):
        os.makedirs(report_path)


def save_to_csv(file_path, data, fieldnames):
    """
    data가 리스트 형식이면 CSV로 저장합니다.
    """
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        if data:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])


def save_report_txt(position_df, futures_balance, report_txt_file, date_prefix, symbol, klines_dict, google_news_data):
    """
    report.txt에 선물 잔고, 포지션 히스토리, 캔들 데이터(보조지표 포함), 구글 뉴스 데이터를 CSV 형식 그대로 기록.
    """
    with open(report_txt_file, 'w', encoding='utf-8', newline='') as txt_file:
        # (1) 포지션 히스토리 기록
        txt_file.write(f"-- {date_prefix}_position_history (CSV)\n")
        if not position_df.empty:
            txt_file.write(position_df.to_csv(sep=';', index=False))
        else:
            txt_file.write("No Data")
        txt_file.write("\n\n")

        # (2) Futures 잔고 기록
        txt_file.write(f"-- {date_prefix}_futures_balance (CSV)\n")
        if futures_balance:
            fieldnames = list(futures_balance[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(futures_balance)
        else:
            txt_file.write("No Data")
        txt_file.write("\n\n")

        # (3) 캔들 데이터(보조지표 포함) 기록
        for interval, df in klines_dict.items():
            txt_file.write(f"-- {date_prefix}_{symbol}_{interval}_klines (CSV)\n")
            csv_str = df.to_csv(sep=';', index=False)
            txt_file.write(csv_str)
            txt_file.write("\n\n")

        # (4) 구글 뉴스 데이터 기록
        txt_file.write(f"-- {date_prefix}_googlenews (CSV)\n")
        if google_news_data:
            fieldnames = list(google_news_data[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(google_news_data)
        else:
            txt_file.write("No Data")
        txt_file.write("\n\n")


def save_klines_csv(client, folder, symbol, interval, limit, file_path):
    """
    바이낸스 선물에서 지정된 심볼과 간격(interval)의 캔들 데이터를 limit만큼 가져와 CSV 파일로 저장.
    OHLCV 데이터에 보조지표가 포함되어 있으며, Open Time은 한국 시간으로 변환됩니다.
    """
    # 원시 데이터 가져오기
    raw_klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
               "Close Time", "Quote Asset Volume", "Number of Trades",
               "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]
    df = pd.DataFrame(raw_klines, columns=columns)
    # 숫자형 컬럼 변환
    numeric_cols = ["Open", "High", "Low", "Close", "Volume",
                    "Quote Asset Volume", "Number of Trades",
                    "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # 시간 컬럼 변환 (한국 시간)
    df["Open Time"] = pd.to_datetime(df["Open Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
    # 보조지표 추가
    df = add_technical_indicators(df, interval)
    # CSV 저장
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"{symbol} {interval} 캔들 데이터 CSV 저장: {file_path}")


# ===============================================================
# 바이낸스 선물 관련 함수
# ===============================================================
def load_env_and_create_client():
    """
    환경 변수 로드 및 바이낸스 클라이언트 객체 생성 후 반환.
    """
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access, secret)
    client.API_URL = 'https://fapi.binance.com'  # USDT-마진 선물 엔드포인트
    return client


def get_nonzero_futures_balance(client):
    """
    선물 계좌잔고에서 0이 아닌 잔고만 추출하여 반환.
    """
    futures_balance = client.futures_account_balance()
    nonzero = [item for item in futures_balance if float(item["balance"]) != 0.0]
    return nonzero


def build_position_dataframe(client, symbol):
    """
    Position History 데이터를 DataFrame으로 빌드하고 반환.
    """
    cutoff_time = datetime.combine(datetime.today(), time.min)
    position_df = build_position_history(
        client=client,
        symbol=symbol,
        limit=500,
        cutoff_dt=cutoff_time
    )
    return position_df


# ===============================================================
# 메인 실행 함수
# ===============================================================
def main():
    # 1) 클라이언트 및 기본 변수 준비
    client = load_env_and_create_client()
    symbol = "BTCUSDT"
    date_prefix = datetime.now().strftime('%Y%m%d%H%M')

    # 2) 보고서 폴더 준비
    report_folder = "report_futures"
    prepare_directories(report_folder)

    # 3) 선물 잔고 및 포지션 히스토리 데이터 수집
    futures_balance = get_nonzero_futures_balance(client)
    position_df = build_position_dataframe(client, symbol)

    # 4) 구글 뉴스 데이터 수집 (예시: "Bitcoin" 관련 최대 10건)
    google_news_data = scrape_news([
        "Bitcoin price prediction", "Bitcoin volatility", "Bitcoin whale activity",
        "Bitcoin institutional adoption", "SEC Bitcoin ETF decision", "Bitcoin regulation",
        "Bitcoin mining difficulty", "Bitcoin network congestion", "US inflation CPI data",
        "Federal Reserve interest rates"
    ], max_articles_per_query=5, date_filter="w")

    # 5) OHLCV 데이터 수집 및 보조지표 추가
    intervals = ["1m", "5m", "15m", "1h"]
    candle_counts = {"1m": 180, "5m": 60, "15m": 24, "1h": 10}
    klines_dict = {}
    columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
               "Close Time", "Quote Asset Volume", "Number of Trades",
               "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]
    for interval in intervals:
        raw_klines = client.futures_klines(symbol=symbol, interval=interval, limit=candle_counts[interval])
        df = pd.DataFrame(raw_klines, columns=columns)
        # 숫자형 컬럼 변환
        numeric_cols = ["Open", "High", "Low", "Close", "Volume",
                        "Quote Asset Volume", "Number of Trades",
                        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # 시간 컬럼 변환 (한국 시간)
        df["Open Time"] = pd.to_datetime(df["Open Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
        df["Close Time"] = pd.to_datetime(df["Close Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
        # 보조지표 추가
        df = add_technical_indicators(df, interval)
        klines_dict[interval] = df

    # 6) CSV 파일 저장
    futures_balance_file = os.path.join(report_folder, f"{date_prefix}_futures_balance.csv")
    if futures_balance:
        futures_balance_fieldnames = list(futures_balance[0].keys())
    else:
        futures_balance_fieldnames = []
    save_to_csv(futures_balance_file, futures_balance, futures_balance_fieldnames)

    history_file = os.path.join(report_folder, f"{date_prefix}_{symbol}_position_history.csv")
    position_csv_data = position_df.to_dict('records')
    position_fieldnames = list(position_df.columns)
    save_to_csv(history_file, position_csv_data, position_fieldnames)

    news_file = os.path.join(report_folder, f"{date_prefix}_googlenews.csv")
    google_news_fieldnames = ['keyword', 'title', 'snippet', 'date', 'source', 'parsed_date']
    save_to_csv(news_file, google_news_data, google_news_fieldnames)

    # 7) report.txt 생성 (모든 데이터를 CSV 형식으로 기록)
    report_txt_file = os.path.join(report_folder, f"{date_prefix}_report.txt")
    save_report_txt(position_df, futures_balance, report_txt_file, date_prefix, symbol, klines_dict, google_news_data)

    # 8) 개별 캔들 CSV 파일 저장 (옵션)
    for interval in intervals:
        filename = f"{date_prefix}_{symbol}_{interval}_klines.csv"
        file_path = os.path.join(report_folder, filename)
        save_klines_csv(client, report_folder, symbol, interval, candle_counts[interval], file_path)

    print(f"\nCSV 파일 및 보고서가 생성되었습니다:\n- Position History: {history_file}\n- Futures Balance: {futures_balance_file}\n- Google News CSV: {news_file}\n- Report (TXT): {report_txt_file}")


if __name__ == "__main__":
    main()
