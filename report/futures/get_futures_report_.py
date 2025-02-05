import os
import csv
from datetime import datetime, time
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from module.clear_folder import clear_folder
from module.mbinance.position_history import build_position_history
from module.get_googlenews import scrape_news  # 필요 시 사용

# ===============================================================
# 보조지표 추가 함수
# ===============================================================
def add_technical_indicators(df, key):
    """
    OHLCV DataFrame에 보조지표를 추가합니다.
      - RSI (14)
      - MACD (EMA12, EMA26, Signal 9)
      - Bollinger Bands (20, 표준편차 2)
      - 이동평균선 (MA20, MA50)
    소수점 이하 2자리까지 반올림하여 출력합니다.
    """
    # RSI (14)
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = (100 - (100 / (1 + rs))).round(2)

    # MACD (12, 26, 9)
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = (ema12 - ema26).round(2)
    df['MACD_signal'] = (df['MACD'].ewm(span=9, adjust=False).mean()).round(2)
    df['MACD_hist'] = (df['MACD'] - df['MACD_signal']).round(2)

    # Bollinger Bands (20, 2)
    bb_middle = df['Close'].rolling(window=20).mean()
    bb_std = df['Close'].rolling(window=20).std()
    df['BB_middle'] = bb_middle.round(2)
    df['BB_std'] = bb_std.round(2)
    df['BB_upper'] = (bb_middle + (2 * bb_std)).round(2)
    df['BB_lower'] = (bb_middle - (2 * bb_std)).round(2)

    # 이동평균선 (MA20, MA50)
    df['MA20'] = df['Close'].rolling(window=20).mean().round(2)
    df['MA50'] = df['Close'].rolling(window=50).mean().round(2)

    return df


# ===============================================================
# 파일 및 폴더 관련 함수
# ===============================================================
def prepare_directories(report_path):
    """
    보고서 저장 폴더 초기화 및 생성
    """
    clear_folder(report_path)
    if not os.path.exists(report_path):
        os.makedirs(report_path)


def save_to_csv(file_path, data, fieldnames):
    """
    data를 CSV 파일로 저장
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
    report.txt 파일에 여러 데이터를 CSV 형식으로 기록
    """
    with open(report_txt_file, 'w', encoding='utf-8', newline='') as txt_file:
        # 포지션 히스토리
        txt_file.write(f"-- {date_prefix}_position_history (CSV)\n")
        txt_file.write(position_df.to_csv(sep=';', index=False) if not position_df.empty else "No Data")
        txt_file.write("\n\n")

        # Futures 잔고
        txt_file.write(f"-- {date_prefix}_futures_balance (CSV)\n")
        if futures_balance:
            fieldnames = list(futures_balance[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(futures_balance)
        else:
            txt_file.write("No Data")
        txt_file.write("\n\n")

        # 캔들 데이터 (보조지표 포함)
        for interval, df in klines_dict.items():
            txt_file.write(f"-- {date_prefix}_{symbol}_{interval}_klines (CSV)\n")
            txt_file.write(df.to_csv(sep=';', index=False))
            txt_file.write("\n\n")

        # 구글 뉴스 데이터
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
    바이낸스 선물 캔들 데이터를 보조지표와 함께 CSV 파일로 저장
    """
    extra_rows = 50
    total_rows = limit + extra_rows

    raw_klines = client.futures_klines(symbol=symbol, interval=interval, limit=total_rows)
    columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
               "Close Time", "Quote Asset Volume", "Number of Trades",
               "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]
    df = pd.DataFrame(raw_klines, columns=columns)

    numeric_cols = ["Open", "High", "Low", "Close", "Volume",
                    "Quote Asset Volume", "Number of Trades",
                    "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["Open Time"] = pd.to_datetime(df["Open Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")

    df = add_technical_indicators(df, interval)
    df = df.iloc[extra_rows:].reset_index(drop=True)

    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"{symbol} {interval} 캔들 데이터 CSV 저장: {file_path}")


# ===============================================================
# 바이낸스 선물 관련 함수
# ===============================================================
def load_env_and_create_client():
    """
    환경 변수 로드 및 바이낸스 클라이언트 생성
    """
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access, secret)
    client.API_URL = 'https://fapi.binance.com'
    return client


def get_nonzero_futures_balance(client):
    """
    선물 계좌에서 잔고가 0이 아닌 항목 반환
    """
    futures_balance = client.futures_account_balance()
    return [item for item in futures_balance if float(item["balance"]) != 0.0]


def build_position_dataframe(client, symbol):
    """
    포지션 히스토리를 DataFrame으로 구성
    """
    cutoff_time = datetime.combine(datetime.today(), time.min)
    return build_position_history(client=client, symbol=symbol, limit=500, cutoff_dt=cutoff_time)


# ===============================================================
# 데이터 수집 및 저장 관련 함수 (메인 실행부 분리)
# ===============================================================
def fetch_klines_data(client, symbol, intervals, candle_counts, extra_rows=50):
    """
    각 interval 별로 추가 데이터를 포함하여 캔들 데이터를 수집한 후 보조지표를 추가하고,
    추가 데이터는 제거한 DataFrame들을 반환합니다.
    """
    columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
               "Close Time", "Quote Asset Volume", "Number of Trades",
               "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]
    klines_dict = {}
    numeric_cols = ["Open", "High", "Low", "Close", "Volume",
                    "Quote Asset Volume", "Number of Trades",
                    "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume"]

    for interval in intervals:
        total_rows = candle_counts[interval] + extra_rows
        raw_klines = client.futures_klines(symbol=symbol, interval=interval, limit=total_rows)
        df = pd.DataFrame(raw_klines, columns=columns)

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df["Open Time"] = pd.to_datetime(df["Open Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
        df["Close Time"] = pd.to_datetime(df["Close Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")

        df = add_technical_indicators(df, interval)
        df = df.iloc[extra_rows:].reset_index(drop=True)
        klines_dict[interval] = df

    return klines_dict


def save_reports(report_folder, date_prefix, symbol, futures_balance, position_df, google_news_data, klines_dict):
    """
    여러 CSV 파일 및 report.txt 파일을 저장하고 각 파일의 경로를 반환합니다.
    """
    futures_balance_file = os.path.join(report_folder, f"{date_prefix}_futures_balance.csv")
    futures_balance_fieldnames = list(futures_balance[0].keys()) if futures_balance else []
    save_to_csv(futures_balance_file, futures_balance, futures_balance_fieldnames)

    history_file = os.path.join(report_folder, f"{date_prefix}_{symbol}_position_history.csv")
    position_csv_data = position_df.to_dict('records')
    position_fieldnames = list(position_df.columns)
    save_to_csv(history_file, position_csv_data, position_fieldnames)

    news_file = os.path.join(report_folder, f"{date_prefix}_googlenews.csv")
    google_news_fieldnames = ['keyword', 'title', 'snippet', 'date', 'source', 'parsed_date']
    save_to_csv(news_file, google_news_data, google_news_fieldnames)

    report_txt_file = os.path.join(report_folder, f"{date_prefix}_report.txt")
    save_report_txt(position_df, futures_balance, report_txt_file, date_prefix, symbol, klines_dict, google_news_data)

    return futures_balance_file, history_file, news_file, report_txt_file


def save_individual_klines_csv(client, intervals, candle_counts, report_folder, date_prefix, symbol):
    """
    각 interval 별로 개별 캔들 CSV 파일을 저장합니다.
    """
    for interval in intervals:
        filename = f"{date_prefix}_{symbol}_{interval}_klines.csv"
        file_path = os.path.join(report_folder, filename)
        save_klines_csv(client, report_folder, symbol, interval, candle_counts[interval], file_path)


# ===============================================================
# Main 실행부 (리팩토링 후 깔끔한 구조)
# ===============================================================
def main():
    client = load_env_and_create_client()
    symbol = "BTCUSDT"
    date_prefix = datetime.now().strftime('%Y%m%d%H%M')
    report_folder = "report_futures"
    prepare_directories(report_folder)

    futures_balance = get_nonzero_futures_balance(client)
    position_df = build_position_dataframe(client, symbol)
    # google_news_data = scrape_news([
    #     "Bitcoin", "Bitcoin price prediction", "Bitcoin volatility", "Bitcoin whale activity",
    #     "Bitcoin institutional adoption", "SEC Bitcoin ETF decision", "Bitcoin regulation",
    #     "Bitcoin mining difficulty", "Bitcoin network congestion", "US inflation CPI data",
    #     "Federal Reserve interest rates"
    # ], max_articles_per_query=5, date_filter="w")
    # 뉴스 사용 잠시 안하는 용도, 지우지 말 것
    google_news_data = []

    intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]
    candle_counts = {
        "1m": 180,  # 3시간
        "5m": 60,   # 5시간
        "15m": 24,  # 6시간
        "1h": 10,   # 10시간
        "4h": 30,   # 5일 (120시간)
        "1d": 14    # 2주 (14일)
    }

    # 캔들 데이터 및 보조지표 수집 (추가 데이터 포함)
    klines_dict = fetch_klines_data(client, symbol, intervals, candle_counts, extra_rows=50)

    # CSV 파일 및 report.txt 저장
    futures_balance_file, history_file, news_file, report_txt_file = save_reports(
        report_folder, date_prefix, symbol,
        futures_balance, position_df, google_news_data, klines_dict
    )

    # 개별 캔들 CSV 파일 저장
    save_individual_klines_csv(client, intervals, candle_counts, report_folder, date_prefix, symbol)

    print(f"\nCSV 파일 및 보고서가 생성되었습니다:\n- Position History: {history_file}\n- Futures Balance: {futures_balance_file}\n- Google News CSV: {news_file}\n- Report (TXT): {report_txt_file}")


if __name__ == "__main__":
    main()
