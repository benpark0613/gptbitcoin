import os
import csv
from datetime import datetime, time
from zoneinfo import ZoneInfo  # Python 3.9 이상 필요

import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from module.clear_folder import clear_folder
from module.mbinance.position_history import build_position_history
from module.get_googlenews import get_latest_10_articles  # 구글 뉴스 데이터 모듈 임포트


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


def prepare_directories(report_path):
    """
    보고서(리포트) 저장 폴더를 초기화하고, 폴더가 없으면 생성.
    """
    clear_folder(report_path)
    if not os.path.exists(report_path):
        os.makedirs(report_path)


def get_nonzero_futures_balance(client):
    """
    선물 계좌잔고에서 0이 아닌 잔고만 추출하여 반환.
    """
    futures_balance = client.futures_account_balance()
    nonzero_futures_balance = [
        item for item in futures_balance if float(item["balance"]) != 0.0
    ]
    return nonzero_futures_balance


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


def save_futures_balance_csv(nonzero_futures_balance, file_path):
    """
    nonzero_futures_balance 리스트를 CSV 파일로 저장.
    """
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        if nonzero_futures_balance:
            writer = csv.DictWriter(f, fieldnames=nonzero_futures_balance[0].keys())
            writer.writeheader()
            writer.writerows(nonzero_futures_balance)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])


def save_position_csv(position_df, file_path):
    """
    포지션 히스토리 DataFrame을 CSV 파일로 저장.
    """
    position_df.to_csv(file_path, index=False, encoding="utf-8-sig")


def save_google_news_csv(google_news_data, file_path):
    """
    구글 뉴스 데이터를 CSV 파일로 저장.
    구글 뉴스 데이터는 리스트 형식이며, 각 요소는 뉴스 기사 정보를 담은 dict입니다.
    """
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        if google_news_data:
            fieldnames = list(google_news_data[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(google_news_data)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])


def remove_decimals(row):
    """
    row 리스트 내의 각 항목에 대해, 숫자형 값이면 소수점 이하를 제거한 정수 문자열로 반환.
    숫자가 아닌 경우 그대로 반환합니다.
    """
    processed = []
    for item in row:
        try:
            # float 변환 후 int로 캐스팅하면 소수점 이하 제거됨
            new_item = str(int(float(item)))
        except (ValueError, TypeError):
            new_item = item
        processed.append(new_item)
    return processed


def write_report_txt(position_df, nonzero_futures_balance, report_txt_file, date_prefix, symbol, klines_dict, google_news_data):
    """
    report.txt 파일에 아래 항목들을 CSV 형태(구분자 ;)
    1) Current Position,
    2) Current totalPnL,
    3) Past Trade History,
    4) Futures Balance,
    5) 캔들 데이터(각 구간별: 5m, 15m, 1h),
    6) 구글 뉴스 데이터
    를 순서대로 작성.
    """
    import pandas as pd
    import csv
    from datetime import datetime
    from zoneinfo import ZoneInfo
    # position_df -> dict 변환 및 pd.Timestamp 문자열 변환
    history_records = position_df.to_dict(orient="records")
    for record in history_records:
        for key, value in record.items():
            if isinstance(value, pd.Timestamp):
                record[key] = value.strftime('%Y-%m-%d %H:%M:%S')

    # Current / Past 구분
    current_position = []
    past_trade_history = []
    latest_total_pnl = None
    latest_closed_time = None

    for record in history_records:
        position_data = {
            "symbol": record.get("symbol"),
            "mode": record.get("mode"),
            "direction": record.get("direction"),
            "entryPrice": record.get("entryPrice"),
            "avgClosePrice": record.get("avgClosePrice"),
            "closingPnL": record.get("closingPnL"),
            "maxOpenInterest": record.get("maxOpenInterest"),
            "closedVol": record.get("closedVol"),
            "openedTime": record.get("openedTime"),
            "closedTime": record.get("closedTime"),
            "totalPnL": record.get("totalPnL")
        }
        # pd.isna()를 사용하여 closedTime이 누락된 경우(열린 포지션)로 분류
        if pd.isna(record.get("closedTime")):
            current_position.append(position_data)
        else:
            past_trade_history.append(position_data)
            if latest_closed_time is None or record["closedTime"] > latest_closed_time:
                latest_closed_time = record["closedTime"]
                latest_total_pnl = record.get("totalPnL")

    with open(report_txt_file, 'w', encoding='utf-8', newline='') as txt_file:
        # (1) Current Position
        txt_file.write(f"-- {date_prefix}_Current_Position (CSV)\n")
        if current_position:
            fieldnames = list(current_position[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(current_position)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")

        # (2) Current totalPnL (바로 Current Position 다음에 기록)
        txt_file.write(f"-- Current totalPnL ; {latest_total_pnl}\n\n")

        # (3) Past Trade History
        txt_file.write(f"-- {date_prefix}_Past_Trade_History (CSV)\n")
        if past_trade_history:
            fieldnames = list(past_trade_history[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(past_trade_history)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")

        # (4) Futures Balance
        txt_file.write(f"-- {date_prefix}_futures_balance (CSV)\n")
        if nonzero_futures_balance:
            fieldnames = list(nonzero_futures_balance[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(nonzero_futures_balance)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")

        # (5) 캔들 데이터 (각 구간별: 5m, 15m, 1h)
        for interval, klines in klines_dict.items():
            txt_file.write(f"-- {date_prefix}_{symbol}_{interval}_klines (CSV)\n")
            headers = ["Readable Time", "Open Time", "Open", "High", "Low", "Close", "Volume",
                       "Close Time", "Quote Asset Volume", "Number of Trades",
                       "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]
            writer = csv.writer(txt_file, delimiter=';')
            writer.writerow(headers)
            for row in klines:
                open_time_ms = row[0]
                readable_time = datetime.fromtimestamp(float(open_time_ms) / 1000, tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
                processed_row = remove_decimals(row)
                writer.writerow([readable_time] + processed_row)
            txt_file.write("\n")

        # (6) 구글 뉴스 데이터
        txt_file.write(f"-- {date_prefix}_Google_News (CSV)\n")
        if google_news_data:
            headers = list(google_news_data[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=headers, delimiter=';')
            writer.writeheader()
            writer.writerows(google_news_data)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")


def save_klines_csv(client, symbol, interval, limit, file_path):
    """
    바이낸스 선물에서 지정된 심볼과 간격(interval)의 캔들 데이터를 limit만큼 가져와 CSV 파일로 저장.
    Open Time 왼쪽에 한국시간(Asia/Seoul)의 사람이 읽을 수 있는 타임스탬프("Readable Time")를 추가하며,
    모든 숫자형 값은 소수점 이하를 제거하여 저장합니다.
    """
    klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    original_headers = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                        "Close Time", "Quote Asset Volume", "Number of Trades",
                        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]
    headers = ["Readable Time"] + original_headers

    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in klines:
            open_time_ms = row[0]
            readable_time = datetime.fromtimestamp(float(open_time_ms) / 1000, tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
            processed_row = remove_decimals(row)
            writer.writerow([readable_time] + processed_row)


def main():
    # 1) 클라이언트 준비
    client = load_env_and_create_client()
    symbol = "BTCUSDT"
    date_prefix = datetime.now().strftime('%Y%m%d%H%M')


    # 2) 폴더 세팅
    report_path = "report_day"
    prepare_directories(report_path)

    # 3) 선물 잔고 및 포지션 히스토리 데이터 가져오기
    nonzero_futures_balance = get_nonzero_futures_balance(client)
    position_df = build_position_dataframe(client, symbol)

    # 4) 각 캔들 간격별 데이터(5m, 15m, 1h)를 API로 가져와 딕셔너리에 저장
    intervals = ["5m", "15m", "1h"]
    candle_counts = {
        "5m": 576,
        "15m": 672,
        "1h": 336
    }
    klines_dict = {}
    for interval in intervals:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=candle_counts[interval])
        klines_dict[interval] = klines

    # 5) 구글 뉴스 데이터 가져오기 (query: "Bitcoin")
    google_news_data = get_latest_10_articles(query="Bitcoin")
    # google_news_data = []

    # 6) CSV 저장 경로 설정
    futures_balance_file = os.path.join(report_path, f"{date_prefix}_futures_balance.csv")
    history_file = os.path.join(report_path, f"{date_prefix}_{symbol}_history.csv")
    report_txt_file = os.path.join(report_path, f"{date_prefix}_report.txt")
    google_news_file = os.path.join(report_path, f"{date_prefix}_google_news.csv")

    # 7) 선물 잔고, 포지션 히스토리, 구글 뉴스 CSV 저장
    save_futures_balance_csv(nonzero_futures_balance, futures_balance_file)
    save_position_csv(position_df, history_file)
    save_google_news_csv(google_news_data, google_news_file)

    # 8) report.txt 작성 (캔들 데이터 및 구글 뉴스 포함)
    write_report_txt(position_df, nonzero_futures_balance, report_txt_file, date_prefix, symbol, klines_dict, google_news_data)

    # 9) 별도 캔들 CSV 파일 저장 (옵션)
    for interval in intervals:
        klines_file = os.path.join(report_path, f"{date_prefix}_{symbol}_{interval}_klines.csv")
        save_klines_csv(client, symbol, interval, candle_counts[interval], klines_file)
        print(f"{symbol} {interval} 캔들 데이터 CSV 저장: {klines_file}")

    print(f"\nCSV files and report saved:")
    print(f"BTCUSDT History: {history_file}")
    print(f"Futures Balance: {futures_balance_file}")
    print(f"Google News CSV: {google_news_file}")
    print(f"Report (TXT): {report_txt_file}")


if __name__ == "__main__":
    main()
