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
            new_item = str(int(float(item)))
        except (ValueError, TypeError):
            new_item = item
        processed.append(new_item)
    return processed


def write_report_txt(position_df, nonzero_futures_balance, report_txt_file, date_prefix, symbol, klines_dict, google_news_data):
    import pandas as pd
    import csv
    from datetime import datetime
    from zoneinfo import ZoneInfo

    # DataFrame을 dict로 변환하고, pd.Timestamp는 문자열로 변환
    history_records = position_df.to_dict(orient="records")
    for record in history_records:
        for key, value in record.items():
            if isinstance(value, pd.Timestamp):
                record[key] = value.strftime('%Y-%m-%d %H:%M:%S')

    # 최종적으로 보고서에 기록할 컬럼 순서를 지정
    report_order = [
        "openedTime", "closedTime", "symbol", "mode", "direction", "entryPrice",
        "avgClosePrice", "closingPnL", "CumulativeROI", "totalPnL", "DailyROI",
        "TradePnLRatio", "maxOpenInterest", "closedVol"
    ]

    # 현재 포지션과 과거 거래를 구분하며 최신 닫힌 거래의 지표들을 추출
    current_position = []
    past_trade_history = []
    latest_total_pnl = None
    latest_daily_roi = None
    latest_cumulative_roi = None
    latest_closed_time = None

    for record in history_records:
        # report_order 순서대로 데이터 딕셔너리 생성
        position_data = {key: record.get(key) for key in report_order}
        if pd.isna(record.get("closedTime")):
            current_position.append(position_data)
        else:
            past_trade_history.append(position_data)
            if latest_closed_time is None or record["closedTime"] > latest_closed_time:
                latest_closed_time = record["closedTime"]
                latest_total_pnl = record.get("totalPnL")
                latest_daily_roi = record.get("DailyROI")
                latest_cumulative_roi = record.get("CumulativeROI")

    with open(report_txt_file, 'w', encoding='utf-8', newline='') as txt_file:
        # (2) 최신 닫힌 거래의 지표들 기록
        txt_file.write(f"-- Current totalPnL ; {latest_total_pnl}\n")
        txt_file.write(f"-- Current DailyROI ; {latest_daily_roi}\n")
        txt_file.write(f"-- Current CumulativeROI ; {latest_cumulative_roi}\n\n")

        # (1) Current Position 섹션
        txt_file.write(f"-- {date_prefix}_Current_Position (CSV)\n")
        if current_position:
            writer = csv.DictWriter(txt_file, fieldnames=report_order, delimiter=';')
            writer.writeheader()
            writer.writerows(current_position)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")

        # (3) Past Trade History 섹션
        txt_file.write(f"-- {date_prefix}_Past_Trade_History (CSV)\n")
        if past_trade_history:
            writer = csv.DictWriter(txt_file, fieldnames=report_order, delimiter=';')
            writer.writeheader()
            writer.writerows(past_trade_history)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")

        # (4) Futures Balance 섹션
        txt_file.write(f"-- {date_prefix}_futures_balance (CSV)\n")
        if nonzero_futures_balance:
            fieldnames = list(nonzero_futures_balance[0].keys())
            writer = csv.DictWriter(txt_file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(nonzero_futures_balance)
        else:
            txt_file.write("No Data\n")
        txt_file.write("\n")

        # (5) 캔들 데이터 섹션
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

        # (6) 구글 뉴스 데이터 섹션
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

    # ---------------------------
    # [추가] 개별 거래 ROI 계산 (TradePnLRatio)
    def compute_trade_pnl_ratio(row):
        try:
            if pd.notnull(row["closedVol"]) and float(row["closedVol"]) != 0 and row["entryPrice"] and float(row["entryPrice"]) != 0:
                return round((row["closingPnL"] / (float(row["entryPrice"]) * float(row["closedVol"]))) * 100, 2)
        except Exception:
            return None
        return None

    position_df["TradePnLRatio"] = position_df.apply(compute_trade_pnl_ratio, axis=1)
    # ---------------------------

    # ---------------------------
    # [추가] 누적 수익율 (Cumulative ROI) 계산
    # 개별 거래에 trade_id 부여
    position_df = position_df.reset_index().rename(columns={'index': 'trade_id'})
    closed_df = position_df[position_df["closedTime"].notna()].copy()

    def compute_notional(row):
        try:
            if pd.notnull(row["closedVol"]) and row["entryPrice"] and float(row["entryPrice"]) != 0:
                return float(row["entryPrice"]) * float(row["closedVol"])
        except Exception:
            return 0
        return 0

    closed_df["notional"] = closed_df.apply(compute_notional, axis=1)
    closed_df.sort_values("closedTime", ascending=True, inplace=True)
    closed_df["cumulativeNotional"] = closed_df["notional"].cumsum()
    closed_df["CumulativePnLRatio"] = closed_df.apply(
        lambda row: round((row["totalPnL"] / row["cumulativeNotional"] * 100), 2)
        if row["cumulativeNotional"] != 0 else 0, axis=1
    )
    # 누적 수익율을 "CumulativeROI" 컬럼으로 저장
    position_df = position_df.merge(closed_df[["trade_id", "CumulativePnLRatio"]], on="trade_id", how="left")
    position_df["CumulativeROI"] = position_df["CumulativePnLRatio"].fillna("")
    # ---------------------------

    # ---------------------------
    # [추가] 일일 ROI 계산
    # USDT 잔고에서 현재 잔고를 가져옴 (대부분 USDT 계좌 사용 가정)
    usdt_balance = None
    for item in nonzero_futures_balance:
        if item.get("asset") == "USDT":
            usdt_balance = float(item.get("balance", 0))
            break
    if usdt_balance is None and nonzero_futures_balance:
        usdt_balance = float(nonzero_futures_balance[0].get("balance", 0))

    closed_trades = position_df[position_df["closedTime"].notna()]
    if not closed_trades.empty:
        daily_total_pnl = closed_trades["closingPnL"].fillna(0).sum()
    else:
        daily_total_pnl = 0

    initial_balance = usdt_balance - daily_total_pnl
    if initial_balance != 0:
        daily_roi = round((daily_total_pnl / initial_balance) * 100, 2)
    else:
        daily_roi = 0
    position_df["DailyROI"] = daily_roi
    # ---------------------------

    # ---------------------------
    # 최종 컬럼 순서 재정렬 (CSV 및 report.txt에 반영)
    final_order = [
        "openedTime", "closedTime", "symbol", "mode", "direction", "entryPrice",
        "avgClosePrice", "closingPnL", "CumulativeROI", "totalPnL", "DailyROI",
        "TradePnLRatio", "maxOpenInterest", "closedVol"
    ]
    final_order = [col for col in final_order if col in position_df.columns]
    position_df = position_df[final_order]
    # ---------------------------

    # 4) 각 캔들 간격별 데이터(5m, 15m, 1h)를 API로 가져와 딕셔너리에 저장
    intervals = ["5m", "15m", "1h"]
    candle_counts = {"5m": 576, "15m": 672, "1h": 336}
    klines_dict = {}
    for interval in intervals:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=candle_counts[interval])
        klines_dict[interval] = klines

    # 5) 구글 뉴스 데이터 가져오기 (query: "Bitcoin")
    google_news_data = get_latest_10_articles(query="Bitcoin")
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
    write_report_txt(position_df, nonzero_futures_balance, report_txt_file, date_prefix, symbol, klines_dict,
                     google_news_data)

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
    print(f"일일 ROI (DailyROI): {daily_roi:.2f}%")


if __name__ == "__main__":
    main()
