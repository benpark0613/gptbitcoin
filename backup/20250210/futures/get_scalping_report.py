# get_futures_report.py

import os
import csv
import logging
from datetime import datetime, timedelta, time
import pandas as pd
import pandas_ta as ta  # pandas-ta 추가
from dotenv import load_dotenv
from binance.client import Client

from module.clear_folder import clear_folder
from module.mbinance.closed_positions import (
    get_default_client,
    save_closed_position_csv,
    save_today_trade_stats_csv
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

EXTRA_ROWS = 50


import pandas as pd
import pandas_ta as ta

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    BTCUSDT ChatGPT Max-Potential Strategy 용 핵심 지표를 계산.
    EXTRA_ROWS=50개는 별도 로직에서 추가로 받아온 뒤 최종 제외 처리.
    """

    # 1) 단기 추세 (EMA)
    df['EMA9'] = ta.ema(close=df['Close'], length=9)
    df['EMA21'] = ta.ema(close=df['Close'], length=21)

    # 2) 변동성 지표 (ATR)
    df['ATR'] = ta.atr(high=df['High'], low=df['Low'], close=df['Close'], length=14)

    # 3) 모멘텀 지표 (RSI)
    df['RSI'] = ta.rsi(close=df['Close'], length=14)

    # 4) 추세 강도 (ADX)
    adx_data = ta.adx(high=df['High'], low=df['Low'], close=df['Close'], length=14)
    df['ADX'] = adx_data['ADX_14']

    # 5) Bollinger Bands (기간=20, 표준편차=2.0)
    bbands = ta.bbands(df['Close'], length=20, std=2.0)
    df['BBL'] = bbands['BBL_20_2.0']
    df['BBM'] = bbands['BBM_20_2.0']
    df['BBU'] = bbands['BBU_20_2.0']
    df['BBB'] = bbands['BBB_20_2.0']  # 볼린저 밴드 폭
    df['BBP'] = bbands['BBP_20_2.0']  # %B (%밴드 위치)

    # 6) 거래량 이동평균(20)
    df['VOL_MA20'] = ta.sma(df['Volume'], length=20)

    # 7) OBV(온밸런스 볼륨)
    df['OBV'] = ta.obv(close=df['Close'], volume=df['Volume'])

    # 소수점 처리
    df['EMA9'] = df['EMA9'].round(2)
    df['EMA21'] = df['EMA21'].round(2)
    df['ATR'] = df['ATR'].round(2)
    df['RSI'] = df['RSI'].round(2)
    df['ADX'] = df['ADX'].round(2)
    df['BBL'] = df['BBL'].round(2)
    df['BBM'] = df['BBM'].round(2)
    df['BBU'] = df['BBU'].round(2)
    df['BBB'] = df['BBB'].round(2)
    df['BBP'] = df['BBP'].round(4)
    df['VOL_MA20'] = df['VOL_MA20'].round(2)
    df['OBV'] = df['OBV'].round(2)

    return df


def parse_klines_data(raw_klines, extra_rows=EXTRA_ROWS):
    df = pd.DataFrame(raw_klines)
    df = df.iloc[:, 0:7]
    df.columns = ["Open Time", "Open", "High", "Low", "Close", "Volume", "Close Time"]

    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["Open Time"] = pd.to_datetime(df["Open Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit='ms', utc=True).dt.tz_convert("Asia/Seoul")

    # 보조지표 계산
    df = add_technical_indicators(df)

    # extra_rows 만큼의 데이터(가장 최근?)를 제외하고 제거
    df = df.iloc[extra_rows:].reset_index(drop=True)
    return df


def prepare_directories(report_path):
    try:
        clear_folder(report_path)
        if not os.path.exists(report_path):
            os.makedirs(report_path)
        logging.info("Report directory prepared: %s", report_path)
    except Exception as e:
        logging.error("Error preparing directories: %s", e)
        raise


def save_to_csv(file_path, data, fieldnames):
    """
    CSV 구분자 ',' 기본 사용
    """
    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.writer(f)
                writer.writerow(["No Data"])
        logging.info("CSV saved: %s", file_path)
    except Exception as e:
        logging.error("Error saving CSV %s: %s", file_path, e)
        raise


def save_report_txt(
        futures_balance,
        report_txt_file,
        symbol,
        klines_dict,
        intervals,
        today_trade_stats_csv=None,
        closed_position_csv=None
):
    """
    report.txt 파일에:
      - futures_balance (CSV)
      - today_trade_stats (CSV)
      - closed_position (CSV)
      - 각 interval klines (CSV)
    """
    try:
        with open(report_txt_file, 'w', encoding='utf-8', newline='') as txt_file:
            # 1) futures_balance
            txt_file.write("-- futures_balance (CSV)\n")
            if futures_balance:
                fieldnames = list(futures_balance[0].keys())
                writer = csv.DictWriter(txt_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(futures_balance)
            else:
                txt_file.write("No Data")
            txt_file.write("\n\n")

            # 2) today_trade_stats
            txt_file.write("-- today_trade_stats (CSV)\n")
            if today_trade_stats_csv and os.path.isfile(today_trade_stats_csv):
                with open(today_trade_stats_csv, 'r', encoding='utf-8') as stat_fp:
                    txt_file.write(stat_fp.read())  # CSV 내용 삽입
            else:
                txt_file.write("No Data\n")
            txt_file.write("\n\n")

            # 3) closed_position (CSV)
            txt_file.write("-- closed_position (CSV)\n")
            if closed_position_csv and os.path.isfile(closed_position_csv):
                with open(closed_position_csv, 'r', encoding='utf-8') as cp:
                    txt_file.write(cp.read())
            else:
                txt_file.write("No Data\n")
            txt_file.write("\n\n")

            # 4) 각 interval klines
            for interval in intervals:
                if interval in klines_dict:
                    txt_file.write(f"-- {symbol}_{interval}_klines (CSV)\n")
                    txt_file.write(klines_dict[interval].to_csv(index=False))
                    txt_file.write("\n\n")
                else:
                    txt_file.write(f"-- {symbol}_{interval}_klines: 데이터 없음\n\n")

        logging.info("Report TXT saved: %s", report_txt_file)
    except Exception as e:
        logging.error("Error saving report TXT %s: %s", report_txt_file, e)
        raise


def save_klines_csv(client, symbol, interval, limit, file_path):
    try:
        total_rows = limit + EXTRA_ROWS
        raw_klines = client.futures_klines(symbol=symbol, interval=interval, limit=total_rows)
        df = parse_klines_data(raw_klines, extra_rows=EXTRA_ROWS)
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info("%s %s 캔들 데이터 CSV 저장: %s", symbol, interval, file_path)
    except Exception as e:
        logging.error("Error saving klines CSV for %s %s: %s", symbol, interval, e)
        raise


def load_env_and_create_client():
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access, secret)
    client.API_URL = 'https://fapi.binance.com'
    logging.info("Binance client created")
    return client


def get_nonzero_futures_balance(client):
    try:
        futures_balance = client.futures_account_balance()
        # 0이 아닌 balance만 필터
        result = [item for item in futures_balance if float(item["balance"]) != 0.0]

        for item in result:
            # updateTime 처리
            if "updateTime" in item:
                utc_time = datetime.utcfromtimestamp(item["updateTime"] / 1000.0)
                seoul_time = utc_time + timedelta(hours=9)
                item["updateTime(UTC+9)"] = seoul_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                item["updateTime(UTC+9)"] = None

            # 소수점 둘째 자리로 반올림할 컬럼
            columns_to_round = [
                "balance",
                "crossWalletBalance",
                "crossUnPnl",
                "availableBalance",
                "maxWithdrawAmount"
            ]

            # 항목이 존재하면 반올림 처리
            for col in columns_to_round:
                if col in item:
                    item[col] = round(float(item[col]), 2)

        logging.info("Non-zero futures balance retrieved")
        return result
    except Exception as e:
        logging.error("Error retrieving futures balance: %s", e)
        raise



def fetch_klines_data(client, symbol, intervals_config, extra_rows=EXTRA_ROWS):
    """
    intervals_config 예시:
       {
         "1m":  {"rows":180},
         "5m":  {"rows":60},
         "15m": {"rows":24},
         "1h":  {"rows":48},
         "4h":  {"rows":30},
         "1d":  {"rows":14}
       }
    각 interval에 대해 parse_klines_data -> add_technical_indicators -> df 생성
    """
    klines_dict = {}
    for interval, cfg in intervals_config.items():
        try:
            total_rows = cfg['rows'] + extra_rows
            raw_klines = client.futures_klines(symbol=symbol, interval=interval, limit=total_rows)
            df = parse_klines_data(raw_klines, extra_rows=extra_rows)
            klines_dict[interval] = df
            logging.info("Fetched and parsed klines for interval: %s", interval)
        except Exception as e:
            logging.error("Error fetching klines for %s: %s", interval, e)
    return klines_dict


def save_reports(report_folder, symbol, futures_balance, klines_dict, intervals, today_trade_stats_csv=None,
                 closed_position_csv=None):
    """
    1) futures_balance -> CSV
    2) report.txt ->
       - futures_balance (CSV)
       - today_trade_stats (CSV)
       - closed_position (CSV)
       - 각 interval klines (CSV)
    """
    try:
        futures_balance_file = os.path.join(report_folder, "futures_balance.csv")
        futures_balance_fieldnames = list(futures_balance[0].keys()) if futures_balance else []
        save_to_csv(futures_balance_file, futures_balance, futures_balance_fieldnames)

        report_txt_file = os.path.join(report_folder, "report.txt")
        save_report_txt(
            futures_balance,
            report_txt_file,
            symbol,
            klines_dict,
            intervals,
            today_trade_stats_csv=today_trade_stats_csv,
            closed_position_csv=closed_position_csv
        )

        return futures_balance_file, report_txt_file
    except Exception as e:
        logging.error("Error saving reports: %s", e)
        raise


def save_individual_klines_csv(client, intervals_config, report_folder, symbol):
    """
    interval별로 parse_klines_data 결과를 CSV로 저장
    """
    for interval, cfg in intervals_config.items():
        try:
            filename = f"{interval}_klines.csv"
            file_path = os.path.join(report_folder, filename)
            save_klines_csv(client, symbol, interval, cfg['rows'], file_path)
        except Exception as e:
            logging.error("Error saving individual klines CSV for %s: %s", interval, e)


def main():
    try:
        client = load_env_and_create_client()
        symbol = "BTCUSDT"
        report_folder = "report_futures"
        prepare_directories(report_folder)

        # 1) 선물 잔고 조회
        futures_balance = get_nonzero_futures_balance(client)

        # 2) interval 설정
        intervals_config = {
            "1m": {"rows": 720},  # 1분봉 720개 → 약 12시간 분량
            "5m": {"rows": 144},  # 5분봉 144개 → 약 12시간 분량
            "15m": {"rows": 48},  # 15분봉 48개 → 약 12시간 분량
            "1h": {"rows": 24},  # 1시간봉 24개 → 약 1일 분량(옵션)
            "4h": {"rows": 6}  # 4시간봉 6개  → 약 1일(옵션: 큰 추세 확인용)
        }

        # 3) 캔들 데이터 수집 & 보조지표 계산
        klines_dict = fetch_klines_data(client, symbol, intervals_config, extra_rows=EXTRA_ROWS)

        # 4) closed_position.csv 생성 (오늘 청산 포지션)
        closed_position_csv_path = os.path.join(report_folder, "closed_position.csv")
        today_positions = save_closed_position_csv(client, symbol, closed_position_csv_path)
        logging.info(f"closed_position.csv created: {closed_position_csv_path}, count={len(today_positions)}")

        # 5) today_trade_stats.csv 생성 (당일 승률/손익비/프로핏 팩터 등)
        today_trade_stats_csv_path = os.path.join(report_folder, "today_trade_stats.csv")
        save_today_trade_stats_csv(today_positions, today_trade_stats_csv_path)
        logging.info(f"today_trade_stats.csv created: {today_trade_stats_csv_path}")

        # 6) report.txt + futures_balance.csv + {today_trade_stats, closed_position}, klines
        futures_balance_file, report_txt_file = save_reports(
            report_folder,
            symbol,
            futures_balance,
            klines_dict,
            list(intervals_config.keys()),
            today_trade_stats_csv=today_trade_stats_csv_path,
            closed_position_csv=closed_position_csv_path
        )

        # 7) interval별 개별 CSV 저장
        save_individual_klines_csv(client, intervals_config, report_folder, symbol)

        logging.info("\n[완료] CSV 및 보고서 생성:\n"
                     f"- Futures balance: {futures_balance_file}\n"
                     f"- Today trade stats: {today_trade_stats_csv_path}\n"
                     f"- Closed Position: {closed_position_csv_path}\n"
                     f"- Report (TXT): {report_txt_file}")

    except Exception as e:
        logging.error("Error in main execution: %s", e)
        raise


if __name__ == "__main__":
    main()
