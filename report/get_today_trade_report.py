import os
import csv
import logging
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta  # pandas-ta 추가
from dotenv import load_dotenv
from binance.client import Client

from module.clear_folder import clear_folder
from module.mbinance.closed_positions import (
    save_closed_position_csv,
    save_today_trade_stats_csv
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def prepare_directories(report_path):
    """
    보고서 폴더 초기화 및 생성
    """
    try:
        clear_folder(report_path)  # 기존 파일 삭제
        if not os.path.exists(report_path):
            os.makedirs(report_path)
        logging.info("Report directory prepared: %s", report_path)
    except Exception as e:
        logging.error("Error preparing directories: %s", e)
        raise


def save_to_csv(file_path, data, fieldnames):
    """
    CSV 파일로 저장
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


def load_env_and_create_client():
    """
    ENV 로드 후 바이낸스 선물 클라이언트 생성
    """
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access, secret)
    client.API_URL = 'https://fapi.binance.com'
    logging.info("Binance client created")
    return client


def get_nonzero_futures_balance(client):
    """
    잔고 조회 (0이 아닌 항목만 필터링)
    """
    try:
        futures_balance = client.futures_account_balance()
        # 0이 아닌 balance만 필터
        result = [item for item in futures_balance if float(item["balance"]) != 0.0]

        for item in result:
            # updateTime 처리 (UTC+9 변환)
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


def main():
    try:
        client = load_env_and_create_client()
        symbol = "BTCUSDT"
        report_folder = "0.today_stats"
        prepare_directories(report_folder)

        # 1) 선물 잔고 조회
        futures_balance = get_nonzero_futures_balance(client)
        # 파일로 저장을 원한다면 예: save_to_csv(...) 등 활용 가능

        # 2) 오늘 청산된 포지션 CSV 생성
        closed_position_csv_path = os.path.join(report_folder, "closed_position.csv")
        today_positions = save_closed_position_csv(client, symbol, closed_position_csv_path)
        logging.info(f"closed_position.csv created: {closed_position_csv_path}, count={len(today_positions)}")

        # 3) 오늘 거래 통계 생성 (승률, 손익비, 프로핏 팩터 등 + 확장 지표)
        today_trade_stats_csv_path = os.path.join(report_folder, "today_trade_stats.csv")
        save_today_trade_stats_csv(today_positions, today_trade_stats_csv_path)
        logging.info(f"today_trade_stats.csv created: {today_trade_stats_csv_path}")

    except Exception as e:
        logging.error("Error in main execution: %s", e)
        raise


if __name__ == "__main__":
    main()
