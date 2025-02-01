import os
import csv
import json
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from module.clear_folder import clear_folder
from module.binance.position_history import build_position_history


def main():
    # 1) 환경 변수 로드 및 바이낸스 클라이언트 연결
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access, secret)
    client.API_URL = 'https://fapi.binance.com'  # USDT-마진 선물 엔드포인트

    # 2) 심볼, 기본 설정
    symbol = "BTCUSDT"

    # 3) 선물 계좌잔고, 포지션 정보 가져오기
    futures_balance = client.futures_account_balance()

    # 4) 저장 폴더 경로 지정
    report_path = "report_day"
    # 파일 이름 접두어(년월일시분)
    date_prefix = datetime.now().strftime('%Y%m%d%H%M')

    # 폴더 내부 파일/폴더 비우기
    clear_folder(report_path)

    # 폴더가 없으면 생성
    if not os.path.exists(report_path):
        os.makedirs(report_path)

    # 5) 잔고(미사용, 또는 0이 아닌 것만) CSV 저장
    nonzero_futures_balance = []
    for item in futures_balance:
        if float(item["balance"]) != 0.0:
            nonzero_futures_balance.append(item)

    # 6) Position History 데이터
    cutoff_time = datetime(2025, 2, 1, 16, 0, 0)
    position_df = build_position_history(
        client=client,  # 클라이언트 객체
        symbol=symbol,  # 조회 심볼
        limit=500,  # 원하는 limit 값
        cutoff_dt=cutoff_time
    )

    # CSV 파일 저장
    futures_balance_file = os.path.join(report_path, f"{date_prefix}_futures_balance.csv")
    with open(futures_balance_file, 'w', newline='', encoding='utf-8') as f:
        if nonzero_futures_balance:
            writer = csv.DictWriter(f, fieldnames=nonzero_futures_balance[0].keys())
            writer.writeheader()
            writer.writerows(nonzero_futures_balance)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])

    history_file = os.path.join(report_path, f"{date_prefix}_{symbol}_history.csv")
    position_df.to_csv(history_file, index=False, encoding="utf-8-sig")

    # 7) JSON 형식으로 텍스트 파일에 저장
    report_txt_file = os.path.join(report_path, f"{date_prefix}_report.txt")
    with open(report_txt_file, 'w', encoding='utf-8') as txt_file:
        # (1) Position History를 JSON 형식으로 변환
        history_json_content = position_df.to_dict(orient="records")

        # pandas.Timestamp를 문자열로 변환
        for record in history_json_content:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.strftime('%Y-%m-%d %H:%M:%S')

        # (2) Futures Balance를 JSON 형식으로 변환
        futures_balance_json_content = nonzero_futures_balance if nonzero_futures_balance else [{"No Data": "No Data"}]

        # (3) Current Position과 Past Trade History로 분리
        current_position = []
        past_trade_history = []
        current_total_pnl = None  # Current totalPnL 값 추가

        # 가장 최근의 totalPnL을 찾기 위한 변수
        latest_total_pnl = None
        latest_closed_time = None

        for record in history_json_content:
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

            if record.get("closedTime"):  # 청산된 포지션
                past_trade_history.append(position_data)
                # 가장 최신의 closedTime과 totalPnL 값을 확인하여 최신 totalPnL을 업데이트
                if latest_closed_time is None or record["closedTime"] > latest_closed_time:
                    latest_closed_time = record["closedTime"]
                    latest_total_pnl = record.get("totalPnL")
            else:  # 열린 포지션
                current_position.append(position_data)

        # 가장 최근의 totalPnL을 Current totalPnl에 추가
        current_total_pnl = latest_total_pnl if latest_total_pnl is not None else "None"

        # (4) JSON 파일에 Current totalPnl 추가
        json_report = {
            "Current Position": current_position if current_position else "None",
            "Current totalPnl": current_total_pnl,
            "Past Trade History": past_trade_history
        }

        # (5) 파일에 작성
        txt_file.write(f"-- {date_prefix}_BTCUSDT_history.json\n")
        json.dump(json_report, txt_file, ensure_ascii=False, indent=4)

        txt_file.write(f"\n-- {date_prefix}_futures_balance.json\n")
        json.dump(futures_balance_json_content, txt_file, ensure_ascii=False, indent=4)

    print(f"\nCSV files and JSON report saved:")
    print(f"BTCUSDT History: {history_file}")
    print(f"Futures Balance: {futures_balance_file}")
    print(f"Report: {report_txt_file}")


if __name__ == "__main__":
    main()
