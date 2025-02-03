import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from binance.client import Client

from module.mbinance.position_history import build_position_history
from module.clear_folder import clear_folder
from module.get_googlenews import get_latest_10_articles
from module.get_rss_google_new import get_top_10_recent_news


def main():
    # 1) 환경 변수 로드 및 바이낸스 클라이언트 연결
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access, secret)
    client.API_URL = 'https://fapi.binance.com'  # USDT-마진 선물 엔드포인트

    # 2) 심볼, 기본 설정
    symbol = "BTCUSDT"
    orderbook_limit = 100

    # 3) 선물 계좌잔고, 포지션 정보, 미체결 주문, 오더북, 뉴스 리스트 가져오기
    futures_balance = client.futures_account_balance()
    positions = client.futures_position_information()
    open_orders = client.futures_get_open_orders(symbol=symbol)
    orderbook = client.futures_order_book(symbol=symbol, limit=orderbook_limit)

    # # (A) 구글 뉴스 최신 10개 기사
    # full_news_list = get_latest_10_articles("Bitcoin")
    # # (B) RSS로부터 최근 10개 뉴스
    # rss_news_list = get_top_10_recent_news("https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US")

    # 4) 저장 폴더 경로 지정
    report_path = "../../report/futures/report_day"
    # 파일 이름 접두어(년월일시분)
    date_prefix = datetime.now().strftime('%Y%m%d%H%M')

    # 폴더 내부 파일/폴더 비우기
    clear_folder(report_path)

    # 폴더가 없으면 생성
    if not os.path.exists(report_path):
        os.makedirs(report_path)

    # 5) 각 인터벌마다 다른 limit을 설정하고 싶다면, 아래와 같이 딕셔너리로 관리
    limit_dict = {
        "15m": 1000,
        "1h": 1000,
        "4h": 1000
    }

    # 수집할 인터벌 리스트
    intervals = ["15m", "1h", "4h"]

    # # 6) 각 interval별로 klines 데이터 수집 후 CSV로 저장
    # for interval in intervals:
    #     limit = limit_dict.get(interval, 500)  # 딕셔너리에서 limit을 가져오고, 없으면 500
    #     klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    #
    #     csv_filename = f"{date_prefix}_{symbol}_{interval}.csv"
    #     csv_filepath = os.path.join(report_path, csv_filename)
    #
    #     with open(csv_filepath, mode='w', newline='', encoding='utf-8') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["open_time", "open", "high", "low", "close", "volume"])
    #
    #         for k in klines:
    #             open_time = datetime.fromtimestamp(k[0] / 1000)
    #             open_price = float(k[1])
    #             high_price = float(k[2])
    #             low_price = float(k[3])
    #             close_price = float(k[4])
    #             volume = float(k[5])
    #
    #             writer.writerow([
    #                 open_time.strftime('%Y-%m-%d %H:%M:%S'),
    #                 open_price,
    #                 high_price,
    #                 low_price,
    #                 close_price,
    #                 volume,
    #             ])

    # 7) 잔고(미사용, 또는 0이 아닌 것만) CSV 저장
    nonzero_futures_balance = []
    for item in futures_balance:
        if float(item["balance"]) != 0.0:
            nonzero_futures_balance.append(item)

    futures_balance_file = os.path.join(report_path, f"{date_prefix}_futures_balance.csv")
    with open(futures_balance_file, 'w', newline='', encoding='utf-8') as f:
        if nonzero_futures_balance:
            writer = csv.DictWriter(f, fieldnames=nonzero_futures_balance[0].keys())
            writer.writeheader()
            writer.writerows(nonzero_futures_balance)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])

    # # (B) 포지션 정보
    # positions_file = os.path.join(report_path, f"{date_prefix}_positions.csv")
    # with open(positions_file, 'w', newline='', encoding='utf-8') as f:
    #     if positions:
    #         writer = csv.DictWriter(f, fieldnames=positions[0].keys())
    #         writer.writeheader()
    #         writer.writerows(positions)
    #     else:
    #         writer = csv.writer(f)
    #         writer.writerow(["No Data"])

    # # (C) 미체결 주문
    # open_orders_file = os.path.join(report_path, f"{date_prefix}_open_orders.csv")
    # with open(open_orders_file, 'w', newline='', encoding='utf-8') as f:
    #     if open_orders:
    #         writer = csv.DictWriter(f, fieldnames=open_orders[0].keys())
    #         writer.writeheader()
    #         writer.writerows(open_orders)
    #     else:
    #         writer = csv.writer(f)
    #         writer.writerow(["No Data"])

    # (D) 오더북 (필요 시 주석 해제)
    # orderbook_file = os.path.join(report_path, f"{date_prefix}_orderbook.csv")
    # with open(orderbook_file, 'w', newline='', encoding='utf-8') as f:
    #     writer = csv.writer(f)
    #     if orderbook:
    #         writer.writerow(["Key", "Value"])
    #         for key, value in orderbook.items():
    #             writer.writerow([key, value])
    #     else:
    #         writer.writerow(["No Data"])

    # # (E) 뉴스 리스트 CSV 저장
    # news_file = os.path.join(report_path, f"{date_prefix}_news_list.csv")
    # with open(news_file, 'w', newline='', encoding='utf-8') as f:
    #     if full_news_list:
    #         fieldnames = full_news_list[0].keys()
    #         writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    #         writer.writeheader()
    #         writer.writerows(full_news_list)
    #     elif rss_news_list:
    #         fieldnames = rss_news_list[0].keys()
    #         writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    #         writer.writeheader()
    #         writer.writerows(rss_news_list)
    #     else:
    #         writer = csv.writer(f)
    #         writer.writerow(["No Data"])

    # 8) 콘솔 출력(확인)
    print("Futures Balance (original):", futures_balance)
    print("Futures Balance (non-zero):", nonzero_futures_balance)
    print("Open Positions:", positions)
    print("Open Orders:", open_orders)
    print("Orderbook Depth:", orderbook)
    # print("Recent News (full_news_list):", full_news_list)
    # print("Recent News (rss_news_list):", rss_news_list)

    # (F) Closed Position History CSV 저장
    #     position_history 모듈의 함수로 과거 포지션 히스토리를 조회하고, CSV로 저장
    cutoff_time = datetime(2025, 2, 1, 16, 0, 0)
    position_df = build_position_history(
        client=client,       # 클라이언트 객체
        symbol=symbol,       # 조회 심볼
        limit=500,            # 원하는 limit 값
        cutoff_dt=cutoff_time
    )

    # CSV 파일명 (예: 202306051230_BTCUSDT_history.csv)
    history_file = os.path.join(report_path, f"{date_prefix}_{symbol}_history.csv")

    # DataFrame을 CSV로 저장
    position_df.to_csv(history_file, index=False, encoding="utf-8-sig")
    print(f"\nPosition History saved to: {history_file}")
    print(position_df)

if __name__ == "__main__":
    main()
