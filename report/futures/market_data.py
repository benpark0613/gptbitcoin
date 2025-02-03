import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from mbinance.client import Client

# clear_folder 함수 불러오기
from module.clear_folder import clear_folder
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
    rss_url = "https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US"

    # 3) 선물 계좌잔고, 포지션 정보, 미체결 주문, 오더북, 뉴스 리스트 가져오기
    futures_balance = client.futures_account_balance()
    positions = client.futures_position_information()
    open_orders = client.futures_get_open_orders(symbol=symbol)
    orderbook = client.futures_order_book(symbol=symbol, limit=orderbook_limit)
    news_list = get_top_10_recent_news(rss_url)

    # 4) 저장 폴더 경로 지정
    report_path = r"C:\MyProjects\gptbitcoin\report\futures\report"

    # 파일 이름에 붙일 접두어(년월일시분) 생성
    date_prefix = datetime.now().strftime('%Y%m%d%H%M')

    # 폴더 내부 파일/폴더 비우기
    clear_folder(report_path)

    # 폴더가 없으면 생성
    if not os.path.exists(report_path):
        os.makedirs(report_path)

    # 5) 각 인터벌마다 다른 limit을 설정하고 싶다면, 아래와 같이 딕셔너리로 관리
    limit_dict = {
        "5m": 1000,
        "15m": 1000,
        "1h": 1000,
        "4h": 500,
        "1d": 300
    }

    # 수집할 인터벌 리스트
    intervals = ["5m", "15m", "1h", "4h", "1d"]

    # 6) 각 interval별로 klines 데이터 수집 후 CSV로 저장
    for interval in intervals:
        # 딕셔너리에서 limit 값을 가져오고, 없으면 디폴트 500 사용
        limit = limit_dict.get(interval, 500)
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)

        csv_filename = f"{date_prefix}_{symbol}_{interval}.csv"
        csv_filepath = os.path.join(report_path, csv_filename)

        with open(csv_filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["open_time", "open", "high", "low", "close", "volume"])

            for k in klines:
                open_time = datetime.fromtimestamp(k[0] / 1000)
                open_price = float(k[1])
                high_price = float(k[2])
                low_price = float(k[3])
                close_price = float(k[4])
                volume = float(k[5])

                writer.writerow([
                    open_time.strftime('%Y-%m-%d %H:%M:%S'),
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                ])

    # 7) 잔고가 0 아닌 경우만 CSV 저장
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

    # (B) 포지션 정보
    positions_file = os.path.join(report_path, f"{date_prefix}_positions.csv")
    with open(positions_file, 'w', newline='', encoding='utf-8') as f:
        if positions:
            writer = csv.DictWriter(f, fieldnames=positions[0].keys())
            writer.writeheader()
            writer.writerows(positions)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])

    # (C) 미체결 주문
    open_orders_file = os.path.join(report_path, f"{date_prefix}_open_orders.csv")
    with open(open_orders_file, 'w', newline='', encoding='utf-8') as f:
        if open_orders:
            writer = csv.DictWriter(f, fieldnames=open_orders[0].keys())
            writer.writeheader()
            writer.writerows(open_orders)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])

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

    # (E) 뉴스 리스트
    news_list_file = os.path.join(report_path, f"{date_prefix}_news_list.csv")
    with open(news_list_file, 'w', newline='', encoding='utf-8') as f:
        if news_list:
            fieldnames = news_list[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(news_list)
        else:
            writer = csv.writer(f)
            writer.writerow(["No Data"])

    # 8) 콘솔 출력(확인)
    print("Futures Balance (original):", futures_balance)
    print("Futures Balance (non-zero):", nonzero_futures_balance)
    print("Open Positions:", positions)
    print("Open Orders:", open_orders)
    print("Orderbook Depth:", orderbook)
    print("Recent News:", news_list)

if __name__ == "__main__":
    main()
