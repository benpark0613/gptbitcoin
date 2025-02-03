import os
import csv
import json
from datetime import datetime
from io import StringIO

import pandas as pd
import pyupbit
from dotenv import load_dotenv
from module.get_googlenews import get_latest_10_articles

def prepare_report_folder(folder_path):
    """
    지정된 폴더가 있으면 내부 파일을 모두 삭제하고,
    폴더가 없으면 새로 생성합니다.
    """
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        os.makedirs(folder_path)


def get_balance(upbit):
    """
    업비트 잔고 목록을 반환합니다.
    """
    return upbit.get_balances()


def get_open_orders(upbit):
    """
    KRW-BTC 종목의 진행 중 주문(wait, watch)을 합쳐 반환합니다.
    """
    open_orders_wait = upbit.get_order("KRW-BTC", state="wait")
    open_orders_watch = upbit.get_order("KRW-BTC", state="watch")
    if open_orders_wait is None or open_orders_watch is None:
        return None
    return open_orders_wait + open_orders_watch


def get_btc_price():
    """
    BTC(KRW-BTC) 시세를 반환합니다.
    """
    return pyupbit.get_current_price("KRW-BTC")


def parse_balance_data(balance_list, btc_price):
    """
    잔고 리스트와 BTC 시세를 받아, balances.csv 저장용 데이터를 생성하고,
    총 KRW, BTC 잔고를 계산해 반환합니다.
    - KRW의 시세는 1.0
    - BTC의 시세는 btc_price
    - evaluation_value = balance × market_price
    """
    balance_data = []
    total_krw_balance = 0.0
    total_btc_balance = 0.0
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in balance_list:
        currency = item.get('currency', '')
        balance_value = float(item.get('balance', 0))

        market_price = 0.0
        if currency == 'KRW':
            market_price = 1.0
        elif currency == 'BTC':
            market_price = btc_price
        else:
            # 다른 코인을 추가하려면 여기서 해당 시세 조회 후 market_price 적용
            pass

        evaluation_value = balance_value * market_price

        balance_data.append({
            'timestamp': current_time,
            'currency': currency,
            'balance': balance_value,
            'market_price': market_price,
            'evaluation_value': evaluation_value
        })

        if currency == 'KRW':
            total_krw_balance += balance_value
        elif currency == 'BTC':
            total_btc_balance += balance_value

    return balance_data, total_krw_balance, total_btc_balance


def parse_open_orders_data(open_orders_list, total_krw_balance, total_btc_balance):
    """
    진행 중인 주문 목록을 CSV 저장용 데이터로 만들고,
    예약 주문에 해당하는 자산도 total_krw_balance, total_btc_balance에 반영합니다.
    """
    open_orders_data = []
    for order in open_orders_list:
        open_orders_data.append({
            'order_id': order['uuid'],
            'side': order['side'],
            'price': order.get('price', 'N/A'),
            'volume': order.get('volume', 'N/A'),
            'state': order['state'],
            'created_at': order['created_at']
        })

    for order in open_orders_list:
        side = order.get('side')
        state = order.get('state')
        price = order.get('price')
        volume = order.get('volume')
        market = order.get('market', '')

        # 매도 예약 주문(ask)
        if side == 'ask' and state in ['wait', 'watch']:
            if 'BTC' in market and volume:
                total_btc_balance += float(volume)

        # 매수 예약 주문(bid)
        elif side == 'bid' and state in ['wait', 'watch']:
            if 'BTC' in market and price and volume:
                total_krw_balance += float(volume) * float(price)

    return open_orders_data, total_krw_balance, total_btc_balance


def calculate_total_assets(krw_balance, btc_balance, btc_price):
    """
    KRW 잔고 + BTC 잔고(BTC 시세 적용)를 합산해 총 자산을 반환합니다.
    """
    total_btc_value_krw = btc_balance * btc_price
    total_balance = krw_balance + total_btc_value_krw
    return total_balance


def save_to_csv(filename, data, fieldnames):
    """
    지정된 CSV 파일에 data를 저장합니다.
    파일이 존재하면 이어 쓰고, 없으면 새로 생성합니다.
    """
    folder_path = 'report_simple'
    file_path = os.path.join(folder_path, filename)

    if not data:
        print(f"{filename}에 저장할 데이터가 없습니다.")
        return

    file_exists = os.path.exists(file_path)
    with open(file_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)


def save_ohlcv_to_csv(df, filename):
    """
    OHLCV DataFrame을 CSV로 저장합니다.
    """
    folder_path = 'report_simple'
    file_path = os.path.join(folder_path, filename)

    df.to_csv(file_path, index=True)
    print(f"{filename}에 OHLCV 데이터가 저장되었습니다.")


def remove_decimals_from_df(df):
    """
    전달된 DF에서 모든 숫자(float, int) 타입 컬럼에 대해
    소수점을 제거(내림)한 정수로 변환합니다.
    """
    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
    df[numeric_cols] = df[numeric_cols].apply(lambda x: x.apply(lambda v: int(v)))
    return df


def list_dict_to_csv_string(data_list, fieldnames, delimiter=';'):
    output = StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames,
        delimiter=delimiter,
        lineterminator='\n'   # 각 행은 \n으로 구분
    )
    writer.writeheader()
    writer.writerows(data_list)

    # getvalue() 결과 끝에 \n이 들어갈 수 있으므로 제거
    csv_str = output.getvalue().rstrip('\r\n')
    return csv_str

def save_to_txt(report_folder,
                open_orders_data,
                total_assets_data,
                df_15m,
                df_1h,
                df_4h,
                google_news_data):
    """
    report.txt에 다음 내용을 CSV 형태 그대로 기록합니다:
    - open_orders.csv
    - total_assets.csv
    - ohlcv_15m.csv
    - ohlcv_1h.csv
    - ohlcv_4h.csv
    - googlenews.csv (신규 추가)

    모두 구분자는 ';'로 설정합니다.
    (OHLCV는 소수점을 제거한 뒤 기록)
    """
    report_path = os.path.join(report_folder, 'report.txt')

    # open_orders.csv와 total_assets.csv에서 사용된 필드명
    open_orders_fieldnames = ['order_id', 'side', 'price', 'volume', 'state', 'created_at']
    total_assets_fieldnames = [
        'timestamp',
        'krw_balance',
        'btc_balance',
        'btc_market_price',
        'btc_evaluation',
        'total_balance'
    ]

    # 구글뉴스 데이터 fieldnames
    google_news_fieldnames = ['title', 'snippet', 'date', 'source', 'parsed_date']

    # --- 소수점 제거 로직(OHLCV에만 적용) ---
    df_15m_csv = df_15m.reset_index()
    df_15m_csv.rename(columns={'index': 'timestamp'}, inplace=True)
    remove_decimals_from_df(df_15m_csv)

    df_1h_csv = df_1h.reset_index()
    df_1h_csv.rename(columns={'index': 'timestamp'}, inplace=True)
    remove_decimals_from_df(df_1h_csv)

    df_4h_csv = df_4h.reset_index()
    df_4h_csv.rename(columns={'index': 'timestamp'}, inplace=True)
    remove_decimals_from_df(df_4h_csv)
    # ----------------------------------------

    # list[dict] 형태로 변환
    df_15m_list = df_15m_csv.to_dict('records')
    df_1h_list = df_1h_csv.to_dict('records')
    df_4h_list = df_4h_csv.to_dict('records')

    # 각각에 대한 fieldnames 동적 추출
    ohlcv_15m_fieldnames = list(df_15m_csv.columns)
    ohlcv_1h_fieldnames = list(df_1h_csv.columns)
    ohlcv_4h_fieldnames = list(df_4h_csv.columns)

    with open(report_path, 'w', encoding='utf-8') as file:
        # -- open_orders.csv --
        file.write("-- open_orders.csv\n")
        open_orders_csv = list_dict_to_csv_string(open_orders_data, open_orders_fieldnames, delimiter=';')
        file.write(open_orders_csv)
        file.write("\n\n")

        # -- total_assets.csv --
        file.write("-- total_assets.csv\n")
        total_assets_csv = list_dict_to_csv_string(total_assets_data, total_assets_fieldnames, delimiter=';')
        file.write(total_assets_csv)
        file.write("\n\n")

        # -- ohlcv_15m.csv --
        file.write("-- ohlcv_15m.csv\n")
        ohlcv_15m_str = list_dict_to_csv_string(df_15m_list, ohlcv_15m_fieldnames, delimiter=';')
        file.write(ohlcv_15m_str)
        file.write("\n\n")

        # -- ohlcv_1h.csv --
        file.write("-- ohlcv_1h.csv\n")
        ohlcv_1h_str = list_dict_to_csv_string(df_1h_list, ohlcv_1h_fieldnames, delimiter=';')
        file.write(ohlcv_1h_str)
        file.write("\n\n")

        # -- ohlcv_4h.csv --
        file.write("-- ohlcv_4h.csv\n")
        ohlcv_4h_str = list_dict_to_csv_string(df_4h_list, ohlcv_4h_fieldnames, delimiter=';')
        file.write(ohlcv_4h_str)
        file.write("\n\n")

        # -- googlenews.csv (신규 추가) --
        file.write("-- googlenews.csv\n")
        google_news_csv_str = list_dict_to_csv_string(google_news_data, google_news_fieldnames, delimiter=';')
        file.write(google_news_csv_str)
        file.write("\n\n")


def update_or_create_record(file_path, new_data, fieldnames):
    """
    total_assets.csv 파일에서 'timestamp'가 같은 레코드가 이미 있으면 업데이트,
    없으면 새 레코드를 추가한 뒤 덮어씁니다.
    """
    if not new_data or 'timestamp' not in new_data[0]:
        return

    today_date = new_data[0]['timestamp']
    rows = []
    updated = False

    if os.path.exists(file_path):
        with open(file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['timestamp'] == today_date:
                    row = new_data[0]
                    updated = True
                rows.append(row)

    if not updated:
        rows.extend(new_data)

    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    load_dotenv()
    access_key = os.getenv("UPBIT_ACCESS_KEY")
    secret_key = os.getenv("UPBIT_SECRET_KEY")
    upbit = pyupbit.Upbit(access_key, secret_key)

    report_folder = 'report_simple'
    prepare_report_folder(report_folder)

    # 1) 구글 뉴스 스크래핑 (최신 기사 10개)
    #    원하는 키워드로 변경 가능 예: "비트코인" / "BTC" 등
    google_news_data = get_latest_10_articles(query="Bitcoin")
    # google_news_data = []

    # 2) googlenews.csv 파일로 저장
    google_news_fieldnames = ['title', 'snippet', 'date', 'source', 'parsed_date']
    save_to_csv("googlenews.csv", google_news_data, google_news_fieldnames)

    try:
        balance_list = get_balance(upbit)
        open_orders_list = get_open_orders(upbit)
        btc_price = get_btc_price()
    except Exception as e:
        print(f"API 호출 중 오류가 발생했습니다: {e}")
        return

    if balance_list is None:
        print("잔고 정보를 가져올 수 없습니다.")
        return
    if open_orders_list is None:
        print("진행 중인 주문 정보를 가져올 수 없습니다. (API 오류)")
        return

    # balances.csv 데이터 생성
    balance_data, total_krw_balance, total_btc_balance = parse_balance_data(balance_list, btc_price)

    # 진행 중 주문 데이터 생성
    open_orders_data, total_krw_balance, total_btc_balance = parse_open_orders_data(
        open_orders_list,
        total_krw_balance,
        total_btc_balance
    )

    # BTC 평가 금액
    btc_evaluation = total_btc_balance * btc_price

    # 총 자산 계산 (KRW + BTC 환산)
    total_balance = calculate_total_assets(total_krw_balance, total_btc_balance, btc_price)

    # 오늘 날짜
    today_date = datetime.now().strftime('%Y-%m-%d')

    # total_assets.csv 저장용 데이터
    total_assets_data = [{
        'timestamp': today_date,
        'krw_balance': total_krw_balance,
        'btc_balance': total_btc_balance,
        'btc_market_price': btc_price,
        'btc_evaluation': btc_evaluation,
        'total_balance': f'{total_balance:.2f}'
    }]

    # total_assets.csv 업데이트
    total_assets_fieldnames = [
        'timestamp',
        'krw_balance',
        'btc_balance',
        'btc_market_price',
        'btc_evaluation',
        'total_balance'
    ]
    total_assets_file_path = os.path.join(report_folder, 'total_assets.csv')
    update_or_create_record(total_assets_file_path, total_assets_data, total_assets_fieldnames)

    # balances.csv 저장
    balances_fieldnames = [
        'timestamp',
        'currency',
        'balance',
        'market_price',
        'evaluation_value'
    ]
    save_to_csv('balances.csv', balance_data, balances_fieldnames)

    # open_orders.csv 저장
    open_orders_fieldnames = ['order_id', 'side', 'price', 'volume', 'state', 'created_at']
    save_to_csv('open_orders.csv', open_orders_data, open_orders_fieldnames)

    # OHLCV 데이터 받아서 CSV 파일로 저장
    try:
        ticker = "KRW-BTC"
        df_15m = pyupbit.get_ohlcv(ticker, "minute15", 672)  # 15분봉 60개
        df_1h = pyupbit.get_ohlcv(ticker, "minute60", 336)   # 1시간봉 60개
        df_4h = pyupbit.get_ohlcv(ticker, "minute240", 180)  # 4시간봉 60개

        save_ohlcv_to_csv(df_15m, "ohlcv_15m.csv")
        save_ohlcv_to_csv(df_1h, "ohlcv_1h.csv")
        save_ohlcv_to_csv(df_4h, "ohlcv_4h.csv")
    except Exception as e:
        print(f"OHLCV 데이터 저장 중 오류가 발생했습니다: {e}")
        return

    # report.txt에 CSV 형식으로 기록 (OHLCV 데이터 + 구글뉴스 데이터 포함)
    save_to_txt(
        report_folder,
        open_orders_data,
        total_assets_data,
        df_15m,
        df_1h,
        df_4h,
        google_news_data
    )

    print("정상 처리되었습니다. report_simple 폴더 안에 CSV 파일들과 report.txt가 생성되었습니다.")
    print("오늘 날짜가 이미 존재하면 최신 데이터로 덮어쓰며, btc_balance, btc_market_price, btc_evaluation이 total_assets.csv에 기록됩니다.")

if __name__ == "__main__":
    main()
