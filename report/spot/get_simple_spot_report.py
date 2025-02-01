import os
import shutil
import pyupbit
from dotenv import load_dotenv
import csv
import json
from datetime import datetime


# 폴더가 존재하면 내용을 지우고, 없으면 폴더를 생성하는 함수
def prepare_report_folder(folder):
    if os.path.exists(folder):
        # 폴더가 존재하면 내용물을 삭제
        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        # 폴더가 없으면 생성
        os.makedirs(folder)


# 잔고 조회 함수
def get_balance(upbit):
    balances = upbit.get_balances()
    return balances


# 진행 중 주문 조회 함수 (지정가, 예약 주문 대기 포함)
def get_open_orders(upbit):
    open_orders = upbit.get_order("KRW-BTC", state="wait") + upbit.get_order("KRW-BTC", state="watch")
    return open_orders


# BTC 시세 조회 함수
def get_btc_price(upbit):
    btc_price = pyupbit.get_current_price("KRW-BTC")  # BTC/KRW 시세
    return btc_price


# 데이터를 CSV로 저장하는 함수
def save_to_csv(filename, data, fieldnames):
    file_path = os.path.join('report_simple', filename)
    if not data:
        print(f"{filename}에 저장할 데이터가 없습니다.")
        return

    # 파일이 존재하면 기존 내용에 이어쓰기, 파일이 없으면 새로 생성
    file_exists = os.path.exists(file_path)
    with open(file_path, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:  # 파일이 없으면 헤더 추가
            writer.writeheader()
        writer.writerows(data)


# 파일에서 오늘 날짜에 이미 자산 정보가 기록되어 있는지 확인하는 함수
def check_existing_record(file_path, today_date):
    if os.path.exists(file_path):
        with open(file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['timestamp'] == today_date:
                    return True  # 이미 오늘 자산 기록이 있음
    return False  # 오늘 자산 기록이 없음


# txt 파일로 JSON 데이터 저장 함수
def save_to_txt(report_folder, open_orders_data, total_assets_data):
    report_path = os.path.join(report_folder, 'report.txt')
    with open(report_path, 'w') as file:
        # open_orders.csv 내용을 JSON 형식으로 저장
        file.write("-- open_orders.csv\n")
        file.write(json.dumps(open_orders_data, ensure_ascii=False, indent=4))  # JSON 형태로 예쁘게 저장

        file.write("\n\n")  # 구분

        # total_assets.csv 내용을 JSON 형식으로 저장
        file.write("-- total_assets.csv\n")
        file.write(json.dumps(total_assets_data, ensure_ascii=False, indent=4))  # JSON 형태로 예쁘게 저장


# 메인 함수
if __name__ == "__main__":
    # 환경 변수에서 API 키 가져오기
    load_dotenv()
    access_key = os.getenv("UPBIT_ACCESS_KEY")
    secret_key = os.getenv("UPBIT_SECRET_KEY")

    # Upbit API 객체 생성
    upbit = pyupbit.Upbit(access_key, secret_key)

    try:
        # 잔고, 진행 중 주문을 가져옵니다.
        balance = get_balance(upbit)
        open_orders = get_open_orders(upbit)
        btc_price = get_btc_price(upbit)  # BTC 시세 조회
    except Exception as e:
        print(f"API 호출 중 오류가 발생했습니다: {e}")
        exit(1)

    if not balance:
        print("잔고 정보를 가져올 수 없습니다.")
        exit(1)

    if not open_orders:
        print("진행 중인 주문 정보를 가져올 수 없습니다.")
        exit(1)

    # 폴더 준비 (폴더가 없으면 생성, 있으면 내용 삭제)
    folder = 'report_simple'
    prepare_report_folder(folder)

    # balances.csv 데이터 준비 (업비트에서 제공하는 데이터 그대로)
    balance_data = []
    total_krw_balance = 0
    total_btc_balance = 0

    for balance_item in balance:
        currency = balance_item['currency']
        balance_value = balance_item['balance']
        market_price = 0  # 시장가는 0으로 초기화

        if currency == 'BTC':  # BTC의 시장가는 조회
            market_price = btc_price  # BTC 가격을 사용

        balance_data.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'currency': currency,
            'balance': balance_value,
            'market_price': market_price
        })

        if currency == 'KRW':
            total_krw_balance += float(balance_value)
        elif currency == 'BTC':
            total_btc_balance += float(balance_value)

    # 진행 중 주문 데이터를 준비 (업비트에서 제공하는 데이터 그대로)
    open_orders_data = []
    for order in open_orders:
        open_orders_data.append({
            'order_id': order['uuid'],
            'side': order['side'],
            'price': order.get('price', 'N/A'),
            'volume': order.get('volume', 'N/A'),
            'state': order['state'],
            'created_at': order['created_at']
        })

    # 진행 중 주문에서 상태가 'wait' 또는 'watch'인 자산 추가
    for order in open_orders:
        if order['side'] == 'ask' and order['state'] in ['wait', 'watch']:  # 매도 예약 주문일 경우
            if 'BTC' in order['market']:  # 'KRW-BTC'에서 'BTC'를 확인
                total_btc_balance += float(order['volume'])
            elif 'KRW' in order['market']:  # 'KRW-BTC'에서 'KRW'를 확인
                total_krw_balance += float(order['volume']) * float(order['price'])

    # 총 자산 계산 (KRW 잔고 + BTC의 현재 시세 반영 자산)
    total_btc_value_krw = total_btc_balance * btc_price
    total_balance = total_krw_balance + total_btc_value_krw

    # total_assets.csv 데이터 준비
    today_date = datetime.now().strftime('%Y-%m-%d')
    total_assets_data = [{
        'timestamp': today_date,
        'krw_balance': total_krw_balance,
        'btc_balance': total_btc_balance,
        'total_balance': f'{total_balance:.2f} KRW'
    }]

    # `total_assets.csv`에 자산 기록 (오늘 자산이 이미 기록되어 있는지 확인 후 저장)
    total_assets_file_path = 'report_simple/total_assets.csv'
    if not check_existing_record(total_assets_file_path, today_date):
        total_assets_fieldnames = ['timestamp', 'krw_balance', 'btc_balance', 'total_balance']
        save_to_csv('total_assets.csv', total_assets_data, total_assets_fieldnames)
    else:
        print(f"오늘 ({today_date}) 자산 기록은 이미 존재합니다.")

    # `balances.csv`에 잔고 및 시장가 기록
    balances_fieldnames = ['timestamp', 'currency', 'balance', 'market_price']
    save_to_csv('balances.csv', balance_data, balances_fieldnames)

    # 진행 중 주문을 `open_orders.csv`에 저장
    open_orders_fieldnames = ['order_id', 'side', 'price', 'volume', 'state', 'created_at']
    save_to_csv('open_orders.csv', open_orders_data, open_orders_fieldnames)

    # report.txt에 데이터를 JSON 형식으로 저장
    save_to_txt(folder, open_orders_data, total_assets_data)

    print("잔고, 진행 중 주문, 총 자산이 report_simple 폴더 안에 CSV로 저장되었습니다. 또한, report.txt에 JSON 형식으로 저장되었습니다.")
