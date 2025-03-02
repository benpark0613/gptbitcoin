import os
import csv
from datetime import datetime
from io import StringIO

import pandas as pd
import pyupbit
from dotenv import load_dotenv
# 구글 뉴스 관련 import 제거
# from module.get_googlenews import scrape_news  # 필요 시 사용


# ===============================================================
# 유틸리티 함수 모음
# ===============================================================
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


def list_dict_to_csv_string(data_list, fieldnames, delimiter=';'):
    """
    리스트 딕셔너리를 CSV 형식 문자열로 변환합니다.
    """
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter, lineterminator='\n')
    writer.writeheader()
    writer.writerows(data_list)
    csv_str = output.getvalue().rstrip('\r\n')
    return csv_str


def round_numeric_columns_in_df(df):
    """
    DataFrame의 모든 숫자 타입 컬럼을 소수점 이하 2자리로 반올림합니다.
    """
    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
    df[numeric_cols] = df[numeric_cols].apply(lambda x: x.round(2))
    return df


# ===============================================================
# 보조지표 계산 함수
# ===============================================================
def add_technical_indicators(df, key):
    """
    OHLCV 데이터프레임에 기술적 보조지표를 추가합니다.

    - RSI: 기간 14
    - MACD: EMA(12), EMA(26) 및 Signal(9)
    - Bollinger Bands: 20기간, 표준편차 2
    - MA21, MA50, MA200

    계산 결과는 소수점 이하 2자리로 반올림합니다.
    """
    # RSI (14)
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = (100 - (100 / (1 + rs))).round(2)

    # MACD (12, 26, 9)
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = (ema12 - ema26).round(2)
    df['MACD_signal'] = (df['MACD'].ewm(span=9, adjust=False).mean()).round(2)
    df['MACD_hist'] = (df['MACD'] - df['MACD_signal']).round(2)

    # Bollinger Bands (20, 2)
    df['BB_middle'] = df['close'].rolling(window=20).mean().round(2)
    df['BB_std'] = df['close'].rolling(window=20).std().round(2)
    df['BB_upper'] = (df['BB_middle'] + (2 * df['BB_std'])).round(2)
    df['BB_lower'] = (df['BB_middle'] - (2 * df['BB_std'])).round(2)

    # 이동평균선 (MA)
    df['MA21'] = df['close'].rolling(window=21).mean().round(2)
    df['MA50'] = df['close'].rolling(window=50).mean().round(2)
    df['MA200'] = df['close'].rolling(window=200).mean().round(2)

    return df


# ===============================================================
# CSV 및 Report 저장 관련 함수
# ===============================================================
def save_to_csv(filename, data, fieldnames):
    """
    지정된 CSV 파일에 데이터를 저장합니다.
    파일이 존재하면 이어 쓰고, 없으면 새로 생성합니다.
    """
    folder_path = 'report_spot'
    file_path = os.path.join(folder_path, filename)

    # 데이터가 비어있으면 저장하지 않습니다.
    if not data:
        print(f"{filename}에 저장할 데이터가 없습니다.")
        return

    file_exists = os.path.exists(file_path)
    try:
        with open(file_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        print(f"{filename} 저장 중 예외 발생: {e}")


def save_ohlcv_to_csv(df, filename):
    """
    OHLCV DataFrame을 CSV 파일로 저장합니다.
    """
    folder_path = 'report_spot'
    file_path = os.path.join(folder_path, filename)
    df.to_csv(file_path, index=True)
    print(f"{filename}에 OHLCV 데이터가 저장되었습니다.")


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


def save_to_txt(report_folder, open_orders_data, total_assets_data, ohlcv_data_dict):
    """
    report.txt에 각 CSV 데이터를 CSV 형식 그대로 기록합니다.
    open_orders, total_assets, 각 OHLCV 데이터를 포함합니다.
    """
    report_path = os.path.join(report_folder, '0.report.txt')

    # 각 CSV 데이터의 필드명 설정
    open_orders_fieldnames = ['order_id', 'side', 'price', 'volume', 'state', 'created_at']
    total_assets_fieldnames = ['timestamp', 'krw_balance', 'btc_balance', 'btc_market_price', 'btc_evaluation',
                               'total_balance']

    with open(report_path, 'w', encoding='utf-8') as file:
        # open_orders.csv 기록
        file.write("-- open_orders.csv\n")
        open_orders_csv = list_dict_to_csv_string(open_orders_data, open_orders_fieldnames, delimiter=';')
        file.write(open_orders_csv)
        file.write("\n\n")

        # total_assets.csv 기록
        file.write("-- total_assets.csv\n")
        total_assets_csv = list_dict_to_csv_string(total_assets_data, total_assets_fieldnames, delimiter=';')
        file.write(total_assets_csv)
        file.write("\n\n")

        # 각 OHLCV 데이터 기록 (소수점은 2자리로 반올림)
        for key, df in ohlcv_data_dict.items():
            df_csv = df.reset_index()
            df_csv.rename(columns={'index': 'timestamp'}, inplace=True)
            df_csv = round_numeric_columns_in_df(df_csv)
            ohlcv_fieldnames = list(df_csv.columns)
            csv_str = list_dict_to_csv_string(df_csv.to_dict('records'), ohlcv_fieldnames, delimiter=';')
            file.write(f"-- ohlcv_{key}.csv\n")
            file.write(csv_str)
            file.write("\n\n")


# ===============================================================
# Upbit API 및 데이터 수집 함수
# ===============================================================
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


def fetch_ohlcv_data(ticker, timeframe_configs):
    """
    시간프레임 설정에 따라 OHLCV 데이터를 수집하고, 보조지표를 추가한 후
    보조지표 계산을 위한 추가 데이터를 제거하여 최종 DataFrame을 반환합니다.
    """
    ohlcv_data = {}
    for config in timeframe_configs:
        key = config['key']
        tf = config['timeframe']
        count = config['count']
        extra_rows = 200
        try:
            df = pyupbit.get_ohlcv(ticker, tf, count + extra_rows)
            df = add_technical_indicators(df, key)
            # 추가 데이터를 제거하여 최종 데이터만 사용
            df = df.iloc[extra_rows:]
            ohlcv_data[key] = df
            save_ohlcv_to_csv(df, f"ohlcv_{key}.csv")
        except Exception as e:
            print(f"OHLCV 데이터 ({key}) 저장 중 오류: {e}")
    return ohlcv_data


# ===============================================================
# 데이터 파싱 함수
# ===============================================================
def parse_balance_data(balance_list, btc_price):
    """
    잔고 리스트와 BTC 시세를 받아 balances.csv 데이터와 총 잔고 계산에 필요한 값을 반환합니다.
    """
    balance_data = []
    total_krw_balance = 0.0
    total_btc_balance = 0.0
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in balance_list:
        currency = item.get('currency', '')
        balance_value = float(item.get('balance', 0))

        if currency == 'KRW':
            market_price = 1.0
        elif currency == 'BTC':
            market_price = btc_price
        else:
            market_price = 0.0

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
    진행 중인 주문 데이터를 파싱하고, 예약 주문에 따른 자산 반영을 수행합니다.
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

        if side == 'ask' and state in ['wait', 'watch']:
            if 'BTC' in market and volume:
                total_btc_balance += float(volume)
        elif side == 'bid' and state in ['wait', 'watch']:
            if 'BTC' in market and price and volume:
                total_krw_balance += float(volume) * float(price)

    return open_orders_data, total_krw_balance, total_btc_balance


def calculate_total_assets(krw_balance, btc_balance, btc_price):
    """
    총 자산 계산: KRW 잔고와 BTC 잔고(시세 적용)의 합계를 반환합니다.
    """
    total_btc_value = btc_balance * btc_price
    return krw_balance + total_btc_value


# ===============================================================
# 메인 실행 함수 (리팩토링 및 가독성 개선)
# ===============================================================
def main():
    # 1. 환경변수 로드 및 Upbit 객체 생성
    load_dotenv()
    access_key = os.getenv("UPBIT_ACCESS_KEY")
    secret_key = os.getenv("UPBIT_SECRET_KEY")
    upbit = pyupbit.Upbit(access_key, secret_key)

    # 2. 보고서 폴더 준비
    report_folder = 'report_spot'
    prepare_report_folder(report_folder)

    # 3. 구글 뉴스 스크래핑 관련 부분 제거

    # 4. Upbit API를 통한 데이터 수집
    try:
        balance_list = get_balance(upbit)
        open_orders_list = get_open_orders(upbit)
        btc_price = get_btc_price()
    except Exception as e:
        print(f"API 호출 중 오류: {e}")
        return

    if balance_list is None or open_orders_list is None:
        print("필수 데이터(잔고/주문)를 가져올 수 없습니다.")
        return

    # 5. 잔고 및 주문 데이터 파싱
    balance_data, total_krw_balance, total_btc_balance = parse_balance_data(balance_list, btc_price)
    open_orders_data, total_krw_balance, total_btc_balance = parse_open_orders_data(
        open_orders_list, total_krw_balance, total_btc_balance
    )

    total_balance = calculate_total_assets(total_krw_balance, total_btc_balance, btc_price)
    btc_evaluation = total_btc_balance * btc_price
    today_date = datetime.now().strftime('%Y-%m-%d')
    total_assets_data = [{
        'timestamp': today_date,
        'krw_balance': total_krw_balance,
        'btc_balance': total_btc_balance,
        'btc_market_price': btc_price,
        'btc_evaluation': btc_evaluation,
        'total_balance': f'{total_balance:.2f}'
    }]
    total_assets_fieldnames = ['timestamp', 'krw_balance', 'btc_balance', 'btc_market_price', 'btc_evaluation',
                               'total_balance']
    total_assets_file_path = os.path.join(report_folder, 'total_assets.csv')
    update_or_create_record(total_assets_file_path, total_assets_data, total_assets_fieldnames)

    # 6. CSV 파일 저장 (balances, open_orders)
    balances_fieldnames = ['timestamp', 'currency', 'balance', 'market_price', 'evaluation_value']
    open_orders_fieldnames = ['order_id', 'side', 'price', 'volume', 'state', 'created_at']
    save_to_csv('balances.csv', balance_data, balances_fieldnames)
    save_to_csv('open_orders.csv', open_orders_data, open_orders_fieldnames)

    # 7. OHLCV 데이터 수집 및 저장 (각 시간프레임별로 추가 데이터 포함 후 제거)
    ohlcv_timeframes = [
        {'key': '1h', 'timeframe': 'minute60', 'count': 300},
        {'key': '4h', 'timeframe': 'minute240', 'count': 200},
        {'key': '1d', 'timeframe': 'day', 'count': 150}
        # 추가 시간프레임 가능
    ]
    ticker = "KRW-BTC"
    ohlcv_data_dict = fetch_ohlcv_data(ticker, ohlcv_timeframes)

    # 8. report.txt 생성 (모든 데이터를 CSV 형식으로 기록)
    save_to_txt(report_folder, open_orders_data, total_assets_data, ohlcv_data_dict)

    print("정상 처리되었습니다. report_spot 폴더 안에 CSV 파일들과 report.txt가 생성되었습니다.")


if __name__ == "__main__":
    main()
