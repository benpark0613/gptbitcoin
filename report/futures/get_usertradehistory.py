import requests
import pandas as pd
import time
import hmac
import hashlib
import os
from urllib.parse import urlencode
import pytz
from dotenv import load_dotenv

load_dotenv()
access = os.getenv("BINANCE_ACCESS_KEY")
secret = os.getenv("BINANCE_SECRET_KEY")

# 바이낸스 API 키와 시크릿
API_KEY = access
API_SECRET = secret

# 바이낸스 API URL
BASE_URL = 'https://fapi.binance.com'


# 타임스탬프를 밀리초로 변환하는 함수
def get_timestamp():
    return int(time.time() * 1000)


# 시그니처 생성 함수
def create_signature(params):
    query_string = urlencode(params)
    return hmac.new(API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()


# 거래 내역을 조회하는 함수
def get_trade_history(symbol='BTCUSDT', limit=1000):
    params = {
        'symbol': symbol,
        'limit': limit,
        'timestamp': get_timestamp(),
    }

    # 시그니처 추가
    params['signature'] = create_signature(params)

    # 헤더에 API 키 추가
    headers = {
        'X-MBX-APIKEY': API_KEY
    }

    # 요청 보내기
    response = requests.get(f'{BASE_URL}/fapi/v1/userTrades', headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []


# 거래 내역을 CSV로 저장하는 함수
def save_trade_history_to_csv():
    trades = get_trade_history(symbol='BTCUSDT')

    if trades:
        # pandas DataFrame으로 변환
        df = pd.DataFrame(trades)

        # UTC 시간 -> 한국 시간(KST)으로 변환
        df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)
        df['time'] = df['time'].dt.tz_convert('Asia/Seoul')

        # 오늘 날짜에 해당하는 거래만 필터링
        df_today = df[df['time'].dt.date == pd.to_datetime('today').date()]

        # trade_result 폴더 비우기
        folder_path = 'trade_result'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)  # 폴더가 없으면 생성
        else:
            # 폴더 안의 파일들 삭제
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)

        # CSV로 저장
        df_today.to_csv(os.path.join(folder_path, 'btc_usdt_trade_history.csv'), index=False)
        print("CSV 파일로 저장 완료!")
    else:
        print("오늘의 거래 내역이 없습니다.")


# 거래 내역을 CSV로 저장
save_trade_history_to_csv()
