import logging
import os

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Alpha Vantage API 키 (환경변수 .env 에 설정)
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# ----------------------
# SPY 시세 수집 (주가)
# ----------------------
symbol = 'SPY'  # S&P 500 ETF (예시)
endpoint_spy = 'https://www.alphavantage.co/query'
params_spy = {
    'function': 'TIME_SERIES_DAILY',
    'symbol': symbol,
    'apikey': ALPHA_VANTAGE_API_KEY,
    'outputsize': 'compact'  # compact: 최근 100일, full: 전체 기간
}

response_spy = requests.get(endpoint_spy, params=params_spy)
data_spy = response_spy.json()

if 'Time Series (Daily)' in data_spy:
    ts_data = data_spy['Time Series (Daily)']
    df_spy = pd.DataFrame(ts_data).T  # 전치(.T)로 행/열 변환
    df_spy.index.name = 'Date'

    # 컬럼 이름 변경
    df_spy.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df_spy = df_spy.astype(float)  # 문자열에서 float 변환
    print("SPY 주가 데이터 (HEAD):")
    print(df_spy.head())
else:
    print("SPY 데이터를 불러오지 못했습니다:", data_spy)

# ----------------------
# USD/KRW 환율 수집 (FX_DAILY)
# ----------------------
endpoint_fx = 'https://www.alphavantage.co/query'
params_fx = {
    'function': 'FX_DAILY',
    'from_symbol': 'USD',
    'to_symbol': 'KRW',
    'apikey': ALPHA_VANTAGE_API_KEY,
    'outputsize': 'compact'
}

response_fx = requests.get(endpoint_fx, params=params_fx)
data_fx = response_fx.json()

if 'Time Series FX (Daily)' in data_fx:
    ts_data_fx = data_fx['Time Series FX (Daily)']
    df_fx = pd.DataFrame(ts_data_fx).T
    df_fx.index.name = 'Date'

    # FX_DAILY는 시가(Open), 고가(High), 저가(Low), 종가(Close) 네 가지 정보가 제공됨
    df_fx.columns = ['Open', 'High', 'Low', 'Close']
    df_fx = df_fx.astype(float)

    print("\nUSD/KRW 환율 데이터 (HEAD):")
    print(df_fx.head())
else:
    print("환율 데이터를 불러오지 못했습니다:", data_fx)