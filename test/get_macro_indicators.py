import logging
import os
import requests
import pandas as pd
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------------
# 1) FRED (미국 10년물 국채금리) 월간
# ----------------------------------
FRED_API_KEY = os.getenv("FRED_API_KEY")
series_id = 'DGS10'  # 미국 10년물 국채금리

url_fred = 'https://api.stlouisfed.org/fred/series/observations'
params_fred = {
    'series_id': series_id,
    'api_key': FRED_API_KEY,
    'file_type': 'json',
    'observation_start': '2020-01-01',     # 2020년 이후
    'frequency': 'm',                     # monthly
    'aggregation_method': 'eop'           # end of period (월말 값)
}

response_fred = requests.get(url_fred, params=params_fred)
data_fred = response_fred.json()

df_fred = None
if 'observations' in data_fred:
    df_fred = pd.DataFrame(data_fred['observations'])
    # 필요한 컬럼만 사용 (date, value)
    df_fred = df_fred[['date', 'value']]
    # 값이 '.' 인 것은 결측치이므로 제거
    df_fred = df_fred[df_fred['value'] != '.']
    # float 변환
    df_fred['value'] = df_fred['value'].astype(float)

    # 날짜를 datetime으로 변환 후 인덱스로 설정
    df_fred['date'] = pd.to_datetime(df_fred['date'])
    df_fred.set_index('date', inplace=True)

    # 날짜 기준 내림차순 정렬 (가장 최근이 위)
    df_fred.sort_index(ascending=False, inplace=True)

    print("[FRED] 미국 10년물 국채금리 (월봉, 최신순 5개):")
    print(df_fred.head(5))
else:
    print("FRED 월간 데이터를 불러오지 못했습니다:", data_fred)


# ----------------------------------
# 2) Alpha Vantage (SPY 월봉)
# ----------------------------------
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

url_spy = 'https://www.alphavantage.co/query'
params_spy = {
    'function': 'TIME_SERIES_MONTHLY',  # 월봉 데이터를 직접 요청
    'symbol': 'SPY',                   # 미국 S&P 500 ETF
    'apikey': ALPHA_VANTAGE_API_KEY
}

response_spy = requests.get(url_spy, params=params_spy)
data_spy = response_spy.json()

df_spy = None
if 'Monthly Time Series' in data_spy:
    df_spy = pd.DataFrame(data_spy['Monthly Time Series']).T
    df_spy.index.name = 'Date'
    # Alpha Vantage 월봉: [open, high, low, close, volume]
    df_spy.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # float 변환
    df_spy = df_spy.astype(float)

    # 인덱스를 datetime으로 변환
    df_spy.index = pd.to_datetime(df_spy.index)

    # 날짜 기준 내림차순 정렬
    df_spy.sort_index(ascending=False, inplace=True)

    print("\n[Alpha Vantage] SPY 주가 데이터 (월봉, 최신순 5개):")
    print(df_spy.head(5))
else:
    print("SPY 월봉 데이터를 불러오지 못했습니다:", data_spy)


# ----------------------------------
# 3) Alpha Vantage (USD/KRW 월봉)
# ----------------------------------
url_fx = 'https://www.alphavantage.co/query'
params_fx = {
    'function': 'FX_MONTHLY',       # 월봉 데이터
    'from_symbol': 'USD',
    'to_symbol': 'KRW',
    'apikey': ALPHA_VANTAGE_API_KEY
}

response_fx = requests.get(url_fx, params=params_fx)
data_fx = response_fx.json()

df_fx = None
if 'Time Series FX (Monthly)' in data_fx:
    df_fx = pd.DataFrame(data_fx['Time Series FX (Monthly)']).T
    df_fx.index.name = 'Date'
    df_fx.columns = ['Open', 'High', 'Low', 'Close']
    df_fx = df_fx.astype(float)

    # 인덱스를 datetime으로 변환
    df_fx.index = pd.to_datetime(df_fx.index)

    # 날짜 기준 내림차순 정렬
    df_fx.sort_index(ascending=False, inplace=True)

    print("\n[Alpha Vantage] USD/KRW 환율 데이터 (월봉, 최신순 5개):")
    print(df_fx.head(5))
else:
    print("환율 월봉 데이터를 불러오지 못했습니다:", data_fx)

# ----------------------------------
# 이후 df_fred, df_spy, df_fx를 활용해 거시경제 월봉 분석 가능
# 예) 날짜 병합, 시각화, 파일 저장 등
# ----------------------------------