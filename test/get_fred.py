import logging
import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FRED API 키
FRED_API_KEY = os.getenv("FRED_API_KEY")

# 10년물 국채금리(미국) 시리즈 ID: DGS10
series_id = 'DGS10'

# FRED API 엔드포인트
url = 'https://api.stlouisfed.org/fred/series/observations'

params = {
    'series_id': series_id,
    'api_key': FRED_API_KEY,
    'file_type': 'json'  # json 또는 xml
}

response = requests.get(url, params=params)
data = response.json()

if 'observations' in data:
    observations = data['observations']
    # 데이터를 pandas DataFrame으로 변환
    df_rates = pd.DataFrame(observations)
    # date, value 컬럼만 추출
    df_rates = df_rates[['date', 'value']]
    # value가 '.'인 경우는 데이터가 없는 경우이므로 제거
    df_rates = df_rates[df_rates['value'] != '.']
    # 숫자 변환
    df_rates['value'] = df_rates['value'].astype(float)

    print(df_rates.head())
else:
    print("데이터를 불러오지 못했습니다:", data)