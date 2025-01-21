import logging
import os

import pyupbit
from dotenv import load_dotenv
from ta.utils import dropna

# .env 파일에 저장된 환경 변수를 불러오기 (API 키 등)
load_dotenv()

# 로깅 설정 - 로그 레벨을 INFO로 설정하여 중요 정보 출력
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Upbit 객체 생성
access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")
upbit = pyupbit.Upbit(access, secret)

### 데이터 가져오기
# 1. 현재 투자 상태 조회
all_balances = upbit.get_balances()
filtered_balances = [
    balance for balance in all_balances if balance["currency"] in ["BTC", "KRW"]
]

# 2. 오더북(호가 데이터) 조회
orderbook = pyupbit.get_orderbook("KRW-BTC")

# 3. 차트 데이터 조회
# 1시간봉 데이터
df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=10000)
df_hourly = dropna(df_hourly)

# 4시간봉 데이터
df_4hour = pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=2500)
df_4hour = dropna(df_4hour)

# 데이터프레임을 CSV로 저장
hourly_file_path = "df_hourly.csv"
df_hourly.to_csv(hourly_file_path, index=True)
print(f"1시간봉 CSV 파일이 저장되었습니다: {hourly_file_path}")

four_hour_file_path = "df_4hour.csv"
df_4hour.to_csv(four_hour_file_path, index=True)
print(f"4시간봉 CSV 파일이 저장되었습니다: {four_hour_file_path}")
