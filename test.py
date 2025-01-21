import logging
import os
import json
import pyupbit
import shutil
from dotenv import load_dotenv
from ta.utils import dropna
from datetime import datetime  # 날짜 및 시간 처리를 위한 모듈

# .env 파일에 저장된 환경 변수를 불러오기 (API 키 등)
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Upbit 객체 생성
access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")
upbit = pyupbit.Upbit(access, secret)

# 현재 시간 타임스탬프 생성
timestamp = datetime.now().strftime("%Y%m%d%H%M")

# chartdata_csv 폴더 명 정의
output_dir = "chartdata_csv"

# 폴더 자체를 삭제
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

# 다시 폴더 생성
os.makedirs(output_dir, exist_ok=True)

### 데이터 가져오기
all_balances = upbit.get_balances()
filtered_balances = [
    balance for balance in all_balances if balance["currency"] in ["BTC", "KRW"]
]

orderbook = pyupbit.get_orderbook("KRW-BTC")

# 15분봉, 1시간봉, 4시간봉
df_15min = pyupbit.get_ohlcv("KRW-BTC", interval="minute15", count=2880)
df_15min = dropna(df_15min)

df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=10000)
df_hourly = dropna(df_hourly)

df_4hour = pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=2500)
df_4hour = dropna(df_4hour)

# 데이터프레임 정렬 (최신 날짜가 위로 오도록)
df_15min = df_15min.sort_index(ascending=False)
df_hourly = df_hourly.sort_index(ascending=False)
df_4hour = df_4hour.sort_index(ascending=False)

# CSV로 저장
fifteen_min_file_path = os.path.join(output_dir, f"{timestamp}_15min.csv")
df_15min.to_csv(fifteen_min_file_path, index=True)
print(f"15분봉 CSV 파일이 저장되었습니다: {fifteen_min_file_path}")

hourly_file_path = os.path.join(output_dir, f"{timestamp}_hourly.csv")
df_hourly.to_csv(hourly_file_path, index=True)
print(f"1시간봉 CSV 파일이 저장되었습니다: {hourly_file_path}")

four_hour_file_path = os.path.join(output_dir, f"{timestamp}_4hour.csv")
df_4hour.to_csv(four_hour_file_path, index=True)
print(f"4시간봉 CSV 파일이 저장되었습니다: {four_hour_file_path}")

# Balances와 Orderbook 데이터를 텍스트 파일로 저장
balances_orderbook_file_path = os.path.join(output_dir, f"{timestamp}_balances_orderbook.txt")
with open(balances_orderbook_file_path, "w", encoding="utf-8") as file:
    file.write("=== 현재 투자 상태 (Balances) ===\n")
    json.dump(all_balances, file, ensure_ascii=False, indent=4)
    file.write("\n\n")
    file.write("=== 오더북 데이터 (Orderbook) ===\n")
    json.dump(orderbook, file, ensure_ascii=False, indent=4)

print(f"Balances와 Orderbook 데이터가 텍스트 파일에 저장되었습니다: {balances_orderbook_file_path}")
