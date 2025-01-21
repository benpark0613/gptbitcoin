import os
from dotenv import load_dotenv
import pyupbit
import pandas as pd
import json
from openai import OpenAI
import ta
from ta.utils import dropna
import time
import requests
import logging
from pydantic import BaseModel
import sqlite3
from datetime import datetime, timedelta
import schedule

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
logger.info(all_balances)

# 2. 오더북(호가 데이터) 조회
orderbook = pyupbit.get_orderbook("KRW-BTC")
logger.info(orderbook)

# 3. 차트 데이터 조회
# 1시간봉 데이터
df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=48)
df_hourly = dropna(df_hourly)
# 4시간봉 데이터
df_daily = pyupbit.get_ohlcv("KRW-BTC", interval="day", count=14)
df_daily = dropna(df_daily)