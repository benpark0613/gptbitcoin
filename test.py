import logging
import os

import pyupbit
import ta
from dotenv import load_dotenv

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


def add_indicators(df, interval):
    """
    데이터프레임에 보조지표를 추가하는 함수
    :param df: OHLCV 데이터프레임
    :param interval: 데이터 간격 ('minute15', 'minute60', 'day')
    :return: 보조지표가 추가된 데이터프레임
    """

    return df

def ai_trading():
    global upbit
    ### 데이터 가져오기
    # 1. 현재 투자 상태 조회
    all_balances = upbit.get_balances()
    filtered_balances = [
        balance for balance in all_balances if balance["currency"] in ["BTC", "KRW"]
    ]

    # 2. 오더북(호가 데이터) 조회
    orderbook = pyupbit.get_orderbook("KRW-BTC")

    # 3. 차트 데이터 조회 및 보조지표 추가
    # 15분봉 데이터
    df_15min = pyupbit.get_ohlcv("KRW-BTC", interval="minute15", count=60)
    df_15min = df_15min.dropna()
    df_15min = add_indicators(df_15min, timeframe="15min")

    # 1시간봉 데이터
    df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=40)
    df_hourly = df_hourly.dropna()
    df_hourly = add_indicators(df_hourly, timeframe="1hour")

    # 4시간봉 데이터
    df_4hour = pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=20)
    df_4hour = df_4hour.dropna()
    df_4hour = add_indicators(df_4hour, timeframe="4hour")

    print(df_15min)
    print(df_hourly)
    print(df_4hour)
