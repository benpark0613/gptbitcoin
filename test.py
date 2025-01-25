import os
import logging
import json
import pyupbit
from dotenv import load_dotenv

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # 1) .env 로드
    load_dotenv()
    access = os.getenv("UPBIT_ACCESS_KEY")
    secret = os.getenv("UPBIT_SECRET_KEY")

    # 2) Upbit 인스턴스 생성
    upbit = pyupbit.Upbit(access, secret)

    # 3) balance(잔고) 조회
    balances = upbit.get_balances()
    logger.info("=== 잔고 조회 결과 ===")
    logger.info(json.dumps(balances, ensure_ascii=False, indent=4))

    # 4) orderbook(오더북) 조회
    orderbook = pyupbit.get_orderbook("KRW-BTC")
    logger.info("=== 오더북 조회 결과 ===")
    logger.info(json.dumps(orderbook, ensure_ascii=False, indent=4))

if __name__ == "__main__":
    main()
