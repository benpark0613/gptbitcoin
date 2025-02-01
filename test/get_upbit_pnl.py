import os
import pyupbit
from dotenv import load_dotenv
from datetime import datetime


# 잔고 조회 함수
def get_balance(upbit):
    balances = upbit.get_balances()
    return balances


# 거래 내역 조회 함수
def get_orders(upbit):
    # 오늘 날짜로 필터링하여 거래 내역 가져오기
    today = datetime.today().strftime('%Y-%m-%d')
    orders = upbit.get_order("KRW-BTC", state="done")  # "KRW-BTC"는 예시, 다른 종목을 사용할 수 있습니다
    today_orders = [order for order in orders if order['created_at'][:10] == today]  # 오늘 날짜로 필터링
    return today_orders


# PnL 계산 함수
def calculate_pnl(orders, balance):
    pnl = 0
    for order in orders:
        if order['side'] == 'ask':  # 매도 주문
            sell_price = float(order['price'])
            qty = float(order['volume'])
            # PnL 계산 (매도 가격 * 수량)
            pnl += sell_price * qty
        elif order['side'] == 'bid':  # 매수 주문
            buy_price = float(order['price'])
            qty = float(order['volume'])
            # PnL 계산 (매수 가격 * 수량)
            pnl -= buy_price * qty
    return pnl


# 메인 함수
if __name__ == "__main__":
    # 환경 변수에서 API 키 가져오기
    load_dotenv()
    access_key = os.getenv("UPBIT_ACCESS_KEY")
    secret_key = os.getenv("UPBIT_SECRET_KEY")

    # Upbit API 객체 생성
    upbit = pyupbit.Upbit(access_key, secret_key)

    # 거래 내역과 잔고를 가져옵니다.
    orders = get_orders(upbit)  # 오늘의 거래 내역
    balance = get_balance(upbit)  # 현재 잔고

    # PnL을 계산합니다.
    pnl = calculate_pnl(orders, balance)

    # 결과 출력
    print(f"오늘의 PnL: {pnl}")
