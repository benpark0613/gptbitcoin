import os
import logging
import uuid
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pyupbit

# ============== (1) 설정 및 준비 ==============
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY")
SECRET_KEY = os.getenv("UPBIT_SECRET_KEY")

if not ACCESS_KEY or not SECRET_KEY:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")


# (1-1) JWT 토큰 생성
def create_jwt_token(access_key, secret_key, query_params=None):
    """
    query_params(딕셔너리)가 있으면 query_hash를 만들어야 하지만,
    단순 조회(GET)일 경우 파라미터가 거의 없으므로 여기서는 nonce만 생성.
    """
    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
    }
    if query_params:
        # 필요 시 query 해싱 로직 추가
        pass

    token = jwt.encode(payload, secret_key, algorithm='HS256')
    return token


# ============== (2) 업비트 API 호출 함수 ==============
def get_deposits():
    """업비트 입금 목록 조회 (단순 목록)"""
    url = "https://api.upbit.com/v1/deposits"
    headers = {
        "Authorization": f"Bearer {create_jwt_token(ACCESS_KEY, SECRET_KEY)}"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def get_withdraws():
    """업비트 출금 목록 조회 (단순 목록)"""
    url = "https://api.upbit.com/v1/withdraws"
    headers = {
        "Authorization": f"Bearer {create_jwt_token(ACCESS_KEY, SECRET_KEY)}"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def get_balances():
    """
    업비트 잔고 조회.
    pyupbit 라이브러리의 Upbit 클래스를 쓰지 않고 직접 REST API를 요청하는 방법 예시.
    """
    url = "https://api.upbit.com/v1/accounts"
    headers = {
        "Authorization": f"Bearer {create_jwt_token(ACCESS_KEY, SECRET_KEY)}"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


# ============== (3) 시세 조회 함수 (pyupbit) ==============
def get_price_df(ticker_list, end_date, count=30):
    """
    예시: pyupbit.get_ohlcv를 사용하여, 지정한 ticker에 대한 일봉 데이터를 가져온다.
    end_date를 기준으로 count일치 데이터만 수집.
    """
    ohlcv_data = {}
    for ticker in ticker_list:
        df_ohlcv = pyupbit.get_ohlcv(ticker, interval='day',
                                     to=end_date.strftime('%Y-%m-%d'),
                                     count=count)
        ohlcv_data[ticker] = df_ohlcv
    return ohlcv_data


# ============== (4) 일자별 손익 계산 로직 ==============
def main():
    # (4-1) 날짜 범위 설정
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 8)
    date_range = pd.date_range(start_date, end_date)

    # (4-2) 입금/출금 데이터 불러오기
    deposits = get_deposits()
    withdraws = get_withdraws()

    df_deposits = pd.DataFrame(deposits)
    df_withdraws = pd.DataFrame(withdraws)

    # created_at을 날짜형으로 변환
    df_deposits['created_at'] = pd.to_datetime(df_deposits['created_at'])
    df_withdraws['created_at'] = pd.to_datetime(df_withdraws['created_at'])

    # date 컬럼 추출
    df_deposits['date'] = df_deposits['created_at'].dt.date
    df_withdraws['date'] = df_withdraws['created_at'].dt.date

    # 날짜별 입금/출금 금액 합계
    # 실제로는 currency(KRW)만 필터링해서 합산하는 등 필요
    daily_deposits = df_deposits.groupby('date')['amount'].sum().reset_index(name='daily_deposit')
    daily_withdraws = df_withdraws.groupby('date')['amount'].sum().reset_index(name='daily_withdraw')

    # (4-3) 기초 코인 목록 추출(예: 주요 보유 코인 몇 개)
    # 실제로는 매일 잔고를 기록해야 정확한 '보유 코인 목록'을 알 수 있음
    balances_now = get_balances()
    coins_to_check = []
    for b in balances_now:
        currency = b['currency']
        if currency == 'KRW':
            continue
        coins_to_check.append("KRW-" + currency)

    # (4-4) 해당 코인들의 일봉 시세를 가져오기
    # 예시: 마지막 날짜(end_date)를 기준으로 30일치
    ohlcv_data = get_price_df(coins_to_check, end_date, count=30)

    # 계산에 필요한 변수들 초기화
    result_list = []
    previous_final_asset = 0
    cumulative_profit = 0

    for i, single_date in enumerate(date_range):
        current_date = single_date.date()

        # === (A) 일자별 기초자산 ===
        if i == 0:
            # 첫날 기초자산은 현재 시점 잔고(balances_now)로 계산 (실제로는 과거 시점에 대한 기록 필요)
            # 정확히는 1/1 00:00 시점 잔고가 되어야 함
            daily_initial_asset = 0
            for b in balances_now:
                currency = b['currency']
                balance = float(b['balance'])
                avg_buy_price = float(b['avg_buy_price'])
                if currency == 'KRW':
                    daily_initial_asset += balance
                else:
                    ticker = "KRW-" + currency
                    # current_date 날짜 종가가 있으면 그 값, 없으면 최근값
                    df_price = ohlcv_data.get(ticker)
                    if df_price is not None and current_date in df_price.index:
                        price = df_price.loc[current_date, 'close']
                    else:
                        price = df_price['close'][-1] if df_price is not None else avg_buy_price
                    daily_initial_asset += balance * price
        else:
            # 둘째 날부터는 "전일 기말자산"을 오늘 기초자산으로 사용
            daily_initial_asset = previous_final_asset

        # === (B) 일자별 입금·출금 조회 ===
        deposit_amount = daily_deposits.loc[daily_deposits['date'] == current_date, 'daily_deposit'].sum()
        withdraw_amount = daily_withdraws.loc[daily_withdraws['date'] == current_date, 'daily_withdraw'].sum()

        # === (C) 기말자산(당일 종가로 평가) ===
        # 실제론 이 날(1/2 등)의 코인 보유량도 과거 잔고를 통해 알아야 하나, 예시에서는 '현재 잔고'를 그대로 쓴다.
        balances_now = get_balances()  # 매 loop마다 실시간 API를 부르면 과거 데이터는 반영 불가
        final_asset_value = 0
        for b in balances_now:
            currency = b['currency']
            balance = float(b['balance'])
            avg_buy_price = float(b['avg_buy_price'])
            if currency == 'KRW':
                final_asset_value += balance
            else:
                ticker = "KRW-" + currency
                df_price = ohlcv_data.get(ticker)
                if df_price is not None and current_date in df_price.index:
                    price = df_price.loc[current_date, 'close']
                else:
                    price = df_price['close'][-1] if df_price is not None else avg_buy_price
                final_asset_value += balance * price

        # === (D) 일일 손익 및 수익률 ===
        daily_profit = final_asset_value - daily_initial_asset - (deposit_amount - withdraw_amount)
        if daily_initial_asset != 0:
            daily_return = (daily_profit / daily_initial_asset) * 100
        else:
            daily_return = 0

        # === (E) 누적 손익 및 누적 수익률 ===
        cumulative_profit += daily_profit
        if i == 0:
            first_day_initial_asset = daily_initial_asset
        if first_day_initial_asset != 0:
            cumulative_return = (cumulative_profit / first_day_initial_asset) * 100
        else:
            cumulative_return = 0

        # 리스트에 저장
        result_list.append({
            '일자': current_date,
            '기초자산': daily_initial_asset,
            '기말자산': final_asset_value,
            '입금': deposit_amount,
            '출금': withdraw_amount,
            '일일 손익': daily_profit,
            '일일 수익률(%)': daily_return,
            '누적 손익': cumulative_profit,
            '누적 수익률(%)': cumulative_return
        })

        # 다음 iteration을 위해 기말자산 저장
        previous_final_asset = final_asset_value

    # (4-5) DataFrame 출력
    df_result = pd.DataFrame(result_list)
    print(df_result)


if __name__ == "__main__":
    main()
