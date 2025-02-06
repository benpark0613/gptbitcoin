import os
import glob
import requests
import pandas as pd
from datetime import datetime

BASE_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
SAVE_DIR = "futures_market_data"

def fetch_data():
    """
    Binance Futures API에서 USDⓈ-M Futures 데이터를 가져옵니다.
    """
    response = requests.get(BASE_URL)
    response.raise_for_status()
    return response.json()

def process_data(data):
    """
    데이터를 Pandas DataFrame으로 변환합니다.
    필요한 열만 선택합니다.
    """
    df = pd.DataFrame(data)
    # 필요한 열만 선택
    df = df[[
        "symbol",            # 거래 심볼
        "lastPrice",         # 마지막 가격
        "priceChangePercent",# 24시간 가격 변동률
        "highPrice",         # 24시간 고가
        "lowPrice",          # 24시간 저가
        "volume",            # 24시간 거래량
        "quoteVolume"        # 24시간 거래대금
    ]]
    # 열 이름을 가독성 좋게 변경
    df.columns = ["Symbol", "Last Price", "24h Change (%)", "24h High", "24h Low", "Volume", "Quote Volume"]
    return df

def save_to_csv(df):
    """
    DataFrame을 CSV 파일로 저장합니다.
    이전에 같은 디렉토리에 존재하는 모든 파일을 제거한 뒤 저장합니다.
    """
    # 1) 폴더 생성
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    # 2) 기존 파일 모두 삭제
    for file_path in glob.glob(os.path.join(SAVE_DIR, "*")):
        os.remove(file_path)

    # 3) CSV 파일 저장
    filename = f"binance_futures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(SAVE_DIR, filename)
    df.to_csv(filepath, index=False, encoding="utf-8")
    print(f"데이터가 {filepath} 파일로 저장되었습니다.")

def main():
    """
    전체 프로세스를 실행합니다.
    """
    print("데이터를 가져오는 중...")
    data = fetch_data()
    print("데이터 처리 중...")
    df = process_data(data)
    print("CSV로 저장 중...")
    save_to_csv(df)
    print("작업이 완료되었습니다!")

if __name__ == "__main__":
    main()
