# utils/binance_data.py

"""
binance_data.py

Binance 선물 데이터를 수집하는 모듈.
OOP 방식을 사용하여 API 호출 관련 로직을 클래스로 캡슐화.
폴더 정리 유틸은 별도 모듈로 분리(예: file_io.py)하여 import해서 사용.
테스트는 if __name__ == "__main__": 아래 main() 함수에서만 수행.
"""

import os
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from binance.client import Client
from dotenv import load_dotenv

# global_settings.py 에 있는 START_DATE, END_DATE 임포트 (프로젝트 구조에 맞게 경로 조정)
from settings.config import START_DATE, END_DATE

# 별도 유틸 모듈로 분리된 폴더 정리 함수
# 예) gptbitcoin/utils/file_io.py 내 clean_data_folder
from utils.file_io import clean_data_folder

load_dotenv()
DEFAULT_API_KEY = os.getenv("BINANCE_ACCESS_KEY", "")
DEFAULT_API_SECRET = os.getenv("BINANCE_SECRET_KEY", "")

class BinanceDataFetcher:
    """
    Binance API를 통해 선물(선물 K라인) 데이터를 가져오는 클래스.
    """

    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key if api_key else DEFAULT_API_KEY
        self.api_secret = api_secret if api_secret else DEFAULT_API_SECRET
        self.client = Client(self.api_key, self.api_secret)

    def fetch_futures_klines(
        self,
        symbol,
        interval,
        start_date=None,
        end_date=None,
        months=None
    ):
        """
        바이낸스 선물 K라인 데이터를 DataFrame으로 반환한다.

        Parameters
        ----------
        symbol : str
            예) "BTCUSDT"
        interval : str
            예) "1h", "4h", "1d" 등
        start_date : str, optional
            "YYYY-MM-DD" 형태의 시작 일자. 지정하면 months 무시됨.
        end_date : str, optional
            "YYYY-MM-DD" 형태의 종료 일자. start_date와 함께 사용.
        months : int, optional
            start_date, end_date 둘 다 미지정 시 최근 n개월간 데이터를 가져옴.

        Returns
        -------
        pd.DataFrame
            [open_time, open, high, low, close, volume] 컬럼을 가진 DataFrame.
        """
        # 날짜 범위 설정
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end   = datetime.strptime(end_date,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            if months is None:
                months = 1
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=30 * months)

        df_list = []
        while True:
            limit = 1500
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=int(start.timestamp() * 1000),
                endTime=int(end.timestamp() * 1000)
            )
            if not klines:
                break

            df_part = pd.DataFrame(klines, columns=[
                "open_time","open","high","low","close","volume",
                "close_time","quote_vol","trades","taker_base",
                "taker_quote","ignore"
            ])
            df_part["open_time"] = pd.to_datetime(
                df_part["open_time"], unit='ms', utc=True
            )

            last_close_time = pd.to_datetime(
                df_part["close_time"].iloc[-1], unit='ms', utc=True
            )
            df_list.append(df_part)

            # 마지막 close_time이 end 이상이면 종료
            if last_close_time >= end:
                break

            # 다음 차수의 시작 지점
            start = last_close_time + timedelta(milliseconds=1)
            time.sleep(0.2)

        if not df_list:
            return pd.DataFrame()

        df = pd.concat(df_list).drop_duplicates(subset=["open_time"]).reset_index(drop=True)

        # 필요한 컬럼만 남김
        df = df[["open_time","open","high","low","close","volume"]]
        df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
        df = df.sort_values("open_time").reset_index(drop=True)

        return df

def main():
    """
    이 모듈을 단독 실행했을 때 동작하는 간단한 테스트 함수.
    """
    # DATA 폴더 정리
    data_dir = os.path.join(os.path.dirname(__file__), "data_fetcher")
    clean_data_folder(data_dir)

    symbol   = "BTCUSDT"
    interval = "1h"

    # config.py에서 불러온 날짜 사용 (예: START_DATE="2024-01-01", END_DATE="2024-12-31")
    fetcher = BinanceDataFetcher()
    df = fetcher.fetch_futures_klines(symbol, interval, start_date=START_DATE, end_date=END_DATE)

    # 결과 저장
    csv_path = os.path.join(data_dir, f"test_{symbol}_{interval}.csv")
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}. (Rows={len(df)})")

    if not df.empty:
        print(df.head())
    else:
        print("No data_fetcher fetched.")

if __name__ == "__main__":
    main()
