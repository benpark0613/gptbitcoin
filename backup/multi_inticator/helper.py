import os
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client

def create_binance_client():
    """
    .env 파일에서 API 키를 로드하여 Binance Client 객체를 생성하고 반환합니다.
    """
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    return Client(api_key, api_secret)

def save_csv(df, file_path):
    """
    DataFrame을 지정된 file_path에 CSV 형식으로 저장합니다.
    만약 저장할 디렉토리가 없다면 생성 후 저장합니다.
    """
    dir_path = os.path.dirname(file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    df.to_csv(file_path, index=False, encoding="utf-8")
    print(f"[INFO] CSV saved → {file_path}")

def prepare_futures_df(df):
    """
    바이낸스 선물 데이터를 포함한 DataFrame에 대해 'open_time'을 datetime으로 변환하고,
    필요한 컬럼("open_time_dt", "open", "high", "low", "close", "volume")만 반환합니다.
    """
    df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms")
    columns_to_keep = ["open_time_dt", "open", "high", "low", "close", "volume"]
    return df[columns_to_keep]
