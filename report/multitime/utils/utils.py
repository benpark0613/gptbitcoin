# utils.py

import os
import datetime
import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

def init_binance_client():
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    return Client(access, secret)

def convert_to_kst(timestamp_ms):
    """
    바이낸스 타임스탬프(ms) -> UTC -> UTC+9(KST) datetime -> 문자열(YYYY-MM-DD HH:MM:SS)
    """
    dt = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000) + datetime.timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def get_futures_ohlcv(client, symbol, interval, limit=1500):
    """
    - limit 개수만큼의 최근 OHLCV 데이터를 가져옵니다.
    """
    klines = client.futures_klines(
        symbol=symbol,
        interval=interval,
        limit=limit
    )
    return klines

def create_dataframe(klines):
    """
    RangeIndex 유지 + opentime 컬럼(문자열) 생성
    """
    columns = [
        "open_time",
        "open", "high", "low", "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore"
    ]
    df = pd.DataFrame(klines, columns=columns)

    # opentime (문자열)
    df["opentime"] = df["open_time"].apply(convert_to_kst)
    df.drop("ignore", axis=1, inplace=True)

    numeric_cols = [
        "open", "high", "low", "close",
        "volume", "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume"
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)

    # 열 순서 재배치
    new_columns_order = ["opentime"] + [c for c in columns if c != "ignore"]
    df = df[new_columns_order]

    return df

def save_to_csv(df, filename):
    df_copy = df.copy()
    df_copy.reset_index(drop=True, inplace=True)

    # 멀티 인덱스 처리
    if isinstance(df_copy.columns, pd.MultiIndex):
        df_copy.columns = [
            "_".join([str(c) for c in col]).rstrip("_") if isinstance(col, tuple) else col
            for col in df_copy.columns
        ]

    # 소수 둘째 자리로 반올림
    float_cols = df_copy.select_dtypes(include="float").columns
    for c in float_cols:
        df_copy[c] = df_copy[c].round(2)

    df_copy.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

def cleanup_report_folder(folder_name):
    """
    보고서를 저장할 폴더를 초기화(존재하면 내부 파일 삭제, 없으면 생성) 합니다.
    """
    if os.path.exists(folder_name):
        for f in os.listdir(folder_name):
            file_path = os.path.join(folder_name, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        os.makedirs(folder_name)

def get_instructions_text():
    """
    최종 TXT 파일 상단에 들어갈 안내 문구를 반환합니다.
    """
    return (
        "1. 현재 최신 데이터를 확인하라.\n"
        "2. 반드시 모든 보조지표를 검토하라.\n"
        "3. 현재 추세장인지 횡보장인지 분석하라.\n"
        "4. 현재 추세를 점수로 제시하라. 추세 점수: -100점 ~ 100점\n"
        "(-100점에 가까울수록 내림추세, 100점에 가까울수록 오름추세, 0점에 가까울수록 횡보)\n"
        "(횡보장 = -49 < 추세 점수 < 49 / 하락 추세장 = 추세 점수 <= -50 / 상승 추세장 = 추세 점수 >= 51)\n"
        "6. 포지션(LONG, SHORT), 진입가, 목표가, 손절가를 제시하라\n"
    )
