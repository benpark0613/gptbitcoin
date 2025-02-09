import os
import datetime
import pandas as pd
import pandas_ta as ta  # pandas‑ta 라이브러리 사용
from dotenv import load_dotenv
from binance.client import Client

def init_binance_client():
    """
    환경변수(.env)에서 Binance API 키를 읽어와 Binance 클라이언트를 초기화합니다.
    """
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    return Client(access, secret)

def parse_date(date_str):
    """
    날짜 문자열("YYYY-MM-DD" 또는 "YYYY-MM-DD HH:MM:SS")을 datetime 객체로 파싱합니다.
    """
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d")

def get_futures_ohlcv(client, symbol, interval, start_date, end_date, limit=1500):
    """
    Binance API를 통해 지정된 기간의 OHLCV 데이터를 가져옵니다.
    """
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    klines = client.futures_klines(
        symbol=symbol,
        interval=interval,
        startTime=start_ts,
        endTime=end_ts,
        limit=limit
    )
    return klines

def convert_to_kst(timestamp):
    """
    밀리초 단위 timestamp를 한국시간(KST, UTC+9) 형식의 문자열로 변환합니다.
    """
    dt = datetime.datetime.utcfromtimestamp(timestamp / 1000) + datetime.timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def create_dataframe(klines):
    """
    Binance API에서 받은 원시 OHLCV 데이터를 DataFrame으로 변환합니다.
    """
    columns = [
        "open_time",               # 캔들 시작 시간 (밀리초)
        "open", "high", "low", "close",  # 시가, 고가, 저가, 종가
        "volume",                  # 거래량
        "close_time",              # 캔들 종료 시간 (밀리초)
        "quote_asset_volume",      # 견적 자산 거래량
        "number_of_trades",        # 거래 횟수
        "taker_buy_base_asset_volume",  # 매수 호가 거래량
        "taker_buy_quote_asset_volume", # 매수 호가 견적 자산 거래량
        "ignore"                   # 사용하지 않는 값
    ]
    df = pd.DataFrame(klines, columns=columns)
    df['opentime'] = df['open_time'].apply(convert_to_kst)
    df.drop("ignore", axis=1, inplace=True)
    new_columns_order = ["opentime"] + [col for col in columns if col != "ignore"]
    df = df[new_columns_order]
    return df

def interval_to_timedelta(interval):
    """
    Binance의 캔들 간격 문자열("1m", "5m", "15m", "1h", "4h", "1d" 등)을
    datetime.timedelta 객체로 변환합니다.
    """
    unit = interval[-1]
    quantity = int(interval[:-1])
    if unit == 'm':
        return datetime.timedelta(minutes=quantity)
    elif unit == 'h':
        return datetime.timedelta(hours=quantity)
    elif unit == 'd':
        return datetime.timedelta(days=quantity)
    else:
        raise ValueError("지원하지 않는 간격입니다.")

def calculate_bollinger(df, bollinger_period=20, multiplier=2):
    """
    pandas‑ta 라이브러리를 사용하여 Bollinger Bands 보조지표를 계산합니다.
    df의 'close' 컬럼을 기반으로 ta.bbands()를 호출하고,
    결과로 나온 하한, 중앙(이동평균), 상한 밴드를 소수점 둘째 자리까지 반올림하여
    각각 'BB_lower', 'BB_MA', 'BB_upper' 컬럼에 저장합니다.
    """
    df['close'] = pd.to_numeric(df['close'])
    bb_df = ta.bbands(df['close'], length=bollinger_period, std=multiplier)
    # pandas-ta의 bbands 함수는 "BBL_{period}_{multiplier}", "BBM_{period}_{multiplier}", "BBU_{period}_{multiplier}" 이름의 컬럼을 반환합니다.
    lower_col = f"BBL_{bollinger_period}_{float(multiplier)}"
    middle_col = f"BBM_{bollinger_period}_{float(multiplier)}"
    upper_col = f"BBU_{bollinger_period}_{float(multiplier)}"
    df["BB_lower"] = bb_df[lower_col].round(2)
    df["BB_MA"]    = bb_df[middle_col].round(2)
    df["BB_upper"] = bb_df[upper_col].round(2)
    return df

def add_bollinger_to_ohlcv(df_main, client, symbol, interval, start_date, bollinger_period=20, multiplier=2):
    df_main = calculate_bollinger(df_main, bollinger_period=bollinger_period, multiplier=multiplier)
    return df_main

def run(symbol, interval, start_date, end_date, bollinger_period=20, multiplier=2):
    client = init_binance_client()
    main_klines = get_futures_ohlcv(client, symbol, interval, start_date, end_date)
    df_main = create_dataframe(main_klines)
    df_main = calculate_bollinger(df_main, bollinger_period=bollinger_period, multiplier=multiplier)
    csv_filename = f"{symbol}_{interval}_bollinger.csv"
    save_to_csv(df_main, csv_filename)

def save_to_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

def main():
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_1MINUTE  # 예: 1분 간격
    start_date = "2025-01-01"
    end_date = "2025-02-01"
    run(symbol, interval, start_date, end_date)

if __name__ == '__main__':
    main()
