import os
import datetime
import pandas as pd
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


def get_futures_ohlcv(client, symbol, interval, start_date, end_date):
    """
    Binance 선물에서 OHLCV 데이터를 가져오는 함수.

    :param client: 초기화된 Binance 클라이언트
    :param symbol: 거래 심볼 (예: "BTCUSDT")
    :param interval: 시간 간격 (예: Client.KLINE_INTERVAL_1HOUR 등)
    :param start_date: 시작 날짜 (YYYY-MM-DD 형식)
    :param end_date: 종료 날짜 (YYYY-MM-DD 형식)
    :return: 캔들스틱 데이터 리스트
    """
    # 날짜 문자열을 datetime 객체로 변환
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    # Binance API는 timestamp를 밀리초 단위로 받으므로 변환
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    # futures_klines API 호출
    klines = client.futures_klines(
        symbol=symbol,
        interval=interval,
        startTime=start_ts,
        endTime=end_ts
    )
    return klines


def convert_to_kst(timestamp):
    """
    밀리초 단위의 timestamp를 한국시간(KST, UTC+9) 형식의 문자열로 변환합니다.
    """
    dt = datetime.datetime.utcfromtimestamp(timestamp / 1000) + datetime.timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def create_dataframe(klines):
    """
    Binance API 반환값을 DataFrame으로 변환하고, human-readable opentime과 closetime 열을 추가합니다.
    """
    columns = [
        "open_time",  # 캔들 시작 시간 (밀리초)
        "open", "high", "low", "close",  # 시가, 고가, 저가, 종가
        "volume",  # 거래량
        "close_time",  # 캔들 종료 시간 (밀리초)
        "quote_asset_volume",  # 해당 기간 동안의 견적 자산 거래량
        "number_of_trades",  # 거래 횟수
        "taker_buy_base_asset_volume",  # 매수 호가 거래량
        "taker_buy_quote_asset_volume",  # 매수 호가 견적 자산 거래량
        "ignore"  # 사용하지 않는 값
    ]
    df = pd.DataFrame(klines, columns=columns)
    # opentime과 closetime 열 추가 (한국시간)
    df['opentime'] = df['open_time'].apply(convert_to_kst)
    df['closetime'] = df['close_time'].apply(convert_to_kst)
    # 열 순서 재정렬: opentime, closetime을 맨 앞에 배치
    new_columns_order = ["opentime", "closetime"] + columns
    return df[new_columns_order]


def save_to_csv(df, filename):
    """
    DataFrame을 CSV 파일로 저장합니다.
    """
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")


def main():
    # Binance 클라이언트 초기화
    client = init_binance_client()

    # 사용자가 쉽게 변경할 수 있는 변수들
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_1HOUR  # Binance 상수 사용 (예: 1시간봉)
    start_date = "2025-01-01"
    end_date = "2025-02-01"

    # OHLCV 데이터 가져오기
    klines = get_futures_ohlcv(client, symbol, interval, start_date, end_date)
    # DataFrame 생성 및 human-readable 시간 열 추가
    df = create_dataframe(klines)

    # CSV 파일 저장 (파일명에 symbol과 interval 포함)
    csv_filename = f"{symbol}_{interval}.csv"
    save_to_csv(df, csv_filename)


if __name__ == '__main__':
    main()
