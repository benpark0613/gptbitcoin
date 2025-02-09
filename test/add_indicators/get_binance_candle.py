import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client

from test.add_indicators.adx.add_adx import add_adx_to_ohlcv
from test.add_indicators.bollinger.add_bollinger import add_bollinger_to_ohlcv
from test.add_indicators.ma.add_ma import add_ma_to_ohlcv
# 새로 만든 pivot 로직
from test.add_indicators.price_action.price_action_indicator import add_price_action_signals

def init_binance_client():
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    return Client(access, secret)

def get_futures_ohlcv(client, symbol, interval, start_date=None, end_date=None, limit=500):
    """
    - start_date, end_date 가 주어지면, 해당 기간의 OHLCV 데이터를 가져옵니다.
    - 주어지지 않으면, limit 개수만큼의 최근 OHLCV 데이터를 가져옵니다.
    """
    # 둘 다 None이 아니라면 특정 기간 조회
    if start_date is not None and end_date is not None:
        # 날짜 문자열을 datetime으로 변환
        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            startTime=start_ts,
            endTime=end_ts,
            limit=limit
        )
    else:
        # 특정 기간이 없는 경우: 현재 시점 기준으로 최근 limit개의 캔들
        klines = client.futures_klines(
            symbol=symbol,
            interval=interval,
            limit=limit
        )
    return klines

def convert_to_kst(timestamp):
    dt = datetime.datetime.utcfromtimestamp(timestamp / 1000) + datetime.timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def create_dataframe(klines):
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
    df['opentime'] = df['open_time'].apply(convert_to_kst)
    df.drop("ignore", axis=1, inplace=True)
    new_columns_order = ["opentime"] + [c for c in columns if c != "ignore"]
    df = df[new_columns_order]
    return df

def save_to_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

def main():
    client = init_binance_client()

    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_5MINUTE
    start_date = "2025-01-01"
    end_date = "2025-02-01"

    klines = get_futures_ohlcv(client, "BTCUSDT", interval,
                               start_date="2025-01-01",
                               end_date="2025-02-01",
                               limit=1500)
    # klines = get_futures_ohlcv(client, "BTCUSDT", interval,
    #                            limit=500)
    df_main = create_dataframe(klines)

    # 1) 기존 지표들 계산
    df_main = add_adx_to_ohlcv(df_main, client, symbol, interval, start_date)  # ADX
    df_main = add_bollinger_to_ohlcv(df_main, client, symbol, interval, start_date)
    df_main = add_ma_to_ohlcv(df_main, client, symbol, interval, start_date, ma_type='ema')

    # 2) 마지막으로 pivot(PriceAction) 시그널 추가
    df_main = add_price_action_signals(df_main)  # 여기서 high/low 컬럼 훼손 안 한다면 OK

    csv_filename = f"{symbol}_{interval}_final.csv"
    save_to_csv(df_main, csv_filename)

if __name__ == '__main__':
    main()
