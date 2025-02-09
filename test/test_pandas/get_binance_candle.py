import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
import pandas_ta as ta

def init_binance_client():
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    return Client(access, secret)

def get_futures_ohlcv(client, symbol, interval, start_date=None, end_date=None, limit=500):
    """
    - start_date, end_date가 주어지면 해당 기간의 OHLCV 데이터를 가져옵니다.
    - 주어지지 않으면, limit 개수만큼의 최근 OHLCV 데이터를 가져옵니다.
    """
    if start_date is not None and end_date is not None:
        # 기존: "%Y-%m-%d" -> 변경: "%Y-%m-%d %H:%M" (또는 "%Y-%m-%d %H:%M:%S")
        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M")  # 변경된 부분
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M")      # 변경된 부분

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

    df["opentime"] = df["open_time"].apply(convert_to_kst)
    df.drop("ignore", axis=1, inplace=True)

    numeric_cols = [
        "open", "high", "low", "close",
        "volume", "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume"
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)

    new_columns_order = ["opentime"] + [c for c in columns if c != "ignore"]
    df = df[new_columns_order]
    return df

def save_to_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

def add_trend_indicators(df):
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    adx_df = ta.adx(high=df["High"], low=df["Low"], close=df["Close"])
    df = pd.concat([df, adx_df], axis=1)

    amat_df = ta.amat(close=df["Close"])
    df = pd.concat([df, amat_df], axis=1)

    aroon_df = ta.aroon(high=df["High"], low=df["Low"])
    df = pd.concat([df, aroon_df], axis=1)

    chop_df = ta.chop(high=df["High"], low=df["Low"], close=df["Close"])
    df = pd.concat([df, chop_df], axis=1)

    cksp_df = ta.cksp(high=df["High"], low=df["Low"], close=df["Close"])
    df = pd.concat([df, cksp_df], axis=1)

    decay_series = ta.decay(df["Close"])
    df = pd.concat([df, decay_series], axis=1)

    decreasing_series = ta.decreasing(df["Close"])
    df = pd.concat([df, decreasing_series], axis=1)

    dpo_df = ta.dpo(close=df["Close"])
    df = pd.concat([df, dpo_df], axis=1)

    increasing_series = ta.increasing(df["Close"])
    df = pd.concat([df, increasing_series], axis=1)

    long_run_df = ta.long_run(close=df["Close"], fast=50, slow=200)
    df = pd.concat([df, long_run_df], axis=1)

    psar_df = ta.psar(high=df["High"], low=df["Low"], close=df["Close"])
    df = pd.concat([df, psar_df], axis=1)

    qstick_df = ta.qstick(open_=df["Open"], close=df["Close"])
    df = pd.concat([df, qstick_df], axis=1)

    short_run_df = ta.short_run(close=df["Close"], fast=5, slow=20)
    df = pd.concat([df, short_run_df], axis=1)

    tsignals_df = ta.tsignals(df["Close"])
    df = pd.concat([df, tsignals_df], axis=1)

    ttm_trend_df = ta.ttm_trend(high=df["High"], low=df["Low"], close=df["Close"])
    df = pd.concat([df, ttm_trend_df], axis=1)

    vhf_df = ta.vhf(close=df["Close"])
    df = pd.concat([df, vhf_df], axis=1)

    vortex_df = ta.vortex(high=df["High"], low=df["Low"], close=df["Close"])
    df = pd.concat([df, vortex_df], axis=1)

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    return df

def main():
    client = init_binance_client()

    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_5MINUTE

    # 예시로 2023-05-01 12:00 부터 2023-05-01 15:30 까지
    # start_date = "2025-02-09 00:00"  # 분단위 포맷
    # end_date   = "2025-02-09 22:00"  # 분단위 포맷
    # klines = get_futures_ohlcv(client, symbol, interval, start_date=start_date, end_date=end_date, limit=1500) # 삭제금지
    klines = get_futures_ohlcv(client, symbol, interval, limit=10) # 삭제금지
    df_main = create_dataframe(klines)

    df_main = add_trend_indicators(df_main)

    csv_filename = f"{symbol}_{interval}_final.csv"
    save_to_csv(df_main, csv_filename)

if __name__ == '__main__':
    main()
