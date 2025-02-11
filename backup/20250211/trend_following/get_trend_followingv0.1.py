import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
import pandas_ta as ta
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def init_binance_client():
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")
    return Client(access, secret)


def get_futures_ohlcv(client, symbol, interval, start_date=None, end_date=None, limit=1500):
    """
    - start_date, end_date가 주어지면 해당 기간의 OHLCV 데이터를 가져옵니다.
    - 주어지지 않으면, limit 개수만큼의 최근 OHLCV 데이터를 가져옵니다.
    """
    if start_date and end_date:
        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M")
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M")
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
    """
    바이낸스 타임스탬프(ms) -> UTC -> UTC+9(KST) datetime -> 문자열(YYYY-MM-DD HH:MM:SS)
    """
    dt = datetime.datetime.utcfromtimestamp(timestamp / 1000) + datetime.timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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


def add_trend_indicators(df):
    # (1) 컬럼명 변경(pandas_ta 표준)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    # --- Moving Averages ---
    # 1) EMA (length=21)
    ema21 = ta.ema(close=df["Close"], length=21)
    df = pd.concat([df, ema21], axis=1)

    # 2) KAMA (length=10)
    kama10 = ta.kama(df["Close"], length=10)
    df = pd.concat([df, kama10], axis=1)

    # 3) AMAT (fast=12, slow=30)
    amat_df = ta.amat(close=df["Close"], fast=12, slow=30)
    df = pd.concat([df, amat_df], axis=1)

    # --- MACD (fast=8, slow=21, signal=5) ---
    macd_df = ta.macd(df["Close"], fast=8, slow=21, signal=5)
    df = pd.concat([df, macd_df], axis=1)

    # --- ADX (length=9) ---
    adx9 = ta.adx(high=df["High"], low=df["Low"], close=df["Close"], length=9)
    df = pd.concat([df, adx9], axis=1)

    # --- Aroon (length=9) ---
    aroon9 = ta.aroon(high=df["High"], low=df["Low"], length=9)
    df = pd.concat([df, aroon9], axis=1)

    # --- PSAR (af=0.03, max_af=0.3) ---
    psar_df = ta.psar(
        high=df["High"], low=df["Low"], close=df["Close"],
        af=0.03, max_af=0.3
    )
    df = pd.concat([df, psar_df], axis=1)

    # --- Supertrend (length=10, multiplier=3.0) ---
    st_result = ta.supertrend(
        high=df["High"], low=df["Low"], close=df["Close"],
        length=10, multiplier=3.0
    )
    st_df = pd.concat(st_result, axis=1) if isinstance(st_result, tuple) else st_result
    df = pd.concat([df, st_df], axis=1)

    # --- STC (fast=10, slow=20) ---
    stc_df = ta.stc(
        high=df["High"], low=df["Low"], close=df["Close"],
        fast=10, slow=20
    )
    df = pd.concat([df, stc_df], axis=1)

    # --- Vortex (length=14) ---
    vortex14 = ta.vortex(
        high=df["High"], low=df["Low"], close=df["Close"], length=14
    )
    df = pd.concat([df, vortex14], axis=1)

    # --- DM (length=14) ---
    dm14 = ta.dm(
        high=df["High"], low=df["Low"], close=df["Close"], length=14
    )
    df = pd.concat([df, dm14], axis=1)

    # --- KST (짧게 조정 예시) ---
    kst_df = ta.kst(
        df["Close"],
        r1=9, r2=13, r3=15, r4=20,
        sm1=9, sm2=9, sm3=9, sm4=9,
        signal=9
    )
    df = pd.concat([df, kst_df], axis=1)

    # --- TSI (fast=13, slow=25) ---
    tsi_df = ta.tsi(
        df["Close"], fast=13, slow=25
    )
    df = pd.concat([df, tsi_df], axis=1)

    # --- Chop (length=14) ---
    chop14 = ta.chop(
        high=df["High"], low=df["Low"], close=df["Close"], length=14
    )
    df = pd.concat([df, chop14], axis=1)

    # --- DPO (length=10) ---
    dpo10 = ta.dpo(close=df["Close"], length=10)
    df = pd.concat([df, dpo10], axis=1)

    # --- ATR (length=14) ---
    atr14 = ta.atr(
        high=df["High"], low=df["Low"], close=df["Close"], length=14
    )
    df = pd.concat([df, atr14], axis=1)

    # --- Bollinger Bands (length=20, std=2.0) ---
    bbands_df = ta.bbands(close=df["Close"], length=20, std=2.0)
    df = pd.concat([df, bbands_df], axis=1)

    # --- Keltner Channel (length=20, scalar=1.5) ---
    kc_df = ta.kc(
        high=df["High"], low=df["Low"], close=df["Close"],
        length=20, scalar=1.5
    )
    df = pd.concat([df, kc_df], axis=1)

    # --- TTM Squeeze ---
    squeeze_df = ta.squeeze(
        high=df["High"], low=df["Low"], close=df["Close"],
        bb_length=20, bb_std=2.0, kc_length=20, kc_scalar=1.5
    )
    df = pd.concat([df, squeeze_df], axis=1)

    # --- RSI (length=14) ---
    rsi14 = ta.rsi(df["Close"], length=14)
    df = pd.concat([df, rsi14], axis=1)

    # --- OBV ---
    obv_df = ta.obv(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, obv_df], axis=1)

    # --- MFI (length=14) ---
    mfi14 = ta.mfi(
        high=df["High"], low=df["Low"], close=df["Close"],
        volume=df["Volume"], length=14
    )
    df = pd.concat([df, mfi14], axis=1)

    # --- CMF (length=20) ---
    cmf20 = ta.cmf(
        high=df["High"], low=df["Low"], close=df["Close"],
        volume=df["Volume"], length=20
    )
    df = pd.concat([df, cmf20], axis=1)

    # --- AD (Accumulation/Distribution) ---
    ad_df = ta.ad(
        high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"]
    )
    df = pd.concat([df, ad_df], axis=1)

    # --- Ichimoku (tenkan=6, kijun=18, senkou=36) ---
    ichimoku_result = ta.ichimoku(
        high=df["High"], low=df["Low"], close=df["Close"],
        tenkan=6, kijun=18, senkou=36
    )
    ichimoku_df = pd.concat(ichimoku_result, axis=1) if isinstance(ichimoku_result, tuple) else ichimoku_result
    df = pd.concat([df, ichimoku_df], axis=1)

    # (3) 컬럼명 원복
    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    return df


def save_to_csv(df, filename):
    df_copy = df.copy()
    df_copy.reset_index(drop=True, inplace=True)

    if isinstance(df_copy.columns, pd.MultiIndex):
        df_copy.columns = [
            "_".join([str(c) for c in col]).rstrip("_") if isinstance(col, tuple) else col
            for col in df_copy.columns
        ]

    float_cols = df_copy.select_dtypes(include="float").columns
    for c in float_cols:
        df_copy[c] = df_copy[c].round(2)

    df_copy.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")


def save_csv_to_txt(csv_file_path, txt_file_path):
    # 1) 안내 문구 준비
    instructions = (
        "1. 현재 최신 데이터를 확인하라.\n"
        "2. 반드시 모든 보조지표를 검토하라.\n"
        "3. 현재 추세장인지 횡보장인지 분석하라.\n"
        "4. 현재 추세를 점수로 제시하라. 점수: -100점 ~ 100점 (-100점에 가까울수록 내림추세, 100점에 가까울수록 오름추세, 0점에 가까울수록 횡보)\n"
        "5. 포지션(Long, Short), 목표가, 손절가를 제시하라\n"
    )

    # 2) CSV 내용 읽기
    with open(csv_file_path, "r", encoding="utf-8-sig") as f:
        csv_data = f.read()

    # 3) TXT 파일에 쓰기
    with open(txt_file_path, "w", encoding="utf-8-sig") as f:
        # 안내 문구 먼저 기록
        f.write(instructions)
        # 줄바꿈 두 번
        f.write("\n\n")
        # CSV 데이터 기록
        f.write(csv_data)

    print(f"'{txt_file_path}' 파일로 저장이 완료되었습니다.")

def main():
    client = init_binance_client()

    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_5MINUTE

    # 폴더 정리
    folder_name = "report"
    if os.path.exists(folder_name):
        for f in os.listdir(folder_name):
            file_path = os.path.join(folder_name, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        os.makedirs(folder_name)

    # 파일명 접두사
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")

    # 데이터 가져오기 (500개)
    klines = get_futures_ohlcv(client, symbol, interval, limit=1500)
    df_main = create_dataframe(klines)

    # 보조지표 추가
    df_main = add_trend_indicators(df_main)

    # 전체 데이터 CSV 저장
    csv_filename_all = os.path.join(folder_name, f"{timestamp}_data_all.csv")
    save_to_csv(df_main, csv_filename_all)

    # (1) 실제 opentime 있는 데이터 중 최근 10개
    df_with_opentime = df_main[df_main["opentime"].notna()]
    df_last_10 = df_with_opentime.tail(60)

    # (2) opentime이 NaN인 미래행 (일목균형표 선행 스팬)
    df_without_opentime = df_main[df_main["opentime"].isna()]

    # (3) 합쳐서 저장
    df_final = pd.concat([df_last_10, df_without_opentime], axis=0)
    csv_filename_final = os.path.join(folder_name, f"{timestamp}_data_recent10.csv")
    save_to_csv(df_final, csv_filename_final)

    # 바로 TXT 파일로도 저장 (csv_filename_final => txt_filename_final)
    txt_filename_final = os.path.join(folder_name, f"{timestamp}_data_recent10.txt")
    save_csv_to_txt(csv_filename_final, txt_filename_final)


if __name__ == '__main__':
    main()
