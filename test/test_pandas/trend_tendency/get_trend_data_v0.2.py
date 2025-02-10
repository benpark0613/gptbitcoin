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


import pandas as pd
import pandas_ta as ta

def add_trend_indicators(df):
    """
    5분봉 BTCUSDT 선물 시장에서
    추세장/횡보장 판단에 빠르게 대응하기 위한 예시 지표 & 파라미터 설정
    """

    # (1) 컬럼명 변경(pandas_ta 표준)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    # ADX (length=7)
    adx_df = ta.adx(high=df["High"], low=df["Low"], close=df["Close"], length=7)
    df = pd.concat([df, adx_df], axis=1)

    # AMAT (fast=12, slow=30) - 일반적으로 단타에 자주 쓰임
    amat_df = ta.amat(close=df["Close"], fast=12, slow=30)
    df = pd.concat([df, amat_df], axis=1)

    # Aroon (length=9)
    aroon_df = ta.aroon(high=df["High"], low=df["Low"], length=9)
    df = pd.concat([df, aroon_df], axis=1)

    # CHOP (length=10)
    chop_df = ta.chop(high=df["High"], low=df["Low"], close=df["Close"], length=14)
    df = pd.concat([df, chop_df], axis=1)

    # CKSP (p=10, x=1.0, q=10) - 약간 단축, 변동성 큰 코인 시장 고려
    cksp_df = ta.cksp(high=df["High"], low=df["Low"], close=df["Close"], p=10, x=1.0, q=10)
    df = pd.concat([df, cksp_df], axis=1)

    # DPO (length=10)
    dpo_df = ta.dpo(close=df["Close"], length=10)
    df = pd.concat([df, dpo_df], axis=1)

    # PSAR (af=0.02, max_af=0.2) - 단기반응 증가
    psar_df = ta.psar(high=df["High"], low=df["Low"], close=df["Close"],
                      af=0.02, max_af=0.2)
    df = pd.concat([df, psar_df], axis=1)

    # QStick (length=5)
    qstick_df = ta.qstick(open_=df["Open"], close=df["Close"], length=5)
    df = pd.concat([df, qstick_df], axis=1)

    # TSignals (length=7)
    tsignals_df = ta.tsignals(df["Close"], length=7)
    df = pd.concat([df, tsignals_df], axis=1)

    # VHF (length=10)
    vhf_df = ta.vhf(close=df["Close"], length=10)
    df = pd.concat([df, vhf_df], axis=1)

    # Vortex (length=10)
    vortex_df = ta.vortex(high=df["High"], low=df["Low"], close=df["Close"], length=10)
    df = pd.concat([df, vortex_df], axis=1)

    # ER (length=10)
    er_df = ta.er(df["Close"], length=10)
    df = pd.concat([df, er_df], axis=1)

    # STC (fast=10, slow=20) - 더 짧은 MACD 기반
    stc_df = ta.stc(high=df["High"], low=df["Low"], close=df["Close"], fast=10, slow=20)
    df = pd.concat([df, stc_df], axis=1)

    # MACD (fast=8, slow=21, signal=5)
    macd_df = ta.macd(df["Close"], fast=8, slow=21, signal=5)
    df = pd.concat([df, macd_df], axis=1)

    # Supertrend (length=10, multiplier=2.0)
    st_result = ta.supertrend(high=df["High"], low=df["Low"], close=df["Close"],
                              length=10, multiplier=2.0)
    if isinstance(st_result, tuple):
        st_df = pd.concat(st_result, axis=1)
    else:
        st_df = st_result
    df = pd.concat([df, st_df], axis=1)

    # Bollinger Bands (length=20, std=2.0)
    bbands_df = ta.bbands(close=df["Close"], length=14, std=2.0)
    df = pd.concat([df, bbands_df], axis=1)

    # Keltner Channel (length=20, scalar=1.5)
    kc_df = ta.kc(high=df["High"], low=df["Low"], close=df["Close"],
                  length=14, scalar=1.5)
    df = pd.concat([df, kc_df], axis=1)

    # Donchian Channel (length=20)
    donchian_df = ta.donchian(high=df["High"], low=df["Low"], close=df["Close"],
                              lower_length=14, upper_length=20)
    df = pd.concat([df, donchian_df], axis=1)

    # ATR (length=14) - 변동성 판단
    atr_df = ta.atr(high=df["High"], low=df["Low"], close=df["Close"], length=14)
    df = pd.concat([df, atr_df], axis=1)

    # RSI (length=14)
    rsi_df = ta.rsi(df["Close"], length=14)
    df = pd.concat([df, rsi_df], axis=1)

    # Stochastic (k=14, d=3)
    stoch_df = ta.stoch(high=df["High"], low=df["Low"], close=df["Close"],
                        k=14, d=3)
    df = pd.concat([df, stoch_df], axis=1)

    # Stoch RSI (length=14, rsi_length=14, k=3, d=3)
    stochrsi_df = ta.stochrsi(df["Close"], length=14, rsi_length=14, k=3, d=3)
    df = pd.concat([df, stochrsi_df], axis=1)

    # CCI (length=20)
    cci_df = ta.cci(high=df["High"], low=df["Low"], close=df["Close"], length=20)
    df = pd.concat([df, cci_df], axis=1)

    # OBV
    obv_df = ta.obv(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, obv_df], axis=1)

    # MFI (length=14)
    mfi_df = ta.mfi(high=df["High"], low=df["Low"], close=df["Close"],
                    volume=df["Volume"], length=14)
    df = pd.concat([df, mfi_df], axis=1)

    # CTI (length=14)
    cti_series = ta.cti(df["Close"], length=14)
    df = pd.concat([df, cti_series], axis=1)

    # DM (+DI, -DI) (length=14)
    dm_df = ta.dm(high=df["High"], low=df["Low"], close=df["Close"], length=14)
    df = pd.concat([df, dm_df], axis=1)

    # KAMA (length=10)
    kama_series = ta.kama(df["Close"], length=10)
    df = pd.concat([df, kama_series], axis=1)

    # Squeeze (TTM Squeeze)
    # (bb_length=20, bb_std=2, kc_length=20, kc_scalar=1.5)
    squeeze_df = ta.squeeze(high=df["High"], low=df["Low"], close=df["Close"],
                            bb_length=20, bb_std=2.0,
                            kc_length=20, kc_scalar=1.5)
    df = pd.concat([df, squeeze_df], axis=1)

    # Squeeze Pro
    squeeze_pro_df = ta.squeeze_pro(high=df["High"], low=df["Low"], close=df["Close"],
                                    bb_length=20, bb_std=2.0,
                                    kc_length=20, kc_scalar=1.5)
    df = pd.concat([df, squeeze_pro_df], axis=1)

    # TTM Trend (length=6)
    ttm_df = ta.ttm_trend(
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        length=6
    )
    df = pd.concat([df, ttm_df], axis=1)

    # Ichimoku (tenkan=9, kijun=26, senkou=52) - 기본 일목 파라미터
    ichimoku_result = ta.ichimoku(high=df["High"], low=df["Low"], close=df["Close"],
                                  tenkan=6, kijun=18, senkou=36)
    if isinstance(ichimoku_result, tuple):
        ichimoku_df = pd.concat(ichimoku_result, axis=1)
    else:
        ichimoku_df = ichimoku_result
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
        "1. 반드시 지금 받은 데이터를 새로 분석하세요.\n"
        "2. 반드시 모든 보조지표를 분석하세요.\n"
        "3. 현재 추세장인지 횡보장인지 말하세요.\n"
        "4. 결론에는 점수를 제시해야합니다. 점수: -100점 ~ 100점 "
        "(-100점에 가까울수록 내림추세, 100점에 가까울수록 오름추세, 0점에 가까울수록 횡보)"
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
    folder_name = "trend"
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
    csv_filename_all = os.path.join(folder_name, f"{timestamp}_{symbol}_{interval}_final_all.csv")
    save_to_csv(df_main, csv_filename_all)

    # (1) 실제 opentime 있는 데이터 중 최근 10개
    df_with_opentime = df_main[df_main["opentime"].notna()]
    df_last_10 = df_with_opentime.tail(10)

    # (2) opentime이 NaN인 미래행 (일목균형표 선행 스팬)
    df_without_opentime = df_main[df_main["opentime"].isna()]

    # (3) 합쳐서 저장
    df_final = pd.concat([df_last_10, df_without_opentime], axis=0)
    csv_filename_final = os.path.join(folder_name, f"{timestamp}_{symbol}_{interval}_final_recent10_future.csv")
    save_to_csv(df_final, csv_filename_final)

    # 바로 TXT 파일로도 저장 (csv_filename_final => txt_filename_final)
    txt_filename_final = os.path.join(folder_name, f"{timestamp}_{symbol}_{interval}_final_recent10_future.txt")
    save_csv_to_txt(csv_filename_final, txt_filename_final)


if __name__ == '__main__':
    main()
