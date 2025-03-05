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
    # (1) 컬럼명 변경 (pandas_ta 사용 표준에 맞춤)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    """
    =============================================================================
     (비트코인 선물 5분봉) 추세 파악 & 중요도 순 지표 전체 목록
    =============================================================================

    [1] 추세/방향성 파악에 핵심적인 지표
      1) ADX(length=5)
      2) DM(DI+, DI-)(length=7)
      3) Aroon(length=7)
      4) CHOP(Choppiness Index)(length=10)
      5) VHF(Vertical Horizontal Filter)(length=7)
      6) Vortex(length=7)
      7) Supertrend(length=7, multiplier=2.0)
      8) Ichimoku(tenkan=6, kijun=13, senkou=26)

    [2] 변동성/채널형 지표
      9)  Bollinger Bands(length=14, std=2.0)
      10) Keltner Channel(length=14, scalar=1.5)
      11) Donchian Channel(lower_length=10, upper_length=10)
      12) ATR(length=10)
      13) PSAR(af=0.02, max_af=0.25)

    [3] 모멘텀/오실레이터 & 추세추종/적응형 지표
      14) MACD(fast=5, slow=13, signal=3)
      15) STC(fast=6, slow=12)
      16) AMAT(fast=9, slow=21)
      17) CKSP(p=7, x=1.0, q=7)
      18) DPO(length=7)
      19) RSI(length=9)
      20) Stochastic(k=9, d=3)
      21) Stoch RSI(length=9, rsi_length=9, k=3, d=3)
      22) CCI(length=10)
      23) MFI(length=9)
      24) ER(Efficiency Ratio)(length=7)
      25) CTI(Cycle Trend Index)(length=9)

    [4] 기타 (가격/거래량 기반, 후순위)
      26) QStick(length=3)
      27) TSignals(length=7)
      28) KAMA(length=7)
      29) Squeeze(TTM Squeeze)(bb_length=14, bb_std=2.0, kc_length=14, kc_scalar=1.5)
      30) Squeeze Pro(동일 파라미터)
      31) OBV(별도 파라미터 없음)
    =============================================================================
    """

    # [1] 추세/방향성 파악 지표 ================================================

    # 1) ADX (length=5)
    adx_df = ta.adx(high=df["High"], low=df["Low"], close=df["Close"], length=5)
    df = pd.concat([df, adx_df], axis=1)

    # 2) DM (Directional Movement, +DI/-DI) (length=7)
    dm_df = ta.dm(high=df["High"], low=df["Low"], close=df["Close"], length=7)
    df = pd.concat([df, dm_df], axis=1)

    # 3) Aroon (length=7)
    aroon_df = ta.aroon(high=df["High"], low=df["Low"], length=7)
    df = pd.concat([df, aroon_df], axis=1)

    # 4) CHOP (length=10)
    chop_df = ta.chop(high=df["High"], low=df["Low"], close=df["Close"], length=10)
    df = pd.concat([df, chop_df], axis=1)

    # 5) VHF (length=7)
    vhf_df = ta.vhf(close=df["Close"], length=7)
    df = pd.concat([df, vhf_df], axis=1)

    # 6) Vortex (length=7)
    vortex_df = ta.vortex(high=df["High"], low=df["Low"], close=df["Close"], length=7)
    df = pd.concat([df, vortex_df], axis=1)

    # 7) Supertrend (length=7, multiplier=2.0)
    st_result = ta.supertrend(high=df["High"], low=df["Low"], close=df["Close"],
                              length=7, multiplier=2.0)
    if isinstance(st_result, tuple):
        st_df = pd.concat(st_result, axis=1)
    else:
        st_df = st_result
    df = pd.concat([df, st_df], axis=1)

    # 8) Ichimoku (tenkan=6, kijun=13, senkou=26)
    ichimoku_result = ta.ichimoku(high=df["High"], low=df["Low"], close=df["Close"],
                                  tenkan=6, kijun=13, senkou=26)
    if isinstance(ichimoku_result, tuple):
        ichimoku_df = pd.concat(ichimoku_result, axis=1)
    else:
        ichimoku_df = ichimoku_result
    df = pd.concat([df, ichimoku_df], axis=1)

    # [2] 변동성/채널형 지표 ================================================

    # # 9) Bollinger Bands (length=14, std=2.0)
    # bbands_df = ta.bbands(close=df["Close"], length=14, std=2.0)
    # df = pd.concat([df, bbands_df], axis=1)
    #
    # # 10) Keltner Channel (length=14, scalar=1.5)
    # kc_df = ta.kc(high=df["High"], low=df["Low"], close=df["Close"],
    #               length=14, scalar=1.5)
    # df = pd.concat([df, kc_df], axis=1)
    #
    # # 11) Donchian Channel (length=10,10)
    # donchian_df = ta.donchian(high=df["High"], low=df["Low"], close=df["Close"],
    #                           lower_length=10, upper_length=10)
    # df = pd.concat([df, donchian_df], axis=1)
    #
    # # 12) ATR (length=10)
    # atr_df = ta.atr(high=df["High"], low=df["Low"], close=df["Close"], length=10)
    # df = pd.concat([df, atr_df], axis=1)
    #
    # # 13) PSAR (af=0.02, max_af=0.25)
    # psar_df = ta.psar(high=df["High"], low=df["Low"], close=df["Close"],
    #                   af=0.02, max_af=0.25)
    # df = pd.concat([df, psar_df], axis=1)

    # [3] 모멘텀/오실레이터 & 추세추종/적응형 지표 ============================

    # # 14) MACD (fast=5, slow=13, signal=3)
    # macd_df = ta.macd(df["Close"], fast=5, slow=13, signal=3)
    # df = pd.concat([df, macd_df], axis=1)
    #
    # # 15) STC (fast=6, slow=12)
    # stc_df = ta.stc(high=df["High"], low=df["Low"], close=df["Close"],
    #                 fast=6, slow=12)
    # df = pd.concat([df, stc_df], axis=1)
    #
    # # 16) AMAT (fast=9, slow=21)
    # amat_df = ta.amat(close=df["Close"], fast=9, slow=21)
    # df = pd.concat([df, amat_df], axis=1)
    #
    # # 17) CKSP (p=7, x=1.0, q=7)
    # cksp_df = ta.cksp(high=df["High"], low=df["Low"], close=df["Close"],
    #                   p=7, x=1.0, q=7)
    # df = pd.concat([df, cksp_df], axis=1)
    #
    # # 18) DPO (length=7)
    # dpo_df = ta.dpo(close=df["Close"], length=7)
    # df = pd.concat([df, dpo_df], axis=1)
    #
    # # 19) RSI (length=9)
    # rsi_df = ta.rsi(df["Close"], length=9)
    # df = pd.concat([df, rsi_df], axis=1)
    #
    # # 20) Stochastic (k=9, d=3)
    # stoch_df = ta.stoch(high=df["High"], low=df["Low"], close=df["Close"],
    #                     k=9, d=3)
    # df = pd.concat([df, stoch_df], axis=1)
    #
    # # 21) Stoch RSI (length=9, rsi_length=9, k=3, d=3)
    # stochrsi_df = ta.stochrsi(df["Close"], length=9, rsi_length=9, k=3, d=3)
    # df = pd.concat([df, stochrsi_df], axis=1)
    #
    # # 22) CCI (length=10)
    # cci_df = ta.cci(high=df["High"], low=df["Low"], close=df["Close"], length=10)
    # df = pd.concat([df, cci_df], axis=1)
    #
    # # 23) MFI (length=9)
    # mfi_df = ta.mfi(high=df["High"], low=df["Low"], close=df["Close"],
    #                 volume=df["Volume"], length=9)
    # df = pd.concat([df, mfi_df], axis=1)
    #
    # # 24) ER (length=7)
    # er_df = ta.er(df["Close"], length=7)
    # df = pd.concat([df, er_df], axis=1)
    #
    # # 25) CTI (length=9)
    # cti_series = ta.cti(df["Close"], length=9)
    # df = pd.concat([df, cti_series], axis=1)

    # [4] 기타 지표 ==========================================================

    # # 26) QStick (length=3)
    # qstick_df = ta.qstick(open_=df["Open"], close=df["Close"], length=3)
    # df = pd.concat([df, qstick_df], axis=1)
    #
    # # 27) TSignals (length=7)
    # tsignals_df = ta.tsignals(df["Close"], length=7)
    # df = pd.concat([df, tsignals_df], axis=1)
    #
    # # 28) KAMA (length=7)
    # kama_series = ta.kama(df["Close"], length=7)
    # df = pd.concat([df, kama_series], axis=1)
    #
    # # 29) Squeeze (TTM Squeeze) (bb_length=14, bb_std=2.0, kc_length=14, kc_scalar=1.5)
    # squeeze_df = ta.squeeze(high=df["High"], low=df["Low"], close=df["Close"],
    #                         bb_length=14, bb_std=2.0,
    #                         kc_length=14, kc_scalar=1.5)
    # df = pd.concat([df, squeeze_df], axis=1)
    #
    # # 30) Squeeze Pro (동일 파라미터)
    # squeeze_pro_df = ta.squeeze_pro(high=df["High"], low=df["Low"], close=df["Close"],
    #                                 bb_length=14, bb_std=2.0,
    #                                 kc_length=14, kc_scalar=1.5)
    # df = pd.concat([df, squeeze_pro_df], axis=1)
    #
    # # 31) OBV (On Balance Volume)
    # obv_df = ta.obv(close=df["Close"], volume=df["Volume"])
    # df = pd.concat([df, obv_df], axis=1)
    #
    # # (5) 컬럼명 원복
    # df = df.rename(columns={
    #     "Open": "open",
    #     "High": "high",
    #     "Low": "low",
    #     "Close": "close",
    #     "Volume": "volume"
    # })

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
    csv_filename_all = os.path.join(folder_name, f"{timestamp}_{symbol}_{interval}_final_all.csv")
    save_to_csv(df_main, csv_filename_all)

    # (1) 실제 opentime 있는 데이터 중 최근 10개
    df_with_opentime = df_main[df_main["opentime"].notna()]
    df_last_10 = df_with_opentime.tail(10)

    # # (2) opentime이 NaN인 미래행 (일목균형표 선행 스팬)
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
