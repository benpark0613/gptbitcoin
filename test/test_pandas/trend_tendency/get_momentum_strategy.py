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
    """
    OHLCV 로 만들 수 있는 주요 보조지표를 pandas_ta 기준으로 예시 추가
    (Volume Profile 등 OHLCV만으로 불가능한 지표는 제외)
    """

    import pandas_ta as ta

    # 1) 컬럼명 변경(pandas_ta 표준)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    # --------------------------------------------
    #  A. 추세 지표 / 모멘텀 지표
    # --------------------------------------------

    # 1. DMI (ADX, DI+, DI-)
    dmi_df = ta.adx(high=df["High"], low=df["Low"], close=df["Close"], length=14)
    df = pd.concat([df, dmi_df], axis=1)

    # 2. Aroon
    aroon_df = ta.aroon(high=df["High"], low=df["Low"], length=14)
    df = pd.concat([df, aroon_df], axis=1)

    # 3. Vortex
    vortex_df = ta.vortex(high=df["High"], low=df["Low"], close=df["Close"], length=14)
    df = pd.concat([df, vortex_df], axis=1)

    # 4. MACD
    macd_df = ta.macd(close=df["Close"], fast=12, slow=26, signal=9)
    df = pd.concat([df, macd_df], axis=1)

    # 5. KST (Know Sure Thing)
    kst_df = ta.kst(close=df["Close"])
    df = pd.concat([df, kst_df], axis=1)

    # 6. TRIX
    trix_df = ta.trix(close=df["Close"], length=14)
    df = pd.concat([df, trix_df], axis=1)

    # 7. TSI (True Strength Index)
    tsi_df = ta.tsi(close=df["Close"], fast=13, slow=25)
    df = pd.concat([df, tsi_df], axis=1)

    # 8. MOM (Momentum)
    mom_df = ta.mom(close=df["Close"], length=10)
    df = pd.concat([df, mom_df], axis=1)

    # 9. ROC (Rate of Change)
    roc_df = ta.roc(close=df["Close"], length=10)
    df = pd.concat([df, roc_df], axis=1)

    # 10. PPO (Percentage Price Oscillator)
    ppo_df = ta.ppo(close=df["Close"], fast=12, slow=26, signal=9)
    df = pd.concat([df, ppo_df], axis=1)

    # 11. APO (Absolute Price Oscillator)
    apo_df = ta.apo(close=df["Close"], fast=12, slow=26)
    df = pd.concat([df, apo_df], axis=1)

    # 12. Ichimoku
    # pandas_ta.ichimoku() 는 기본적으로 여러 컬럼(Conversion, Base, SpanA, SpanB 등)을 반환
    ichimoku_result = ta.ichimoku(high=df["High"], low=df["Low"], close=df["Close"])
    # ichimoku_result 가 tuple 또는 DataFrame 등 버전에 따라 다를 수 있음
    if isinstance(ichimoku_result, tuple):
        for sub_df in ichimoku_result:
            df = pd.concat([df, sub_df], axis=1)
    else:
        df = pd.concat([df, ichimoku_result], axis=1)

    # 13. Parabolic SAR
    psar_df = ta.psar(high=df["High"], low=df["Low"], close=df["Close"], af=0.02, max_af=0.2)
    df = pd.concat([df, psar_df], axis=1)

    # 14. Supertrend
    st_df = ta.supertrend(high=df["High"], low=df["Low"], close=df["Close"], length=10, multiplier=3.0)
    # pandas_ta.supertrend() 의 반환형이 버전에 따라 tuple일 수 있으므로 처리
    if isinstance(st_df, tuple):
        st_df = pd.concat(st_df, axis=1)
    df = pd.concat([df, st_df], axis=1)

    # 16. Slope, Linear Regression (linreg)
    # slope
    slope_series = ta.slope(df["Close"], length=14)
    df = pd.concat([df, slope_series], axis=1)
    # linreg
    linreg_df = ta.linreg(close=df["Close"], length=14)
    df = pd.concat([df, linreg_df], axis=1)

    # 1) SMA
    df["SMA_20"] = ta.sma(df["Close"], length=20)
    # 2) EMA
    df["EMA_20"] = ta.ema(df["Close"], length=20)
    # 3) WMA
    df["WMA_20"] = ta.wma(df["Close"], length=20)
    # 4) HMA
    df["HMA_20"] = ta.hma(df["Close"], length=20)
    # 5) ZLEMA (Zero Lag EMA)
    df["ZLEMA_20"] = ta.zlma(df["Close"], length=20)
    # 6) KAMA (Kaufman’s Adaptive MA)
    df["KAMA_20"] = ta.kama(df["Close"], length=20)
    # 7) MCGINLEY Dynamic (mcgd)
    df["McGinley_20"] = ta.mcgd(df["Close"], length=20)
    # 8) TRIMA
    df["TRIMA_20"] = ta.trima(df["Close"], length=20)
    # 9) T3
    df["T3_20"] = ta.t3(df["Close"], length=20)
    # 10) DEMA
    df["DEMA_20"] = ta.dema(df["Close"], length=20)
    # 11) ALMA
    df["ALMA_20"] = ta.alma(df["Close"], length=20)
    # 12) VIDYA
    df["VIDYA_20"] = ta.vidya(df["Close"], length=20)
    # 13) Holt-Winter Moving Average (HWMA)
    hwma_df = ta.hwma(df["Close"], length=20)  # 반환값 시리즈
    df["HWMA_20"] = hwma_df
    # 14) Ehler’s Super Smoother Filter (SSF)
    try:
        ssf_df = ta.ssfi(df["Close"], length=20)
        df["SSF_20"] = ssf_df
    except:
        pass
    # 15) RMA (Wilder’s Smoothing)
    df["RMA_20"] = ta.rma(df["Close"], length=20)
    # 16) FWMA (Fibonacci WMA)
    df["FWMA_20"] = ta.fwma(df["Close"], length=20)
    # 17) Midprice
    midprice_df = ta.midprice(high=df["High"], low=df["Low"], length=2)  # 예: length=2
    df = pd.concat([df, midprice_df], axis=1)
    # 18) Midpoint
    midpoint_series = ta.midpoint(df["Close"], length=2)
    df["MIDPOINT_2"] = midpoint_series

    # 1) Bollinger Bands
    bbands_df = ta.bbands(close=df["Close"], length=20, std=2.0)
    df = pd.concat([df, bbands_df], axis=1)

    # 2) Keltner Channel
    kc_df = ta.kc(high=df["High"], low=df["Low"], close=df["Close"], length=20, scalar=1.5)
    df = pd.concat([df, kc_df], axis=1)

    # 3) Donchian Channel
    donchian_df = ta.donchian(high=df["High"], low=df["Low"], close=df["Close"], lower_length=20, upper_length=20)
    df = pd.concat([df, donchian_df], axis=1)

    # 4) Mass Index
    massi_df = ta.massi(high=df["High"], low=df["Low"], length=9)
    df = pd.concat([df, massi_df], axis=1)

    # 5) ATR (Average True Range)
    atr_df = ta.atr(high=df["High"], low=df["Low"], close=df["Close"], length=14)
    df = pd.concat([df, atr_df], axis=1)

    # 1) Accumulation/Distribution Index (AD)
    ad_series = ta.ad(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, ad_series], axis=1)

    # 2) Chaikin A/D Oscillator (ADOSC)
    adosc_df = ta.adosc(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"], fast=3, slow=10)
    df = pd.concat([df, adosc_df], axis=1)

    # 3) Chaikin Money Flow (CMF)
    cmf_df = ta.cmf(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"], length=20)
    df = pd.concat([df, cmf_df], axis=1)

    # 4) Elder's Force Index (EFI)
    efi_df = ta.efi(close=df["Close"], volume=df["Volume"], length=13)
    df = pd.concat([df, efi_df], axis=1)

    # 5) Ease of Movement (EOM)
    eom_df = ta.eom(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"], length=14)
    df = pd.concat([df, eom_df], axis=1)

    # 6) Klinger Volume Oscillator (KVO)
    kvo_df = ta.kvo(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, kvo_df], axis=1)

    # 7) MFI (Money Flow Index)
    mfi_df = ta.mfi(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"], length=14)
    df = pd.concat([df, mfi_df], axis=1)

    # 8) Negative Volume Index (NVI)
    nvi_series = ta.nvi(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, nvi_series], axis=1)

    # 9) On-Balance Volume (OBV)
    obv_series = ta.obv(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, obv_series], axis=1)

    # 10) Positive Volume Index (PVI)
    pvi_series = ta.pvi(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, pvi_series], axis=1)

    # 11) Price Volume Trend (PVT)
    pvt_series = ta.pvt(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, pvt_series], axis=1)

    # --------------------------------------------
    #  E. 통계(Statistics) 지표
    # --------------------------------------------
    # Entropy
    ent_series = ta.entropy(df["Close"], length=10)
    df = pd.concat([df, ent_series], axis=1)

    # Kurtosis
    kurt_df = ta.kurtosis(df["Close"], length=10)
    df = pd.concat([df, kurt_df], axis=1)

    # Mean Absolute Deviation
    mad_df = ta.mad(df["Close"], length=10)
    df = pd.concat([df, mad_df], axis=1)

    # Median
    median_df = ta.median(df["Close"], length=10)
    df = pd.concat([df, median_df], axis=1)

    # Quantile
    # pandas_ta.quantile()에 quantile 값(q=0.5) 등 파라미터 필요
    quantile_df = ta.quantile(df["Close"], length=10, q=0.5)
    df = pd.concat([df, quantile_df], axis=1)

    # Skew
    skew_df = ta.skew(df["Close"], length=10)
    df = pd.concat([df, skew_df], axis=1)

    # Standard Deviation
    stdev_df = ta.stdev(df["Close"], length=10)
    df = pd.concat([df, stdev_df], axis=1)

    # Variance
    var_df = ta.variance(df["Close"], length=10)
    df = pd.concat([df, var_df], axis=1)

    # Z-Score
    zscore_df = ta.zscore(df["Close"], length=10)
    df = pd.concat([df, zscore_df], axis=1)

    # --------------------------------------------
    #  최종적으로 컬럼명 원복
    # --------------------------------------------
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

    # (2-b) 아래는 NaN없이 최근 10개만 CSV로 추가 저장하고 싶다면:
    csv_filename_final = os.path.join(folder_name, f"{timestamp}_{symbol}_{interval}_final_recent10_only.csv")
    save_to_csv(df_last_10, csv_filename_final)


if __name__ == '__main__':
    main()
