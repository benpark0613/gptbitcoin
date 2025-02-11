import datetime
import os

import pandas as pd
import pandas_ta as ta
from binance.client import Client
from dotenv import load_dotenv


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

    # 1. ADX
    df.ta.adx(
        high="High", low="Low", close="Close",
        length=14,             # 권장 표준값
        append=True
    )

    # 2. Choppiness Index
    df.ta.chop(
        high="High", low="Low", close="Close",
        length=14,            # 권장 표준값
        append=True
    )

    # 3. Bollinger Bands
    df.ta.bbands(
        close="Close",
        length=20,            # 표준값
        std=2.0,              # 표준편차
        append=True
    )

    # 4. MACD
    df.ta.macd(
        close="Close",
        fast=12, slow=26,     # 표준값
        signal=9,
        append=True
    )

    # 5. Ichimoku
    df.ta.ichimoku(
        high="High", low="Low", close="Close",
        tenkan=9, kijun=26, senkou=52,   # 전통 일목 값
        append=True
    )

    # 6. Keltner Channel
    df.ta.kc(
        high="High", low="Low", close="Close",
        length=20,            # 표준값
        scalar=1.5,           # 1.5 ~ 2.0
        append=True
    )

    # 7. TTM Trend
    df.ta.ttm_trend(
        high="High", low="Low", close="Close",
        length=20,            # 4시간 봉 기준 노이즈 감소
        append=True
    )

    # 8. Squeeze Pro
    df.ta.squeeze_pro(
        bb_length=20,
        bb_std=2.0,
        kc_length=20,
        kc_mult=1.5,          # 1.5 ~ 2.0
        append=True
    )

    # 9. Squeeze
    df.ta.squeeze(
        bb_length=20,
        bb_std=2.0,
        kc_length=20,
        kc_mult=1.5,
        append=True
    )

    # 10. Aroon
    df.ta.aroon(
        high="High", low="Low",
        length=14,            # 표준값
        append=True
    )

    # 11. Supertrend
    df.ta.supertrend(
        high="High", low="Low", close="Close",
        length=10,            # 10 또는 14
        multiplier=3.0,       # 추세 신호 필터
        append=True
    )

    # 12. Parabolic SAR
    df.ta.psar(
        high="High", low="Low", close="Close",
        step=0.02,            # 표준값
        max_step=0.2,         # 표준값
        append=True
    )

    # 13. Schaff Trend Cycle
    df.ta.stc(
        close="Close",
        fast=14,              # MACD 기반 파라미터
        slow=28,
        factor=0.5,
        append=True
    )

    # 14. KST Oscillator
    df.ta.kst(
        close="Close",
        roc1=9, roc2=13, roc3=15, roc4=20,   # 권장 조합
        smroc1=6, smroc2=6, smroc3=6, smroc4=6,
        signal=9,
        append=True
    )

    # 15. VHF (Vertical Horizontal Filter)
    df.ta.vhf(
        close="Close",
        length=14,            # 표준값
        append=True
    )

    # 16. Efficiency Ratio
    df.ta.er(
        close="Close",
        length=10,            # 반응이 빠른 편
        append=True
    )

    # 17. Inertia (ER 기반 추세 지표)
    df.ta.inertia(
        close="Close",
        r=14,                 # 보편적 표준
        append=True
    )

    # 18. Chande Kroll Stop
    df.ta.cksp(
        high="High", low="Low", close="Close",
        len1=10, len2=20,     # 단기/중기 추세
        append=True
    )

    # 19. Vortex
    df.ta.vortex(
        high="High", low="Low", close="Close",
        length=14,            # ADX와 동일 표준값
        append=True
    )

    # 20. Archer Moving Averages Trends (AMAT)
    df.ta.amat(
        close="Close",
        fast=10, slow=20,     # 단/중기 MA
        append=True
    )

    # 21. Trend Signals (TSignals)
    df.ta.tsignals(
        close="Close",
        length=14,            # 표준값
        append=True
    )

    # 22. Kaufman’s Adaptive Moving Average (KAMA)
    df.ta.kama(
        close="Close",
        length=10,
        fast=2, slow=30,      # 기본 권장값
        append=True
    )

    # 23. Donchian Channel
    df.ta.donchian(
        high="High", low="Low", close="Close",
        lower_length=20, upper_length=20,   # 터틀트레이딩 표준
        append=True
    )

    # 24. Slope
    df.ta.slope(
        close="Close",
        length=14,            # 10 또는 14
        append=True
    )

    # 25. ATR (Average True Range)
    df.ta.atr(
        high="High", low="Low", close="Close",
        length=14,            # 변동성 표준값
        append=True
    )

    # (5) 컬럼명 원복
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
        "5. 점수를 바탕으로 추세추종전략 또는 박스권매매전략을 추천하라.\n"
        "6. 진입가, 목표가, 손절가를 제시하라\n"
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

    # 15m, 1h, 4h: 100, 50, 30
    # 5m, 15m, 1h: 200, 100, 50
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_4HOUR

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
    df_last_10 = df_with_opentime.tail(30)

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
