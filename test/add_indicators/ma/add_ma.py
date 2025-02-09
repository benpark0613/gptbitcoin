import os
import datetime
import pandas as pd
import numpy as np
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
    밀리초 단위의 timestamp를 한국시간(KST, UTC+9) 형식의 문자열로 변환합니다.
    """
    dt = datetime.datetime.utcfromtimestamp(timestamp / 1000) + datetime.timedelta(hours=9)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def create_dataframe(klines):
    """
    Binance API에서 받은 원시 OHLCV 데이터를 DataFrame으로 변환합니다.
    """
    columns = [
        "open_time",  # 캔들 시작 시간 (밀리초)
        "open", "high", "low", "close",  # 시가, 고가, 저가, 종가
        "volume",  # 거래량
        "close_time",  # 캔들 종료 시간 (밀리초)
        "quote_asset_volume",  # 견적 자산 거래량
        "number_of_trades",  # 거래 횟수
        "taker_buy_base_asset_volume",  # 매수 호가 거래량
        "taker_buy_quote_asset_volume",  # 매수 호가 견적 자산 거래량
        "ignore"  # 사용하지 않는 값
    ]
    df = pd.DataFrame(klines, columns=columns)
    df['opentime'] = df['open_time'].apply(convert_to_kst)
    df.drop("ignore", axis=1, inplace=True)
    new_order = ["opentime"] + [col for col in columns if col != "ignore"]
    df = df[new_order]
    return df


def interval_to_timedelta(interval):
    """
    Binance의 캔들 간격 문자열("1m", "5m", "15m", "30m", "1h", "4h", "1w", "1d" 등)을
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
    elif unit == 'w':
        return datetime.timedelta(weeks=quantity)
    else:
        raise ValueError("지원하지 않는 간격입니다.")


def get_ma_periods(interval):
    """
    차트 시간프레임에 따른 단기, 중기, 장기 이동평균 기간을 추천합니다.
    비트코인 선물의 경우, 1m, 5m, 15m, 30m 차트는 (5, 20, 60),
    1h 이상 차트는 (10, 50, 200)를 기본으로 사용합니다.
    """
    mapping = {
        '1m': (5, 20, 60),
        '5m': (5, 20, 60),
        '15m': (5, 20, 60),
        '30m': (5, 20, 60),
        '1h': (10, 50, 200),
        '4h': (10, 50, 200),
        '1w': (10, 50, 200),
        '1d': (10, 50, 200)
    }
    return mapping.get(interval, (5, 20, 60))


def calculate_slope_series(series, window=10, slope_mode='relative'):
    """
    주어진 series에 대해 rolling window를 사용하여 선형 회귀 기울기를 계산합니다.
    - window: 기울기를 계산할 기간(윈도우 길이)
    - slope_mode: 'absolute' (원시 값 기준) 또는 'relative' (로그 변환 후 계산)
      비트코인 선물과 같이 가격 규모가 크고 변동성이 높은 경우 'relative' 방식(로그 기반)이 더 안정적인 추세 평가에 도움이 됩니다.
    반환: 동일한 길이의 pandas Series (윈도우 내 데이터가 부족한 경우 NaN)
    """

    def polyfit_slope(x):
        # x 값이 모두 양수인지 확인 후, relative 모드일 경우 로그 변환
        if slope_mode == 'relative':
            y = np.log(x)
        else:
            y = x
        return np.polyfit(range(len(y)), y, 1)[0]

    return series.rolling(window=window).apply(polyfit_slope, raw=True)


def calculate_ma(df, periods, ma_type='ema', slope_window=10, slope_mode='relative'):
    """
    주어진 기간(periods: (단기, 중기, 장기))과 ma_type('ema', 'sma', 'ma')에 따라 이동평균을 계산하고,
    각 MA의 기울기를 계산하여 DataFrame에 추가합니다.
    결과 컬럼은 각각 "MA_<단기>", "MA_<중기>", "MA_<장기>"와
    "Slope_MA_<단기>", "Slope_MA_<중기>", "Slope_MA_<장기>" 입니다.
    """
    df['close'] = pd.to_numeric(df['close'])
    short, medium, long = periods
    if ma_type.lower() == 'ema':
        df[f"MA_{short}"] = ta.ema(df['close'], length=short)
        df[f"MA_{medium}"] = ta.ema(df['close'], length=medium)
        df[f"MA_{long}"] = ta.ema(df['close'], length=long)
    elif ma_type.lower() in ['sma', 'ma']:
        df[f"MA_{short}"] = ta.sma(df['close'], length=short)
        df[f"MA_{medium}"] = ta.sma(df['close'], length=medium)
        df[f"MA_{long}"] = ta.sma(df['close'], length=long)
    else:
        # 기본은 EMA로 계산
        df[f"MA_{short}"] = ta.ema(df['close'], length=short)
        df[f"MA_{medium}"] = ta.ema(df['close'], length=medium)
        df[f"MA_{long}"] = ta.ema(df['close'], length=long)

    # 반올림
    df[f"MA_{short}"] = df[f"MA_{short}"].round(2)
    df[f"MA_{medium}"] = df[f"MA_{medium}"].round(2)
    df[f"MA_{long}"] = df[f"MA_{long}"].round(2)

    # 각 MA에 대한 기울기 계산 (rolling window: slope_window)
    df[f"Slope_MA_{short}"] = calculate_slope_series(df[f"MA_{short}"], window=slope_window,
                                                     slope_mode=slope_mode).round(4)
    df[f"Slope_MA_{medium}"] = calculate_slope_series(df[f"MA_{medium}"], window=slope_window,
                                                      slope_mode=slope_mode).round(4)
    df[f"Slope_MA_{long}"] = calculate_slope_series(df[f"MA_{long}"], window=slope_window, slope_mode=slope_mode).round(
        4)
    return df


def calculate_weighted_slope(df, periods, weights=(0.2, 0.3, 0.5)):
    """
    단기, 중기, 장기 MA 기울기를 가중 평균하여 하나의 'Weighted_Slope' 컬럼으로 추가합니다.
    weights: (weight_short, weight_medium, weight_long) - 기본값은 0.2, 0.3, 0.5
    """
    short, medium, long = periods
    df["Weighted_Slope"] = (df[f"Slope_MA_{short}"] * weights[0] +
                   df[f"Slope_MA_{medium}"] * weights[1] +
                   df[f"Slope_MA_{long}"] * weights[2]).round(4)
    return df


def add_ma_to_ohlcv(df_main, client, symbol, interval, start_date, ma_type='ema', slope_window=10,
                    slope_mode='relative'):
    """
    메인 OHLCV 데이터(df_main)에 대해 선택된 MA 종류(ma_type)를 사용하여
    단기, 중기, 장기 이동평균과 각 MA의 기울기를 추가하고,
    가중 평균 기울기를 'Weighted_Slope' 컬럼으로 계산하여 함께 반환합니다.
    시간프레임(interval)에 따라 추천 기간을 자동으로 적용합니다.
    """
    periods = get_ma_periods(interval)
    df_main = calculate_ma(df_main, periods, ma_type=ma_type, slope_window=slope_window, slope_mode=slope_mode)
    df_main = calculate_weighted_slope(df_main, periods, weights=(0.2, 0.3, 0.5))
    return df_main


def run(symbol, interval, start_date, end_date, ma_type='ema', slope_window=10, slope_mode='relative'):
    client = init_binance_client()
    klines = get_futures_ohlcv(client, symbol, interval, start_date, end_date)
    df_main = create_dataframe(klines)
    df_main = add_ma_to_ohlcv(df_main, client, symbol, interval, start_date, ma_type=ma_type,
                              slope_window=slope_window, slope_mode=slope_mode)
    csv_filename = f"{symbol}_{interval}_{ma_type}_ma.csv"
    save_to_csv(df_main, csv_filename)


def save_to_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"데이터가 '{filename}' 파일로 저장되었습니다.")


def main():
    # 예시: 사용자는 차트 시간프레임과 MA 종류를 선택할 수 있습니다.
    symbol = "BTCUSDT"
    # 차트 간격: "1m", "5m", "15m", "30m", "1h", "4h", "1w", "1d" 등
    interval = Client.KLINE_INTERVAL_1HOUR  # 예: "1m"
    start_date = "2025-01-01"
    end_date = "2025-02-01"
    ma_type = 'ema'  # 사용자가 선택: 'ema', 'sma', 또는 'ma'
    run(symbol, interval, start_date, end_date, ma_type=ma_type, slope_window=10, slope_mode='relative')


if __name__ == '__main__':
    main()
