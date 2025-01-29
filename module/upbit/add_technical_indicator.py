import pandas as pd
import numpy as np


# ===================== 보조지표 계산 함수들 =====================
def calculate_rsi(df, period=14):
    df = df.copy()
    df["change"] = df["close"].diff()
    df["gain"] = np.where(df["change"] > 0, df["change"], 0)
    df["loss"] = np.where(df["change"] < 0, -df["change"], 0)

    df["avg_gain"] = df["gain"].rolling(window=period).mean()
    df["avg_loss"] = df["loss"].rolling(window=period).mean()

    # 분모가 0이 되지 않도록 처리
    df["rs"] = np.where(df["avg_loss"] == 0, np.nan, df["avg_gain"] / df["avg_loss"])
    df["rsi"] = 100 - (100 / (1.0 + df["rs"]))
    return df["rsi"]


def calculate_macd(df, short=12, long=26, signal=9):
    df = df.copy()
    df["ema_short"] = df["close"].ewm(span=short, adjust=False).mean()
    df["ema_long"] = df["close"].ewm(span=long, adjust=False).mean()
    df["macd"] = df["ema_short"] - df["ema_long"]
    df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    return df["macd"], df["macd_signal"], df["macd_hist"]


def calculate_bollinger_bands(df, period=20, num_std=2):
    df = df.copy()
    df["mbb"] = df["close"].rolling(window=period).mean()  # 중심선(평균)
    df["std"] = df["close"].rolling(window=period).std()  # 표준편차
    df["upper_bb"] = df["mbb"] + (df["std"] * num_std)  # 상단선
    df["lower_bb"] = df["mbb"] - (df["std"] * num_std)  # 하단선
    return df["mbb"], df["upper_bb"], df["lower_bb"]


def calculate_obv(df):
    """On-Balance Volume"""
    df = df.copy()
    df["obv"] = 0.0
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            df.at[df.index[i], "obv"] = df["obv"].iloc[i - 1] + df["volume"].iloc[i]
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            df.at[df.index[i], "obv"] = df["obv"].iloc[i - 1] - df["volume"].iloc[i]
        else:
            df.at[df.index[i], "obv"] = df["obv"].iloc[i - 1]
    return df["obv"]


def calculate_fibonacci_levels(df, lookback=100):
    """
    최근 lookback 개의 봉에서 최소/최대값을 이용해
    기본적인 피보나치 되돌림 구간을 계산합니다.
    """
    recent_df = df.tail(lookback)
    min_price = recent_df["close"].min()
    max_price = recent_df["close"].max()
    diff = max_price - min_price

    fib_levels = {
        "0%": max_price,
        "23.6%": max_price - diff * 0.236,
        "38.2%": max_price - diff * 0.382,
        "50%": max_price - diff * 0.5,
        "61.8%": max_price - diff * 0.618,
        "78.6%": max_price - diff * 0.786,
        "100%": min_price
    }
    return fib_levels


# ===================== 보조지표 추가 함수 =====================
def add_technical_indicators(
        df: pd.DataFrame,
        indicators: list = None
):
    """
    OHLCV DataFrame에 원하는 보조지표만 선택적으로 추가합니다.

    Parameters
    ----------
    df : pd.DataFrame
        'open', 'high', 'low', 'close', 'volume' 컬럼이 포함된 OHLCV 데이터프레임
    indicators : list
        추가할 지표의 목록 (예: ["rsi", "macd", "bollinger", "obv", "ema_50", "ema_200", "fib_levels"])
        None이면 모든 지표를 적용합니다.

    Returns
    -------
    pd.DataFrame
        입력 df의 사본에 선택된 지표들이 새로운 컬럼으로 추가된 DataFrame.
    """
    if df.empty:
        return df

    # 지표 목록이 None이면 모든 지표를 적용
    if indicators is None:
        indicators = ["rsi", "macd", "bollinger", "obv", "ema_50", "ema_200", "fib_levels"]

    # 지표 계산 시 오름차순 정렬 후, 완료 후 복귀
    ascending_order = True
    if df.index[0] > df.index[-1]:
        ascending_order = False
        df = df.sort_index(ascending=True)

    # 복사본 사용(원본 수정 방지)
    df = df.copy()

    # RSI
    if "rsi" in indicators:
        df["rsi"] = calculate_rsi(df)

    # MACD
    if "macd" in indicators:
        macd, macd_signal, macd_hist = calculate_macd(df)
        df["macd"] = macd
        df["macd_signal"] = macd_signal
        df["macd_hist"] = macd_hist

    # Bollinger Bands
    if "bollinger" in indicators:
        bb_mid, bb_upper, bb_lower = calculate_bollinger_bands(df, period=20, num_std=2)
        df["bb_mid"] = bb_mid
        df["bb_upper"] = bb_upper
        df["bb_lower"] = bb_lower
        df["bb_width"] = ((df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]) * 100

    # OBV
    if "obv" in indicators:
        df["obv"] = calculate_obv(df)

    # EMA 50
    if "ema_50" in indicators:
        df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()

    # EMA 200
    if "ema_200" in indicators:
        df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()

    # Fibonacci Levels
    if "fib_levels" in indicators:
        fib_dict = calculate_fibonacci_levels(df, lookback=100)
        # 컬럼으로 추가
        for fib_key, fib_val in fib_dict.items():
            col_name = f"fib_{fib_key}"
            df[col_name] = fib_val

    # 지표 계산 중 생긴 초기 결측치 정리 (필요 시)
    df.dropna(inplace=True)

    # 원래 정렬 순서로 복귀
    df = df.sort_index(ascending=ascending_order)

    return df


# ===================== 모듈 단독 실행 예시 =====================
if __name__ == "__main__":
    import pyupbit

    # 간단히 테스트용 OHLCV 가져오기
    ohlcv_df = pyupbit.get_ohlcv("KRW-BTC", "day", 200)
    print("원본 OHLCV(앞부분):\n", ohlcv_df.head())

    # 원하는 지표만 선택: rsi, macd, bollinger
    selected_indicators = ["rsi", "macd", "bollinger"]
    result_df = add_technical_indicators(ohlcv_df, indicators=selected_indicators)
    print("\n보조지표 추가된 DF(앞부분):\n", result_df.head())