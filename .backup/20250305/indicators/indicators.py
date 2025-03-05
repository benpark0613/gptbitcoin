# gptbitcoin/indicators/indicators.py
# MA, RSI, OBV, Filter, Support/Resistance, Channel Breakout 등 보조지표 계산 모듈
# 주석은 필요한 최소한으로 작성(한글), docstring은 구글 스타일로 작성

import pandas as pd
import numpy as np

def compute_ma(
    df: pd.DataFrame,
    period: int,
    price_col: str = "close",
    col_name: str = None
) -> pd.DataFrame:
    """
    이동평균(SMA)을 계산하고 결과를 df에 새로운 칼럼으로 추가한다.
    윈도우 초기 구간에서 NaN이 발생할 수 있으며, 이를 발견하면 예외를 발생시킨다.

    Args:
        df (pd.DataFrame): OHLCV 등을 포함하는 DataFrame
        period (int): 이동평균 기간
        price_col (str, optional): 기준이 되는 가격 칼럼명. 기본값 'close'
        col_name (str, optional): 결과를 저장할 컬럼명. 기본값은 None -> f"MA_{period}"

    Returns:
        pd.DataFrame: 입력 df에 이동평균 결과 칼럼을 추가한 DataFrame

    Raises:
        ValueError: 계산 결과에 NaN(결측치)이 존재하면 발생
    """
    if col_name is None:
        col_name = f"MA_{period}"

    df[col_name] = df[price_col].rolling(window=period, min_periods=period).mean()

    # NaN 검사
    if df[col_name].isnull().any():
        raise ValueError(f"compute_ma(): '{col_name}' NaN 발생. 워밍업 데이터 부족 혹은 설정 확인 필요.")

    return df


def compute_rsi(
    df: pd.DataFrame,
    period: int,
    price_col: str = "close",
    col_name: str = None
) -> pd.DataFrame:
    """
    RSI(Relative Strength Index)를 계산한다(Wilder 방식).
    윈도우 초기 구간에서 NaN이 발생할 수 있으며, 이를 발견하면 예외를 발생시킨다.

    Args:
        df (pd.DataFrame): OHLCV 등을 포함하는 DataFrame
        period (int): RSI 계산 기간
        price_col (str, optional): 기준이 되는 가격 칼럼명. 기본값 'close'
        col_name (str, optional): 결과를 저장할 컬럼명. 기본값은 None -> f"RSI_{period}"

    Returns:
        pd.DataFrame: 입력 df에 RSI 결과 칼럼을 추가한 DataFrame

    Raises:
        ValueError: 계산 결과에 NaN(결측치)이 존재하면 발생
    """
    if col_name is None:
        col_name = f"RSI_{period}"

    # 종가 기준 변화량
    delta = df[price_col].diff()

    # 상승분, 하락분 분리
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)

    # 평균 상승/하락 (Wilder의 경우 지수방식 or 단순방식 선택 가능)
    roll_up = up.rolling(window=period, min_periods=period).mean()
    roll_down = down.rolling(window=period, min_periods=period).mean()

    # 분모가 0이 되지 않도록 처리
    rs = roll_up / (roll_down + 1e-14)

    df[col_name] = 100.0 - (100.0 / (1.0 + rs))

    if df[col_name].isnull().any():
        raise ValueError(f"compute_rsi(): '{col_name}' NaN 발생. 워밍업 데이터 부족 혹은 설정 확인 필요.")

    return df


def compute_obv(
    df: pd.DataFrame,
    price_col: str = "close",
    vol_col: str = "volume",
    obv_col: str = "OBV"
) -> pd.DataFrame:
    """
    OBV(On-Balance Volume)를 계산하고 결과를 df에 추가한다.
    OBV = 이전 OBV + (금일 거래량) or - (금일 거래량)
      - 종가가 이전보다 상승하면 더하고, 하락하면 빼는 방식

    Args:
        df (pd.DataFrame): OHLCV 등을 포함하는 DataFrame
        price_col (str, optional): 종가 칼럼명
        vol_col (str, optional): 거래량 칼럼명
        obv_col (str, optional): 결과 OBV 칼럼명

    Returns:
        pd.DataFrame: 입력 df에 OBV 결과 칼럼을 추가한 DataFrame

    Raises:
        ValueError: 계산 결과에 NaN(결측치)이 존재하면 발생
    """
    # 1) 우선 0행에 대해서는 OBV=해당 행의 거래량 or 0 등으로 초기화
    df[obv_col] = 0.0

    # 2) 이전 종가와 비교
    for i in range(1, len(df)):
        if df.loc[i, price_col] > df.loc[i - 1, price_col]:
            df.loc[i, obv_col] = df.loc[i - 1, obv_col] + df.loc[i, vol_col]
        elif df.loc[i, price_col] < df.loc[i - 1, price_col]:
            df.loc[i, obv_col] = df.loc[i - 1, obv_col] - df.loc[i, vol_col]
        else:
            df.loc[i, obv_col] = df.loc[i - 1, obv_col]

    if df[obv_col].isnull().any():
        raise ValueError("compute_obv(): OBV 계산 중 NaN 발생.")

    return df


def compute_rolling_min(
    df: pd.DataFrame,
    period: int,
    price_col: str = "close",
    col_name: str = None
) -> pd.DataFrame:
    """
    주어진 윈도우(period) 동안의 최솟값(rolling min)을 계산한다.

    Args:
        df (pd.DataFrame): 데이터프레임
        period (int): 윈도우 크기
        price_col (str, optional): 기준 칼럼
        col_name (str, optional): 결과 칼럼명

    Returns:
        pd.DataFrame: rolling min 결과 칼럼이 추가된 DataFrame

    Raises:
        ValueError: NaN 존재 시
    """
    if col_name is None:
        col_name = f"Min_{period}"
    df[col_name] = df[price_col].rolling(window=period, min_periods=period).min()

    if df[col_name].isnull().any():
        raise ValueError(f"compute_rolling_min(): '{col_name}' NaN 발생.")

    return df


def compute_rolling_max(
    df: pd.DataFrame,
    period: int,
    price_col: str = "close",
    col_name: str = None
) -> pd.DataFrame:
    """
    주어진 윈도우(period) 동안의 최대값(rolling max)을 계산한다.

    Args:
        df (pd.DataFrame): 데이터프레임
        period (int): 윈도우 크기
        price_col (str, optional): 기준 칼럼
        col_name (str, optional): 결과 칼럼명

    Returns:
        pd.DataFrame: rolling max 결과 칼럼이 추가된 DataFrame

    Raises:
        ValueError: NaN 존재 시
    """
    if col_name is None:
        col_name = f"Max_{period}"
    df[col_name] = df[price_col].rolling(window=period, min_periods=period).max()

    if df[col_name].isnull().any():
        raise ValueError(f"compute_rolling_max(): '{col_name}' NaN 발생.")

    return df


def compute_channel_width(
    df: pd.DataFrame,
    high_col: str,
    low_col: str,
    col_name: str = "channel_width"
) -> pd.DataFrame:
    """
    채널 폭(고점-저점)을 계산한다. 예: 채널 브레이크아웃 등에서 사용할 수 있음.

    Args:
        df (pd.DataFrame): 데이터프레임 (이미 rolling max/min이 계산되어 있어야 함)
        high_col (str): 고점 칼럼명
        low_col (str): 저점 칼럼명
        col_name (str, optional): 결과 칼럼명

    Returns:
        pd.DataFrame: 채널 폭 계산 결과 칼럼이 추가된 DataFrame

    Raises:
        ValueError: NaN 존재 시
    """
    df[col_name] = df[high_col] - df[low_col]

    if df[col_name].isnull().any():
        raise ValueError(f"compute_channel_width(): '{col_name}' NaN 발생.")

    return df


if __name__ == "__main__":
    """
    간단한 테스트 코드 예시
    """
    data = {
        "close": [100, 102, 101, 105, 110, 108, 109, 111, 115, 117, 120],
        "volume": [1.0, 2.0, 1.5, 3.0, 2.5, 2.0, 2.2, 3.1, 2.8, 2.9, 3.5]
    }
    df_test = pd.DataFrame(data)

    # MA 테스트
    try:
        df_test = compute_ma(df_test, period=3)
        print("MA_3:\n", df_test["MA_3"])
    except ValueError as e:
        print("MA 계산 예외:", e)

    # RSI 테스트
    try:
        df_test = compute_rsi(df_test, period=3)
        print("RSI_3:\n", df_test["RSI_3"])
    except ValueError as e:
        print("RSI 계산 예외:", e)

    # OBV 테스트
    try:
        df_test = compute_obv(df_test)
        print("OBV:\n", df_test["OBV"])
    except ValueError as e:
        print("OBV 계산 예외:", e)

    # rolling min/max 테스트
    try:
        df_test = compute_rolling_min(df_test, period=3, price_col="close", col_name="rolling_min_3")
        df_test = compute_rolling_max(df_test, period=3, price_col="close", col_name="rolling_max_3")
        df_test = compute_channel_width(df_test, high_col="rolling_max_3", low_col="rolling_min_3", col_name="chan_width_3")
        print("Min/Max/Channel:\n", df_test[["rolling_min_3", "rolling_max_3", "chan_width_3"]])
    except ValueError as e:
        print("Min/Max 계산 예외:", e)
