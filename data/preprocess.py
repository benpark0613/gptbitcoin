# gptbitcoin/data/preprocess.py
"""
Collector 단계에서 이미 결측치가 제거된 OHLCV 데이터에 대해
중복·이상치가 발견되면 예외를 발생시키고 즉시 종료하는 전처리 모듈.
(보조지표 계산은 indicators/ 폴더에서 수행)
"""

import sys
import pandas as pd

def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    OHLCV 데이터에 대해 중복 여부와 간단한 이상치를 검사한다.
    중복이나 이상치가 하나라도 발견되면 예외를 발생시키고 프로그램을 종료한다.
    결측치(NaN)는 Collector 단계에서 허용되지 않으므로 여기서는 전제하지 않는다.

    Args:
        df (pd.DataFrame): 'open_time', 'open', 'high', 'low', 'close', 'volume' 칼럼이 포함된 DataFrame

    Returns:
        pd.DataFrame: 원본 그대로 반환 (단, 중복/이상이 발견되면 예외 발생)

    Raises:
        ValueError: 중복행 또는 이상치가 발견되면 발생
    """

    # 1) 중복 검사
    # (open_time, open, high, low, close, volume)가 완전히 같은 행이 존재하면 예외 발생
    duplicated_count = df.duplicated(subset=["open_time", "open", "high", "low", "close", "volume"]).sum()
    if duplicated_count > 0:
        raise ValueError(f"중복 행이 {duplicated_count}개 발견되었습니다. Collector 단계에서 중복이 없도록 확인하세요.")

    # 2) 이상치 검사
    # 0 이하인 price나 음수 volume 등을 이상치로 간주. 하나라도 발견 시 예외 발생
    anomaly_condition = (
        (df["open"] <= 0) |
        (df["high"] <= 0) |
        (df["low"] <= 0) |
        (df["close"] <= 0) |
        (df["volume"] < 0)
    )
    anomalies_count = anomaly_condition.sum()
    if anomalies_count > 0:
        raise ValueError(f"이상치(0 또는 음수) 데이터가 {anomalies_count}개 발견되었습니다. Collector 단계에서 데이터 정합성 확인 필요.")

    # 문제가 없으면 그대로 리턴
    return df


def merge_old_recent(df_old: pd.DataFrame, df_recent: pd.DataFrame) -> pd.DataFrame:
    """
    old_data, recent_data를 시계열 순으로 합치는 함수.
    두 DataFrame 모두 NaN이 없다는 전제 하에 병합한다.
    중복이나 이상치 검사는 clean_ohlcv에서 이미 하거나 Collector 단계에서 처리됨.

    Args:
        df_old (pd.DataFrame): 과거 구간 데이터
        df_recent (pd.DataFrame): 최신 구간 데이터

    Returns:
        pd.DataFrame: 시간순으로 병합된 DataFrame
    """
    merged = pd.concat([df_old, df_recent], ignore_index=True)
    merged.sort_values("open_time", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    return merged


if __name__ == "__main__":
    """
    테스트 코드: 샘플 OHLCV 데이터를 만들어 전처리 로직을 검증한다.
    중복/이상이 있으면 예외가 발생하도록 의도적 데이터 예시를 넣었다.
    """
    sample_data = {
        # open_time=3인 행이 중복됨
        "open_time": [1, 2, 3, 3, 4],
        "open": [10000, 10050, 10100, 10100, -1],   # 마지막 행 open=-1 (이상치 유발)
        "high": [10100, 10100, 10200, 10200, 10300],
        "low": [9900, 10000, 10050, 10050, 10100],
        "close": [10050, 10080, 10150, 10150, 10250],
        "volume": [1.2, 2.5, 3.7, 3.7, 4.0],
    }
    df_test = pd.DataFrame(sample_data)

    try:
        df_cleaned = clean_ohlcv(df_test)
        print("중복·이상치 검사 통과:", df_cleaned)
    except ValueError as e:
        print("예외 발생:", e)

    # 예외가 발생하므로, 이 아래 코드는 통과하지 않을 수 있음
    df_old = df_test[df_test["open_time"] < 3]
    df_recent = df_test[df_test["open_time"] >= 3]
    df_merged = merge_old_recent(df_old, df_recent)
    print("\nMerged DataFrame:")
    print(df_merged)
