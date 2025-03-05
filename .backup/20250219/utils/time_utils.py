# gptbitcoin/utils/time_utils.py

"""
time_utils.py

시간 관련 유틸 함수 모음.
"""

def infer_timeframe_hours(interval: str) -> float:
    """
    문자열로 된 타임프레임(interval)을
    부동소수(float) 시간(hour) 단위로 변환.

    예) "4h"  -> 4.0
        "1d"  -> 24.0
        "15m" -> 0.25
        그 외 -> 1.0 (기본값)

    Parameters
    ----------
    interval : str
        예: "4h", "1h", "1d", "15m" 등

    Returns
    -------
    float
        변환된 시간(시 단위). 예: 15m = 0.25, 1d = 24.0
    """
    if interval.endswith("h"):
        return float(interval.replace("h",""))
    elif interval.endswith("d"):
        return 24.0 * float(interval.replace("d",""))
    elif interval.endswith("m"):
        mins = float(interval.replace("m",""))
        return mins / 60.0
    else:
        return 1.0
