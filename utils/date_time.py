# gptbitcoin/utils/date_time.py
# 날짜/시간 관련 유틸 모듈
# 기존 today, ms_to_kst_str 함수 + timeframe_to_timedelta 함수 추가

import datetime
from datetime import timedelta

def timeframe_to_timedelta(tf_str: str) -> timedelta:
    """
    타임프레임 문자열을 timedelta로 변환.
    예) "1d" -> 1일, "4h" -> 4시간, "15m" -> 15분

    Args:
        tf_str (str): "1d", "4h", "15m" 등

    Returns:
        timedelta: 변환된 시간 간격
    """
    tf_lower = tf_str.lower().strip()
    if tf_lower.endswith("d"):
        day_str = tf_lower[:-1]
        day_val = int(day_str) if day_str.isdigit() else 1
        return timedelta(days=day_val)
    elif tf_lower.endswith("h"):
        hour_str = tf_lower[:-1]
        hour_val = int(hour_str) if hour_str.isdigit() else 1
        return timedelta(hours=hour_val)
    elif tf_lower.endswith("m"):
        minute_str = tf_lower[:-1]
        minute_val = int(minute_str) if minute_str.isdigit() else 1
        return timedelta(minutes=minute_val)
    # 그 외 예외 상황: 기본 1일로 처리
    return timedelta(days=1)


def today() -> str:
    """
    현재 시각을 "YYYY-MM-DD HH:MM:SS" 형식으로 반환.

    Returns:
        str: 현재 로컬 시각 문자열
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ms_to_kst_str(ms_val: int) -> str:
    """
    UTC 기반 밀리초(ms_val)를 KST(UTC+9) 시각 문자열로 변환.

    Args:
        ms_val (int): UTC 기준 밀리초(에포크) 값

    Returns:
        str: "YYYY-MM-DD HH:MM:SS" 형식의 KST 시각
    """
    dt_utc = datetime.datetime.utcfromtimestamp(ms_val / 1000.0)
    dt_kst = dt_utc + timedelta(hours=9)
    return dt_kst.strftime("%Y-%m-%d %H:%M:%S")


def main():
    """
    간단 테스트: today 함수와 timeframe_to_timedelta 함수 시연.
    """
    print("[test] 현재 시각:", today())
    print("[test] timeframe_to_timedelta('1d'):", timeframe_to_timedelta("1d"))
    print("[test] timeframe_to_timedelta('4h'):", timeframe_to_timedelta("4h"))
    print("[test] timeframe_to_timedelta('15m'):", timeframe_to_timedelta("15m"))
    print("[test] timeframe_to_timedelta('abc'):", timeframe_to_timedelta("abc"))

if __name__ == "__main__":
    main()
