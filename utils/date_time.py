# gptbitcoin/utils/date_time.py

import datetime
from datetime import timedelta

def today():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ms_to_kst_str(ms_val: int) -> str:
    """
    UTC 밀리초(ms_val)를 KST(UTC+9) 시각 문자열("YYYY-MM-DD HH:MM:SS")로 변환
    """
    dt_utc = datetime.datetime.utcfromtimestamp(ms_val / 1000.0)
    dt_kst = dt_utc + timedelta(hours=9)
    return dt_kst.strftime("%Y-%m-%d %H:%M:%S")

def main():
    """today 함수를 테스트."""
    print("현재 시각 테스트:", today())

if __name__ == "__main__":
    main()

