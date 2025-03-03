# gptbitcoin/utils/date_time.py

import datetime
import zoneinfo

def today():
    # 한국 시간대(KST = UTC+9)
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    """today 함수를 테스트."""
    print("현재 시각 테스트:", today())

if __name__ == "__main__":
    main()
