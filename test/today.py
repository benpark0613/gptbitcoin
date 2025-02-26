# gptbitcoin/test/test_date.py
# 구글 스타일, 최소한의 한글 주석

from datetime import datetime

def main():
    # 현재 날짜를 YYYY-MM-DD 형식으로 a에 저장 후 출력
    now = datetime.now()
    a = now.strftime("%Y-%m-%d")
    print(a)

if __name__ == "__main__":
    main()

