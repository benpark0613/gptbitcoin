# gptbitcoin/test_ccxt.py
# CCXT 라이브러리를 사용해 Binance USDⓂ 선물에서
# 2023-08-15 00:00 UTC ~ 2023-08-17 00:00 UTC 사이의
# 1일봉(1D) 데이터를 콘솔에 찍는다.
# 정확히 해당 기간에 해당하는 봉만 필터링하여 출력한다.

import datetime
import ccxt
from config.config import BINANCE_API_KEY, BINANCE_SECRET_KEY

def main():
    binance = ccxt.binance({
        'apiKey': BINANCE_API_KEY,
        'secret': BINANCE_SECRET_KEY,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future',  # USDⓂ 선물
        },
        'adjustForTimeDifference': True  # 시간차 자동 보정
    })

    symbol = "BTC/USDT"
    timeframe = "1d"

    # 원하는 날짜 범위
    start_dt = datetime.datetime(2023, 8, 15, 0, 0, 0)
    end_dt   = datetime.datetime(2023, 8, 17, 0, 0, 0)
    start_ts = int(start_dt.timestamp() * 1000)  # CCXT since 파라미터용 (ms)
    end_ts   = int(end_dt.timestamp() * 1000)

    # 일단 limit을 여유 있게 잡고, 이후 파이썬에서 필터링
    klines = binance.fetch_ohlcv(
        symbol=symbol,
        timeframe=timeframe,
        since=start_ts,
        limit=10  # 대략 몇 개만 가져온 뒤 필터링
    )

    print(f"[INFO] Fetched {len(klines)} candles before filtering.\n")

    filtered = []
    for c in klines:
        # c = [timestamp(ms), open, high, low, close, volume]
        tstamp_ms, o, h, l, close_, vol = c
        if start_ts <= tstamp_ms < end_ts:
            filtered.append(c)

    print(f"[INFO] Filtered {len(filtered)} candle(s) for {start_dt} ~ {end_dt}")
    for c in filtered:
        tstamp_ms, o, h, l, close_, vol = c
        dt_utc = datetime.datetime.utcfromtimestamp(tstamp_ms / 1000)
        print(
            f"Time(UTC)={dt_utc}, "
            f"Open={o}, High={h}, Low={l}, Close={close_}, Volume={vol}"
        )

if __name__ == "__main__":
    main()
