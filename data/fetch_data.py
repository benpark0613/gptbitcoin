# gptbitcoin/data/fetch_data.py
# Binance 선물 API 연동, OHLCV 수집 및 DB 저장 (Collector 역할)
# 모든 시간 처리는 기본적으로 UTC 기준으로 진행.
# DB 저장 시 timestamp_kst 컬럼에는 KST로 변환된 문자열만 기록.

import datetime
import sys
import time
import zoneinfo
import logging

import pandas as pd
import requests

# 타임존 객체
_UTC = datetime.timezone.utc
_TZ_KST = zoneinfo.ZoneInfo("Asia/Seoul")

# Binance 선물 API 엔드포인트
BINANCE_FAPI_URL = "https://fapi.binance.com/fapi/v1/klines"

# 로거 설정
logger = logging.getLogger(__name__)


def _parse_utc_datetime(dt_str: str) -> datetime.datetime:
    """UTC 기준의 날짜/시간 문자열(예: 'YYYY-MM-DD HH:MM:SS')을 UTC datetime 객체로 변환한다.

    Args:
        dt_str (str): UTC 기준 시간 문자열

    Returns:
        datetime.datetime: tzinfo가 UTC로 지정된 datetime 객체
    """
    return datetime.datetime.fromisoformat(dt_str).replace(tzinfo=_UTC)


def _convert_utc_to_kst_str(utc_dt: datetime.datetime) -> str:
    """UTC datetime 객체를 KST(UTC+9) 시간 문자열로 변환한다.

    Args:
        utc_dt (datetime.datetime): tzinfo가 UTC로 설정된 datetime 객체

    Returns:
        str: 'YYYY-MM-DD HH:MM:SS' 형식의 KST 문자열
    """
    dt_kst = utc_dt.astimezone(_TZ_KST)
    return dt_kst.strftime("%Y-%m-%d %H:%M:%S")


def _fetch_binance_futures_ohlcv(
    symbol: str,
    timeframe: str,
    start_utc_dt: datetime.datetime,
    end_utc_dt: datetime.datetime,
    limit: int = 1500,
) -> pd.DataFrame:
    """Binance 선물 API를 사용하여 OHLCV 데이터를 수집한다.

    Args:
        symbol (str): 거래 심볼 (예: 'BTCUSDT')
        timeframe (str): 바이낸스 선물 간격 (예: '1d', '1h' 등)
        start_utc_dt (datetime.datetime): UTC 기준 시작 시각
        end_utc_dt (datetime.datetime): UTC 기준 종료 시각
        limit (int): 한 번 API 호출로 가져올 수 있는 최대 캔들 수 (기본 1500)

    Returns:
        pd.DataFrame:
            OHLCV 정보를 담은 DataFrame
            컬럼: ['symbol', 'timeframe', 'open_time_ms', 'timestamp_kst',
                   'open', 'high', 'low', 'close', 'volume']
    """
    all_rows = []
    current_utc_dt = start_utc_dt

    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "limit": limit,
            "startTime": int(current_utc_dt.timestamp() * 1000),
            "endTime": int(end_utc_dt.timestamp() * 1000),
        }
        response = requests.get(BINANCE_FAPI_URL, params=params, timeout=10)

        if response.status_code != 200:
            logger.error(f"Binance API 오류: {response.text}")
            raise Exception(f"Binance API Error: {response.text}")

        data = response.json()
        if not data:
            # 더 이상 가져올 데이터가 없으면 종료
            break

        for item in data:
            open_time_ms = int(item[0])
            open_price = float(item[1])
            high_price = float(item[2])
            low_price = float(item[3])
            close_price = float(item[4])
            volume = float(item[5])

            # open_time_ms(UTC ms) -> UTC datetime -> KST 문자열
            utc_dt = datetime.datetime.utcfromtimestamp(open_time_ms / 1000).replace(tzinfo=_UTC)
            kst_time_str = _convert_utc_to_kst_str(utc_dt)

            row = [
                symbol,
                timeframe,
                open_time_ms,
                kst_time_str,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
            ]
            all_rows.append(row)

        # 마지막 봉의 open_time_ms로부터 다음 호출 시작 시점 계산
        last_open_time_ms = int(data[-1][0])
        last_utc_dt = datetime.datetime.utcfromtimestamp(last_open_time_ms / 1000).replace(tzinfo=_UTC)

        # 더 이상 진행할 필요 없거나 limit 미만 캔들이면 반복 종료
        if last_utc_dt >= end_utc_dt or len(data) < limit:
            break

        # 다음 요청의 시작 시간: 마지막 봉 + 1ms
        current_utc_dt = last_utc_dt + datetime.timedelta(milliseconds=1)
        time.sleep(0.2)  # Rate Limit 보호

    columns = [
        "symbol", "timeframe", "open_time_ms", "timestamp_kst",
        "open", "high", "low", "close", "volume"
    ]
    return pd.DataFrame(all_rows, columns=columns)


def update_data_db(
    symbol: str,
    timeframes: list[str],
    start_utc_str: str,
    end_utc_str: str,
    dropna_indicators: bool = False,
) -> None:
    """지정된 (symbol, timeframes)와 UTC 구간(start_utc_str ~ end_utc_str)에 대해
    Binance 선물 OHLCV 데이터를 수집하고 DB에 저장한다.

    처리 순서:
      1) 입력된 시간 문자열을 UTC datetime 객체로 변환
      2) DB에 해당 구간 데이터 삭제
      3) Binance API를 통해 OHLCV 수집
      4) 결측 여부 검사 후 DB에 삽입

    Args:
        symbol (str): 예) 'BTCUSDT'
        timeframes (List[str]): 예) ['1d', '4h', ...]
        start_utc_str (str): UTC 기준 시작 시간 (예: '2021-01-01 00:00:00')
        end_utc_str (str): UTC 기준 종료 시간
        dropna_indicators (bool): True 시 DataFrame 내 결측 행 제거
    """
    start_utc_dt = _parse_utc_datetime(start_utc_str)
    end_utc_dt = _parse_utc_datetime(end_utc_str)
    if end_utc_dt <= start_utc_dt:
        logger.error("종료 시점이 시작 시점보다 같거나 작습니다.")
        sys.exit(1)

    from db_manager import delete_ohlcv_data, insert_ohlcv_batch

    for tf in timeframes:
        logger.info(f"[{symbol}-{tf}] {start_utc_str} ~ {end_utc_str} 구간 데이터 수집 시작...")

        # UTC 문자열 그대로 delete_ohlcv_data에 전달
        delete_ohlcv_data(symbol, tf, start_utc_str, end_utc_str)

        df = _fetch_binance_futures_ohlcv(symbol, tf, start_utc_dt, end_utc_dt)
        if df.empty:
            logger.warning(f"[{symbol}-{tf}] 수집된 데이터가 없습니다.")
            continue

        # 결측치 검사
        if df[["open", "high", "low", "close", "volume"]].isnull().any().any():
            logger.error("OHLCV 데이터에 결측치 발견. 수집을 중단합니다.")
            sys.exit(1)

        if dropna_indicators:
            df.dropna(inplace=True)

        insert_ohlcv_batch(df)
        logger.info(f"[{symbol}-{tf}] 구간 데이터 DB 저장 완료.")


def main():
    """단독 실행 시 사용되는 예시 함수."""
    logging.basicConfig(level=logging.INFO)  # 간단한 로거 설정
    symbol = "BTCUSDT"
    timeframes = ["1d", "4h"]
    start_utc_str = "2019-09-08 00:00:00"
    end_utc_str = "2025-03-04 00:00:00"
    update_data_db(symbol, timeframes, start_utc_str, end_utc_str, dropna_indicators=False)


if __name__ == "__main__":
    main()
