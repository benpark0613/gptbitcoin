# gptbitcoin/config/config.py
# config.py
"""
백테스트 및 전체 시스템 환경설정을 관리하는 모듈.
(보조지표 관련 설정은 indicator_config.py로 분리했으므로 여기에는 포함하지 않는다.)
"""

import os
from dotenv import load_dotenv

from utils.date_time import today

# 로그 레벨
LOG_LEVEL = "DEBUG"  # 필요에 따라 "INFO", "WARNING" 등 변경 가능

# .env 로드
load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")

# 거래소 및 레버리지 설정
MARGIN_TYPE = "ISOLATED"  # 마진 유형 (예: ISOLATED)
LEVERAGE = 1              # 레버리지 배수

# 백테스트 기본 설정
ALLOW_SHORT = True        # 공매도(Short) 허용 여부
COMMISSION_RATE = 0.0004  # 커미션 비율
SLIPPAGE_RATE = 0.0002    # 슬리피지 비율
START_CAPITAL = 100_000   # 백테스트 시작 자본

# 심볼, 타임프레임 관련
SYMBOL = "BTCUSDT"        # 기본 심볼
TIMEFRAMES = ["1d", "4h", "1h", "15m"]  # 사용할 타임프레임 목록
# TIMEFRAMES = ["1d"]       # 사용할 타임프레임 목록

# 바이낸스 비트코인 선물 오픈일 (API 요청 시 구간 참조, UTC 기준)
EXCHANGE_OPEN_DATE = "2019-09-08 00:00:00"

# DB_BOUNDARY_DATE:
#   DB에 저장할 때 old_data/recent_data를 분리하는 기준 시점(UTC).
#   old_data: open_time < DB_BOUNDARY_DATE
#   recent_data: open_time >= DB_BOUNDARY_DATE
DB_BOUNDARY_DATE = "2025-02-01 00:00:00"

# USE_IS_OOS:
#   백테스트 시점에서 IS/OOS 분할을 할지 말지를 결정.
#   True이면 인샘플(IS) & 아웃샘플(OOS)로 나누어 run_is → run_oos 수행
#   False이면 단일 구간 백테스트(run_nosplit)만 수행
USE_IS_OOS = True

# IS_OOS_BOUNDARY_DATE:
#   IS/OOS 구간을 나누는 기준 시점(UTC)
#   open_time < IS_OOS_BOUNDARY_DATE => IS 구간
#   open_time >= IS_OOS_BOUNDARY_DATE => OOS 구간
IS_OOS_BOUNDARY_DATE = "2025-01-01 00:00:00"

# 백테스트 전체 기간 (UTC)
START_DATE = "2024-03-01 00:00:00"  # 시작
# END_DATE = "2021-05-20 00:00:00"
END_DATE = today()                  # 종료 (오늘 날짜를 기본값)

# 결과/로그 폴더
RESULTS_DIR = "results"   # 백테스트 결과가 저장될 폴더
LOGS_DIR = "logs"         # 로그 파일 폴더

# DB 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 프로젝트 최상위 디렉토리
DATA_DIR = os.path.join(BASE_DIR, "data")                               # 데이터 폴더
DB_FOLDER = os.path.join(DATA_DIR, "db")                                # DB 폴더
os.makedirs(DB_FOLDER, exist_ok=True)
DB_PATH = os.path.join(DB_FOLDER, "ohlcv.sqlite")                       # sqlite DB 파일 경로
