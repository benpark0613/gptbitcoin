# gptbitcoin/config/config.py

import os
from dotenv import load_dotenv

from utils.date_time import today

# .env 로드
load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY", "")   # 바이낸스 API 키
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "") # 바이낸스 시크릿 키

# 거래소 및 레버리지 설정
MARGIN_TYPE = "ISOLATED"  # 마진 유형(예: ISOLATED)
LEVERAGE = 1              # 레버리지 배수

# 백테스트 기본 설정
ALLOW_SHORT = True        # 공매도(Short) 허용 여부
COMMISSION_RATE = 0.0004  # 백테스트 시 커미션 비율
SLIPPAGE_RATE = 0.0002    # 백테스트 시 슬리피지 비율
START_CAPITAL = 1_000_000 # 백테스트 시작 자본

# 심볼, 타임프레임
SYMBOL = "BTCUSDT"        # 기본 심볼
# TIMEFRAMES = ["1d", "4h", "1h", "15m"]  # 사용할 타임프레임 목록
TIMEFRAMES = ["1d"]  # 사용할 타임프레임 목록

# 바이낸스 비트코인 선물 오픈일 (API 요청 시 구간 참조)
EXCHANGE_OPEN_DATE = "2019-09-08 00:00:00"

# DB에서 old_data / recent_data 를 나누는 기준 시점
DB_BOUNDARY_DATE = "2025-01-01 00:00:00"

# 백테스트 전체 기간
START_DATE = "2023-01-01 00:00:00"  # 백테스트 시작
END_DATE = today()                  # 백테스트 종료

# IS/OOS 구분 시점 (테스트 전체 기간 중 이 값을 기준으로 IS/OOS를 나눔)
IS_OOS_BOUNDARY_DATE = "2024-12-31 00:00:00"

# 로그 레벨
LOG_LEVEL = "DEBUG"  # "INFO", "WARNING" 등으로 조정 가능

# 지표 관련
INDICATOR_COMBO_SIZES = [1, 2]  # 한 번에 사용할 보조지표 개수
# INDICATOR_COMBO_SIZES 최대 3인 경우 사용할 보조지표 설정값, 삭제 금지
# INDICATOR_CONFIG = {
#     "MA": {
#         "short_periods": [5, 10, 20],
#         "long_periods": [50, 100, 200],
#         "band_filters": [0.0, 0.02],
#     },
#     "RSI": {
#         "lengths": [14, 21, 30],
#         "overbought_values": [70, 80],
#         "oversold_values": [30, 20],
#     },
#     "OBV": {
#         "short_periods": [5, 10],
#         "long_periods": [30, 50, 100],
#     },
#     "Filter": {
#         "windows": [10, 20],
#         "x_values": [0.05, 0.1],
#         "y_values": [0.05, 0.1],
#     },
#     "Support_Resistance": {
#         "windows": [10, 20],
#         "band_pcts": [0.0, 0.01, 0.02],
#     },
#     "Channel_Breakout": {
#         "windows": [14, 20],
#         "c_values": [0.1, 0.2, 0.3],
#     },
# }

# INDICATOR_COMBO_SIZES 최대 2인 경우 사용할 보조지표 설정값, 삭제 금지
INDICATOR_CONFIG = {
    "MA": {
        # 주로 쓰이는 단기MA 4종, 장기MA 4종 + 필터 3종
        "short_periods": [5, 10, 20, 30],
        "long_periods": [50, 100, 200, 300],
        "band_filters": [0.0, 0.01, 0.02],
    },
    "RSI": {
        # 길이는 14,21,30,50 등 자주 사용
        # 과매수/과매도 범위도 3개씩
        "lengths": [14, 21, 30, 50],
        "overbought_values": [70, 80, 85],
        "oversold_values": [30, 20, 15],
    },
    "OBV": {
        # OBV 계산 시 단/장기 윈도우 예시
        "short_periods": [5, 10, 20],
        "long_periods": [30, 50, 100, 200],
    },
    "Filter": {
        # 필터룰 window는 10,20,30,50 / x,y_pct는 보통 5%,10%,15%
        "windows": [10, 20, 30, 50],
        "x_values": [0.05, 0.1, 0.15],
        "y_values": [0.05, 0.1, 0.15],
    },
    "Support_Resistance": {
        # 윈도우 10,20,30,50,100 + 밴드 0,1%,2%,3%
        "windows": [10, 20, 30, 50, 100],
        "band_pcts": [0.0, 0.01, 0.02, 0.03],
    },
    "Channel_Breakout": {
        # 윈도우 14,20,30,50,60 + c_values 0.1~0.6
        "windows": [14, 20, 30, 50, 60],
        "c_values": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
    },
}

# DB 경로
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 프로젝트 최상위 디렉토리
DATA_DIR = os.path.join(BASE_DIR, "data")                               # 데이터 폴더 경로
DB_FOLDER = os.path.join(DATA_DIR, "db")                                # DB 폴더
os.makedirs(DB_FOLDER, exist_ok=True)
DB_PATH = os.path.join(DB_FOLDER, "ohlcv.sqlite")                       # sqlite DB 파일 경로

# 결과/로그 폴더
RESULTS_DIR = "results"   # 백테스트 결과 폴더
LOGS_DIR = "logs"         # 로그 파일 폴더
