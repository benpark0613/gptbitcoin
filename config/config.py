# gptbitcoin/config/config.py
# 구글 스타일, 최소한의 한글 주석
# SQLite DB를 사용하기 위한 DB_PATH 추가

import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")

# 백테스트 기본 설정
SYMBOL = "BTCUSDT"
TIMEFRAMES = ["1d"]
START_CAPITAL = 1_000_000
INSAMPLE_RATIO = 0.7
ALLOW_SHORT = True
COMMISSION_RATE = 0.0004
SLIPPAGE_RATE = 0.0002

# 거래소 오픈 날짜
EXCHANGE_OPEN_DATE = "2019-09-08"
BOUNDARY_DATE = "2025-01-01"

# 테스트 기본 구간
START_DATE = "2019-01-01"
END_DATE = "2019-12-31"

INDICATOR_COMBO_SIZES = [1]

INDICATOR_CONFIG = {
    "MA": {
        "short_periods": [5, 10, 20],
        "long_periods":  [50, 100, 200],
        "band_filters": [0.0, 0.02],
    },
    "RSI": {
        "lengths": [14, 21, 30],
        "overbought_values": [70, 80],
        "oversold_values":   [30, 20],
    },
    "OBV": {
        "short_periods": [5, 10],
        "long_periods": [30, 50, 100],
    },
    "Filter": {
        "windows": [10, 20],
        "x_values": [0.05, 0.1],
        "y_values": [0.05, 0.1],
    },
    "Support_Resistance": {
        "windows": [10, 20],
        "band_pcts": [0.0, 0.01, 0.02],
    },
    "Channel_Breakout": {
        "windows": [14, 20],
        "c_values": [0.1, 0.2, 0.3],
    },
}

METRICS = [
    "StartCapital",
    "EndCapital",
    "Return",
    "CAGR",
    "Sharpe",
    "MDD",
    "Trades",
    "WinRate",
    "ProfitFactor",
    "AvgHoldingPeriod",
    "AvgPnlPerTrade",
]


# SQLite DB 경로
# gptbitcoin 루트에서 data 폴더 아래 "ohlcv.sqlite" 파일로 지정 (예시)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FOLDER = os.path.join(DATA_DIR, "db")

# SQLite 파일 위치
os.makedirs(DB_FOLDER, exist_ok=True)
DB_PATH = os.path.join(DB_FOLDER, "ohlcv.sqlite")

# 결과 및 로그 관련 폴더
RESULTS_DIR = "results"
LOGS_DIR = "logs"