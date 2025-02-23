# gptbitcoin/config/config.py
# 구글 스타일, 최소한의 한글 주석

import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_ACCESS_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")

# 백테스트 기본 설정
SYMBOL = "BTCUSDT"
TIMEFRAMES = ["1d"]
START_CAPITAL = 100000
WARMUP_BARS = 250
INSAMPLE_RATIO = 0.7
ALLOW_SHORT = True
COMMISSION_RATE = 0.0004
SLIPPAGE_RATE = 0.0002

START_DATE = "2024-01-01"
END_DATE = "2024-12-31"

INDICATOR_COMBO_SIZES = [1,2,3]

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
    "OBV": {
        "short_periods": [5, 10],
        "long_periods":  [30, 50, 100],
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

# 폴더 경로 (루트 기준 상대경로로 지정)
DATA_DIR = "results"
ORIGIN_OHLCV_DIR = "origin_ohlcv"
RESULTS_DIR = "results"
LOGS_DIR = "logs"
