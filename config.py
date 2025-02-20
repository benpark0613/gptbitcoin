# binance_btc_futures_backtest/config.py
# 전역 설정 및 보조지표 파라미터를 정의한다.

"""글로벌 설정 및 보조지표 파라미터를 정의한다."""

# 데이터 수집 설정
DEFAULT_TIMEFRAMES = ["1D", "4H", "1H", "15M"]
WARMUP_BARS = 200

# 백테스트 구간 설정
INSAMPLE_RATIO = 0.7
OUTSAMPLE_RATIO = 0.3

# 초기 자금
INITIAL_CAPITAL = 100_000

# 매매 비용
COMMISSION_RATE = 0.0004  # 0.04%
SLIPPAGE_RATE = 0.0002    # 0.02%

# 숏 허용 여부
ALLOW_SHORT = True

# 보조지표 설정값
TREND_INDICATORS = {
    "MA_CROSS": [
        (5, 20),
        (10, 50),
        (20, 100)
    ],
    "MACD": [
        (9, 18, 7),
        (12, 26, 9),
        (7, 14, 5)
    ]
}

MOMENTUM_INDICATORS = {
    "RSI": [
        (7, (80, 20)),
        (14, (70, 30)),
        (14, (80, 20)),
        (9, (80, 20))
    ],
    "STOCHASTIC": [
        (9, 3, (80, 20)),
        (14, 3, (80, 20)),
        (14, 3, (75, 25)),
        (5, 3, (80, 20))
    ]
}

VOLATILITY_INDICATORS = {
    "BOLLINGER_BANDS": [
        (14, 2.0),
        (20, 2.0),
        (20, 2.5)
    ],
    "ATR": [
        (14, 2),
        (20, 2)
    ]
}

VOLUME_INDICATORS = {
    "OBV": [
        (5, 20),
        (10, 50)
    ],
    "MFI": [
        (14, (80, 20)),
        (20, (80, 20))
    ]
}

# 매매 임계값 설정 (보조지표 2개, 3개, 4개 조합별)
THRESHOLD_2_INDICATORS = {"LONG": 1, "SHORT": -1}
THRESHOLD_3_INDICATORS = {"LONG": 1, "SHORT": -1}
THRESHOLD_4_INDICATORS = {"LONG": 1, "SHORT": -1}
