# gptbitcoin/config/indicator_config.py
# 기술적 거래 규칙의 파라미터를 정의하는 모듈

INDICATOR_COMBO_SIZES = [1]
INDICATOR_CONFIG = {
    "MA": {
        # 단기 이동평균선 기간 (p)
        "short_ma_periods": [1, 2, 6, 12, 18, 24, 30, 48, 96, 144, 168],
        # 장기 이동평균 기간
        "long_ma_periods": [2, 6, 12, 18, 24, 30, 48, 96, 144, 168, 192],
        # 고정 퍼센트 밴드 필터
        "band_filters": [0, 0.05, 0.1, 0.5, 1, 5],
        # 시간 지연 필터
        "time_delays": [0, 2, 3, 4, 5],
        # 보유 기간
        "holding_periods": [6, 12, 24, float('inf')]
    },
    "RSI": {
        # RSI lookback 기간
        "lookback_periods": [2, 6, 12, 14, 18, 24, 30, 48, 96, 144, 168, 192],
        # RSI 임계값
        "thresholds": [10, 15, 20, 25],
        # 시간 지연 필터
        "time_delays": [1, 2, 5],
        # 보유 기간
        "holding_periods": [1, 6, 12, 24, float('inf')]
    },
    "SR": {
        # SR lookback 기간
        "lookback_periods": [2, 6, 12, 18, 24, 30, 48, 96, 168],
        # 고정 퍼센트 밴드 필터
        "band_filters": [0.05, 0.1, 0.5, 1, 2.5, 5, 10],
        # 시간 지연 필터
        "time_delays": [0, 1, 2, 3, 4, 5],
        # 보유 기간
        "holding_periods": [1, 6, 12, 24, float('inf')]
    },
    "Filter": {
        # 필터의 lookback 기간
        "lookback_periods": [1, 2, 6, 12, 24],
        # 매수 신호 필터
        "buy_signal_filters": [0.05, 0.1, 0.5, 1, 5, 10, 20],
        # 매도 신호 필터
        "sell_signal_filters": [0.05, 0.1, 0.5, 1, 5, 10, 20],
        # 매수 신호 시간 지연 필터
        "buy_time_delay": [0, 1, 2, 3, 4, 5],
        # 매도 신호 시간 지연 필터
        "sell_time_delays": [0, 1, 2, 3, 4],
        # 공통 시간 지연 필터
        "uniform_time_delays": [0, 1, 2, 3, 4, 5],
        # 보유 기간
        "holding_periods": [6, 12, 18, 20, 24, float('inf')]
    },
    "CB": {
        # Lookback 기간
        "lookback_periods": [6, 12, 18, 24, 36, 72, 120, 168],
        # c% 트레이딩 채널
        "c_percent_channels": [0.5, 1, 5, 10, 15],
        # 고정 퍼센트 밴드 필터
        "band_filters": [0.05, 0.1, 0.5, 1, 5],
        # 시간 지연 필터
        "time_delays": [0, 1, 2],
        # 보유 기간
        "holding_periods": [1, 6, 12, 24, float('inf')]
    },
    "OBV": {
        # 단기 이동평균 기간
        "short_ma_periods": [2, 6, 12, 18, 24, 30, 48, 96, 144, 168],
        # 장기 이동평균 기간
        "long_ma_periods": [2, 6, 12, 18, 24, 30, 48, 96, 144, 168, 192],
        # 고정 퍼센트 밴드 필터
        "band_filters": [0, 0.01, 0.05],
        # 시간 지연 필터
        "time_delays": [0, 2, 3, 4, 5],
        # 보유 기간
        "holding_periods": [6, 12, float('inf')]
    }
}
