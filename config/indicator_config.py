# gptbitcoin/config/indicator_config.py
# 기술적 지표 파라미터 설정 (band_filters, time_delays, holding_periods는 각 2~3개 정도)
# 주로 테스트할 시간 프레임: 1d, 4h, 1h, 15m (별도 변수 선언 없음)
# 트레이더들이 실제로 많이 사용하는 설정값 위주 설정

INDICATOR_COMBO_SIZES = [1, 2]
INDICATOR_CONFIG = {
    "MA": {
        "short_ma_periods": [5, 9, 20, 30],
        "long_ma_periods": [50, 100, 200],       # 100일 추가(중장기 추세 확인용)
        "band_filters": [0, 0.03, 0.05],         # 3% 추가
        "time_delays": [0, 2],                   # 2일 딜레이 추가
        "holding_periods": [float('inf')]
    },

    "RSI": {
        "lookback_periods": [7, 14, 21],
        "thresholds": [20, 30],                  # 10 삭제 (현실적이지 않음)
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "OBV": {
        "short_ma_periods": [5, 10, 20],
        "long_ma_periods": [30, 50],
        "band_filters": [0, 0.03, 0.05],         # 3% 추가
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "MACD": {
        "fast_periods": [6, 9, 12],
        "slow_periods": [26],
        "signal_periods": [9],
        "band_filters": [0],
        "time_delays": [0],
        "holding_periods": [float('inf')]
    },

    "DMI_ADX": {
        "dmi_periods": [14, 20, 30],
        "adx_thresholds": [20, 25],
        "band_filters": [0],
        "time_delays": [0, 3],
        "holding_periods": [float('inf')]
    },

    "BOLL": {
        "lookback_periods": [20],
        "stddev_multipliers": [2, 2.5, 3],
        "band_filters": [0],
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "ICHIMOKU": {
        "tenkan_period": [9],
        "kijun_period": [26],
        "senkou_span_b_period": [52],
        "band_filters": [0],
        "time_delays": [0],
        "holding_periods": [float('inf')]
    },

    "PSAR": {
        "lookback_periods": [5],
        "acceleration_step": [0.01, 0.02],
        "acceleration_max": [0.2],
        "band_filters": [0, 0.03],               # 3% 추가
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "SUPERTREND": {
        "atr_period": [10],
        "multiplier": [2, 3, 4],                 # multiplier 4 추가
        "band_filters": [0, 0.03],
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "FIBO": {
        "levels": [
            [0.382, 0.618],
            [0.236, 0.382, 0.5, 0.618],
            [0.382, 0.618, 1.272]
        ],
        "band_filters": [0, 0.03],
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "SR": {
        "lookback_periods": [10, 20, 60, 120],   # 장기(120) 추가
        "band_filters": [0, 0.03],
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "CB": {
        "lookback_periods": [15, 20, 55, 200],   # 단기(15일) 추가
        "c_percent_channels": [0, 0.03],
        "band_filters": [0],
        "time_delays": [0, 2],
        "holding_periods": [float('inf')]
    },

    "Filter": {
        "lookback_periods": [5, 10, 20],
        "uniform_filters": [0.05, 0.1, 0.5],
        "uniform_time_delays": [0, 2],
        "holding_periods": [float('inf')]
    }
}
