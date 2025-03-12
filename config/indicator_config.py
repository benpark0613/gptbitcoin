# gptbitcoin/config/indicator_config.py
# 기술적 지표 파라미터 설정 (band_filters, time_delays, holding_periods는 각 2~3개 정도)
# 주로 테스트할 시간 프레임: 1d, 4h, 1h, 15m (별도 변수 선언 없음)
# 트레이더들이 실제로 많이 사용하는 설정값 위주로 확장하여
# 하나의 시간 프레임당 보조지표 조합이 대략 10,000~20,000개 정도 되도록 구성

# 콤보 사이즈 1일 경우, 수정 시 삭제 금지
# INDICATOR_COMBO_SIZES = [1]  # 단일 지표 단위로 조합
# INDICATOR_CONFIG = {
#     # ---------------------------------------------------
#     # 1) 이동평균(MA)
#     # ---------------------------------------------------
#     "MA": {
#         # 단기 이동평균: 범위를 늘려 다양한 주기를 커버
#         "short_ma_periods": [1,2,6,12,18,24,30,48,96,144,168],
#         # 장기 이동평균
#         "long_ma_periods": [2,6,12,18,24,30,48,96,144,168,192],
#         # 밴드 필터 (노이즈 제어용)
#         "band_filters": [0.0, 0.05, 0.1],
#         # 시간 지연 필터
#         "time_delays": [0, 3, 5],
#         # 보유 기간
#         "holding_periods": [12, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 2) RSI
#     # ---------------------------------------------------
#     "RSI": {
#         "lookback_periods": [2,6,12, 14,18,24,30,48,96,144,168,192],
#         "thresholds": [10,15,20,25],
#         "time_delays": [1,2,5],
#         "holding_periods": [6, 12, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 3) 필터 룰 (Filter)
#     # ---------------------------------------------------
#     "Filter": {
#         "lookback_periods": [1,2,6,12,24],
#         "uniform_filters": [0.05,0.1,0.5,1,5,10,20],
#         "uniform_time_delays": [0, 3, 5],
#         "holding_periods": [6, 18, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 4) 채널 돌파 (CB)
#     # ---------------------------------------------------
#     "CB": {
#         "lookback_periods": [6,12,18,24,36,72,120,168],
#         "c_percent_channels": [0.5,1,5,10, 15],
#         "band_filters": [0.05,0.1,0.5,1,5],
#         "time_delays": [0,1,2],
#         "holding_periods": [6, 18, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 5) OBV (On-Balance Volume)
#     # ---------------------------------------------------
#     "OBV": {
#         "short_ma_periods": [2,6,12,18,24,30,48,96,144,168],
#         "long_ma_periods": [2,6,12,18,24,30,48,96,144,168,192],
#         "band_filters": [0,0.01,0.05],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 6) MACD
#     # ---------------------------------------------------
#     "MACD": {
#         "fast_periods": [8, 10, 12, 15, 18, 21, 24, 27],
#         "slow_periods": [26, 30, 35, 40, 45, 50],
#         "signal_periods": [9, 10, 12, 15],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 7) DMI & ADX
#     # ---------------------------------------------------
#     "DMI_ADX": {
#         "dmi_periods": [14, 20, 25, 30, 35, 40, 45, 50],
#         "adx_thresholds": [20, 25, 30, 35, 40],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 8) 볼린저 밴드 (BOLL)
#     # ---------------------------------------------------
#     "BOLL": {
#         "lookback_periods": [14, 20, 26, 30, 35, 40, 50, 60],
#         "stddev_multipliers": [1.5, 2, 2.5, 3],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 9) 이치모쿠 클라우드 (ICHIMOKU)
#     # ---------------------------------------------------
#     "ICHIMOKU": {
#         "tenkan_period": [7, 9, 13, 15, 20, 25],
#         "kijun_period": [21, 26, 30, 35, 40, 45],
#         "senkou_span_b_period": [52, 60, 65, 70, 80, 90],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 10) 파라볼릭 SAR (PSAR)
#     # ---------------------------------------------------
#     "PSAR": {
#         "acceleration_step": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08],
#         "acceleration_max": [0.2, 0.3, 0.4, 0.5],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 11) 슈퍼트렌드 (SUPERTREND)
#     # ---------------------------------------------------
#     "SUPERTREND": {
#         "atr_period": [7, 10, 14, 20, 28, 35, 40, 50],
#         "multiplier": [2, 3, 4, 5, 6],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 12) 피보나치 되돌림 (FIBO)
#     # ---------------------------------------------------
#     "FIBO": {
#         "levels": [
#             [0.236, 0.382, 0.5, 0.618],
#             [0.382, 0.5, 0.618, 0.786],
#             [0.5, 0.618, 0.786, 0.886],
#             [0.236, 0.382, 0.618, 0.786, 1.618],
#             [0.382, 0.618, 1.0, 1.618],
#             [0.618, 1.618, 2.618, 3.618],
#             [0.236, 0.382, 0.786, 1.618, 2.618],
#             [0.5, 0.786, 1.618, 2.618, 3.618]
#         ],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, float('inf')]
#     },
#
#     # ---------------------------------------------------
#     # 13) Support and Resistance
#     # ---------------------------------------------------
#     "SR": {
#         "lookback_periods": [2,6,12,18,24,30,48,96,168],
#         "band_filters": [0.05,0.5,1,5],
#         "time_delays": [0, 3, 5],
#         "holding_periods": [6, 12, float('inf')]
#     }
# }

# 콤보 사이즈 1,2 일 경우, 수정 시 삭제 금지
# gptbitcoin/config/indicator_config.py
# combo_sizes = [1, 2]
INDICATOR_COMBO_SIZES = [1, 2]

INDICATOR_CONFIG = {
    # 1) 이동평균(MA)
    #   short: 4개, long: 2개, band_filters: 2개, time_delays: 1개, holding: 1개
    #   4 * 2 = 8 → band 2배 → 16 → time_delay/holding_period이 1개씩이면 그대로 16
    "MA": {
        "short_ma_periods": [5, 9, 20, 30],   # (4개)
        "long_ma_periods": [50, 200],        # (2개)
        "band_filters": [0, 0.05],           # (2개 - 0% or 1% 폭)
        "time_delays": [0],                  # (1개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 2) RSI
    #   lookback: 4개, time_delay: 2개, → 4 * 2 = 8
    #   band_filters나 oversold/overbought를 더 다양화할 수도 있으나,
    #   여기서는 time_delay로 2배수. (band_filter=2개로도 가능)
    #   holding_period=1개씩이면 8 → 16 원하는 분기는 아래와 다를 수 있음.
    #   여기서는 band_filters=[0], time_delays=[0,2] 식으로 2배수
    "RSI": {
        "lookback_periods": [7, 14, 21, 28],  # (4개)
        "thresholds": [10,20],
        "time_delays": [0, 2],               # (2개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 3) OBV
    #   short_ma:2개, long_ma:2개 →4, band=[0,0.01]→×2=8, time_delays=[0,2]→×2=16
    "OBV": {
        "short_ma_periods": [5, 10],         # (2개)
        "long_ma_periods": [20, 50],         # (2개)
        "band_filters": [0, 0.05],           # (2개)
        "time_delays": [0, 2],               # (2개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 4) MACD
    #   fast=[3,6,9,12], slow=[26,30], signal=[9,12] 하면 4×2×2=16, band=0/time_delay=0/holding=1로 16 유지
    "MACD": {
        "fast_periods": [3, 6, 9, 12],       # (4개)
        "slow_periods": [26, 30],            # (2개)
        "signal_periods": [9, 12],           # (2개)
        "band_filters": [0],                 # (1개)
        "time_delays": [0],                  # (1개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 5) DMI & ADX
    #   dmi_periods: 4개, adx_thresholds: 2개, time_delays: 2개 → 4×2×2=16, band=1, hold=1
    "DMI_ADX": {
        "dmi_periods": [14, 20, 30, 40],     # (4개)
        "adx_thresholds": [20, 25],          # (2개)
        "band_filters": [0],                 # (1개)
        "time_delays": [0, 6],               # (2개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 6) 볼린저 밴드 (BOLL)
    #   lookback=2개, stddev=4개 →8, time_delay=2개→16
    "BOLL": {
        "lookback_periods": [20, 50],        # (2개)
        "stddev_multipliers": [2, 2.5, 3, 3.5], # (4개)
        "band_filters": [0],                 # (1개)
        "time_delays": [0, 6],               # (2개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 7) 이치모쿠 (ICHIMOKU)
    #   tenkan=4, kijun=2, span_b=2 →4×2×2=16
    "ICHIMOKU": {
        "tenkan_period": [7, 9, 12, 15],     # (4개)
        "kijun_period": [26, 30],            # (2개)
        "senkou_span_b_period": [52, 60],    # (2개)
        "band_filters": [0],                 # (1개)
        "time_delays": [0],                  # (1개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 8) 파라볼릭 SAR (PSAR)
    #   step=2개, max=2개, band=2개, time_delay=1개 등으로 16 맞춤
    "PSAR": {
        "acceleration_step": [0.01, 0.02],   # (2개)
        "acceleration_max": [0.2, 0.3],      # (2개)
        "band_filters": [0, 0.05],           # (2개)
        "time_delays": [0],                  # (1개)
        "holding_periods": [float('inf')]    # (1개)
    },

    # 9) 슈퍼트렌드 (SUPERTREND)
    #   atr_period=2, multiplier=2 → 4 combos
    #   band=2개, time_delay=2개 → 4×2=8×2=16
    "SUPERTREND": {
        "atr_period": [7, 10],               # (2개)
        "multiplier": [2, 3],               # (2개)
        "band_filters": [0, 0.05],          # (2개)
        "time_delays": [0, 2],              # (2개)
        "holding_periods": [float('inf')]   # (1개)
    },

    # 10) 피보나치 (FIBO)
    #   levels: 4세트, band=2, time_delay=2 → 4×2=8×2=16
    "FIBO": {
        "levels": [
            [0.382, 0.5, 0.618],
            [0.5, 0.618, 0.786],
            [0.236, 0.382, 0.5, 0.618],
            [0.382, 0.5, 0.618, 0.786],
        ],
        "band_filters": [0, 0.05],
        "time_delays": [0, 6],
        "holding_periods": [float('inf')]
    },

    # 11) Support/Resistance (SR)
    #   lookback=4개, band=2개, time_delay=2개 => 4×2×2=16
    "SR": {
        "lookback_periods": [10, 20, 30, 60],
        "band_filters": [0, 0.05],
        "time_delays": [0, 6],
        "holding_periods": [float('inf')]
    },

    # 12) 채널 돌파 (CB)
    #   lookback=4개, c_percent=[0,0.01], time_delay=2개 => 4×2×2=16
    "CB": {
        "lookback_periods": [20, 55, 100, 200],
        "c_percent_channels": [0, 0.05],
        "band_filters": [0],
        "time_delays": [0, 6],
        "holding_periods": [float('inf')]
    },

    # 13) Filter 룰
    #   lookback=2, uniform_filters=4 =>8, time_delay=2 =>16 (band_filter=?)
    #   holding_period=1개 => 그대로 16
    "Filter": {
        "lookback_periods": [5, 20],
        "uniform_filters": [0.05, 0.1, 0.5, 1],
        "uniform_time_delays": [0, 3],
        "holding_periods": [float('inf')]
    }
}

