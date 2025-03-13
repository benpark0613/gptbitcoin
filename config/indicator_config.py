# gptbitcoin/config/indicator_config.py
# 보조지표 파라미터 및 매매시그널 정의 (매수: +1, 중립: 0, 매도: -1)
# 이 설정은 전반적으로 "추세추종" 방식을 가정한다.

INDICATOR_COMBO_SIZES = [1] # 테스트용 삭제 금지
# INDICATOR_COMBO_SIZES = [1, 2]

INDICATOR_CONFIG = {
    "MA": {
        "short_ma_periods": [5, 10, 20, 30, 50],
        "long_ma_periods": [100, 150, 200, 250],
        # 매매시그널(추세추종): short_ma > long_ma → +1, short_ma < long_ma → -1, 그 외 → 0
    },

    "RSI": {
        "lookback_periods": [7, 14, 21, 28],
        "thresholds": [[30, 70], [20, 80], [25, 75], [40, 60]],
        # 매매시그널(추세추종): RSI > upper_threshold → +1, RSI < lower_threshold → -1, 그 외 → 0
    },

    "OBV": {
        "short_ma_periods": [5, 10, 20, 30, 50, 60],
        "long_ma_periods": [100, 150, 200, 250, 300],
        # 매매시그널(추세추종): OBV의 단기이평 > 장기이평 → +1, 단기이평 < 장기이평 → -1, 그 외 → 0
    },

    "MACD": {
        "fast_periods": [6, 9, 12, 15],
        "slow_periods": [26, 30],
        "signal_periods": [9, 12],
        # 매매시그널(추세추종): MACD 라인 > 시그널 라인 → +1, MACD 라인 < 시그널 라인 → -1, 그 외 → 0
    },

    "DMI_ADX": {
        "lookback_periods": [7, 14, 20, 28],
        "adx_thresholds": [20, 25, 30, 35, 40],
        # 매매시그널(추세추종): ADX > threshold일 때 +DI > -DI → +1, +DI < -DI → -1, 그 외 → 0
    },

    "BOLL": {
        "lookback_periods": [14, 20, 30, 50],
        "stddev_multipliers": [2, 2.5, 3],
        # 매매시그널(추세추종): 가격 > upper_band → +1, 가격 < lower_band → -1, 그 외 → 0
    },

    "ICHIMOKU": {
        "tenkan_period": [7, 9, 12],
        "kijun_period": [22, 26, 30],
        "senkou_span_b_period": [52, 60],
        # 매매시그널(추세추종): 전환선 > 기준선 & (가격 > 구름대) → +1,
        #                     반대 조건(전환선 < 기준선 & 가격 < 구름대) → -1, 그 외 → 0
    },

    "PSAR": {
        "acceleration_step": [0.01, 0.02, 0.03],
        "acceleration_max": [0.2, 0.3],
        # 매매시그널(추세추종): 가격이 PSAR 위 → +1, 가격이 PSAR 아래 → -1
    },

    "SUPERTREND": {
        "atr_period": [10, 14, 20],
        "multiplier": [2, 3, 4],
        # 매매시그널(추세추종): 가격이 Supertrend 위 → +1, 아래 → -1
    },

    "DONCHIAN_CHANNEL": {
        "lookback_periods": [20, 30, 55, 100],
        # 매매시그널(추세추종): 가격 > upper_channel → +1, 가격 < lower_channel → -1, 그 외 → 0
    },

    "STOCH": {
        "k_period": [14, 21],
        "d_period": [3, 5],
        "thresholds": [[20, 80], [25, 75], [30, 70]],
        # 매매시그널(추세추종): K & D > upper_threshold → +1, K & D < lower_threshold → -1, 그 외 → 0
    },

    "STOCH_RSI": {
        "rsi_periods": [14, 21],       # RSI 계산에 사용될 기간
        "stoch_periods": [14, 21],     # Stochastic 변환에 사용될 기간
        "k_period": [3, 5],
        "d_period": [3, 5],
        "thresholds": [[20, 80], [25, 75], [30, 70]],
        # 매매시그널(추세추종): StochRSI > upper_threshold → +1, StochRSI < lower_threshold → -1, 그 외 → 0
    },

    "MFI": {
        "lookback_periods": [14, 20, 28],
        "thresholds": [[20, 80], [30, 70], [40, 60]],
        # 매매시그널(추세추종): MFI > upper_threshold → +1, MFI < lower_threshold → -1, 그 외 → 0
    },

    "VWAP": {
        # 매매시그널(추세추종): 가격 > VWAP → +1, 가격 < VWAP → -1
    },

}
