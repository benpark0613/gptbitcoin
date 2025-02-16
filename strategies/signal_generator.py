# signal_generator.py
# -------------------------------------------
# 논문과 같은 "각 지표 × 파라미터" 독립 테스트를 위해,
# 이전에 여러 지표 신호를 합산하던 로직을 제거하고,
# 단 하나의 지표에 대해서만 매매 시그널을 생성하는 클래스를 정의합니다.
# -------------------------------------------

import pandas as pd

# 개별 지표(Indicator) 클래스들
from indicators.ma import MAIndicator
from indicators.rsi import RSIIndicator
from indicators.filter import FilterIndicator
from indicators.channel_breakout import ChannelBreakoutIndicator
from indicators.obv import OBVIndicator
from indicators.support_resistance import SupportResistanceIndicator


class SignalGenerator:
    """
    논문 방식:
      - "하나의 지표"와 "해당 지표의 파라미터 조합"을 독립된 전략으로 간주.
      - 예: MAIndicator(short_period=5, long_period=20) 하나면 그 자체가 하나의 룰.
      - RSIIndicator(length=14, overbought=70, oversold=30) 또 다른 룰.
      => 각각 별도로 백테스트 후 성과 비교.

    이 클래스로 "signal" 칼럼만 생성해서, Strategy가 해당 신호를 매매에 활용.

    ※ config 예시:
      {
        "MA": {
          "short_period": 5,
          "long_period": 20,
          "price": "close"
        },
        "shorting_allowed": false,    # -> Strategy에서만 사용
        "time_delay": 0,             # -> Strategy에서만 사용
        ...
      }
      또는
      {
        "RSI": {
          "length":14,
          "overbought":70,
          "oversold":30
        }
      }
    """

    def __init__(self, config):
        """
        config 딕셔너리 안에는 반드시
        MA / RSI / Filter / ChannelBreakout / OBV / Support_Resistance
        중 '하나'만 존재해야 함.
        """
        self.config = config
        self.indicator_name = None
        self.indicator = None

        # 어떤 지표를 쓸지 확인하고, 인스턴스 생성
        self._init_indicator()

    def _init_indicator(self):
        """
        config에서 지표명이 하나만 있어야 함. (논문 방식: 독립 테스트)
        여러 지표가 들어 있으면 ValueError 발생.
        """
        valid_keys = ["MA", "RSI", "Filter", "ChannelBreakout", "OBV", "Support_Resistance"]
        indicator_keys = [k for k in self.config.keys() if k in valid_keys]

        if len(indicator_keys) == 0:
            raise ValueError(
                "config에 사용 가능한 지표(MA, RSI, Filter, ChannelBreakout, "
                "OBV, Support_Resistance) 중 하나가 포함되어야 합니다."
            )
        if len(indicator_keys) > 1:
            raise ValueError(
                f"SingleIndicatorSignalGenerator는 한 지표만 지원합니다. "
                f"지금 발견된 지표={indicator_keys}"
            )

        # 이제 indicator_keys[0]만 실제 사용
        self.indicator_name = indicator_keys[0]
        ind_cfg = self.config[self.indicator_name]

        if self.indicator_name == "MA":
            short_p = ind_cfg.get("short_period", 12)
            long_p = ind_cfg.get("long_period", 26)
            price_col = ind_cfg.get("price", "close")
            self.indicator = MAIndicator(
                short_period=short_p,
                long_period=long_p,
                price=price_col
            )

        elif self.indicator_name == "RSI":
            length = ind_cfg.get("length", 14)
            overb = ind_cfg.get("overbought", 70)
            overs = ind_cfg.get("oversold", 30)
            self.indicator = RSIIndicator(
                length=length,
                overbought=overb,
                oversold=overs
            )

        elif self.indicator_name == "Filter":
            x_val = ind_cfg.get("x", 0.05)
            y_val = ind_cfg.get("y", 0.05)
            window = ind_cfg.get("window", 10)
            d_val = ind_cfg.get("d", 0)
            self.indicator = FilterIndicator(
                x=x_val,
                y=y_val,
                window=window,
                d=d_val
            )

        elif self.indicator_name == "ChannelBreakout":
            win = ind_cfg.get("window", 20)
            c_val = ind_cfg.get("c", 0.05)
            self.indicator = ChannelBreakoutIndicator(
                window=win,
                c=c_val
            )

        elif self.indicator_name == "OBV":
            sp = ind_cfg.get("short_period", 2)
            lp = ind_cfg.get("long_period", 6)
            self.indicator = OBVIndicator(
                short_period=sp,
                long_period=lp
            )

        elif self.indicator_name == "Support_Resistance":
            win = ind_cfg.get("window", 20)
            self.indicator = SupportResistanceIndicator(window=win)
        else:
            raise ValueError(f"지원하지 않는 지표명: {self.indicator_name}")

    def generate_signal(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        주어진 OHLCV DataFrame에 'signal' 칼럼(+1/-1/0)을 추가하여 반환.
        이때, 단일 지표만 사용해 매매 시그널을 생성.
        """
        df_ind = self.indicator.generate_signals(df)
        result = df.copy()
        # 지표가 반환하는 "signal" 컬럼을 그대로 최종 매매 시그널로 쓴다.
        # (논문은 '지표 하나 × 파라미터'가 하나의 전략)
        result["signal"] = df_ind["signal"].fillna(0)
        return result


# ---------------------------------------------------------------------
# 사용 예시:
#
# config = {
#   "MA": {
#       "short_period": 5,
#       "long_period": 20
#   },
#   "time_delay": 0,
#   "holding_period": None
# }
#
# sig_gen = SingleIndicatorSignalGenerator(config)
# df_with_signal = sig_gen.generate_signal(df_ohlcv)
# -> df_with_signal["signal"]이 +1/-1/0
#
# 그런 뒤 Strategy(simulate) 등에 넘겨 독립적으로 백테스트.
# ---------------------------------------------------------------------
