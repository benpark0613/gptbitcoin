# strategies/signal_generator.py

import pandas as pd

# 인디케이터별 클래스 임포트
from backup.multi_inticator.indicators import MAIndicator
from backup.multi_inticator.indicators import RSIIndicator
from backup.multi_inticator.indicators import FilterIndicator
from backup.multi_inticator.indicators import ChannelBreakoutIndicator
from backup.multi_inticator.indicators import OBVIndicator
from backup.multi_inticator.indicators import SupportResistanceIndicator

class SignalGenerator:
    def __init__(self, config, combiner=None):
        """
        :param config: dict 형태로 각 인디케이터별 설정이 들어 있음
            예시:
            {
              "MA": {"short_period": 12, "long_period": 26, "price": "close"},
              "RSI": {"length": 14, "overbought": 70, "oversold": 30},
              "Filter": {"x": 0.05, "y": 0.05, "window": 10},
              "ChannelBreakout": {"window": 20, "c": 0.05},
              "OBV": {"short_period": 2, "long_period": 6},
              "Support_Resistance": {"window": 20}
            }
        :param combiner: 신호 결합 함수(디폴트는 단순 합산 후 ±1/0 결정)
        """
        self.config = config
        self.indicators = {}
        self._init_indicators()  # 인디케이터 초기화
        self.combiner = combiner if combiner else self.default_combiner

    def _init_indicators(self):
        """
        config에 인디케이터별 섹션이 있으면, 해당 인자를 명시적으로 꺼내
        인디케이터 객체를 생성.
        """
        # 예: "MA": {"short_period": 12, "long_period": 26}
        if "MA" in self.config:
            ma_cfg = self.config["MA"]
            short_p = ma_cfg.get("short_period", 12)
            long_p = ma_cfg.get("long_period", 26)
            price = ma_cfg.get("price", "close")
            self.indicators["MA"] = MAIndicator(
                short_period=short_p,
                long_period=long_p,
                price=price
            )

        if "RSI" in self.config:
            rsi_cfg = self.config["RSI"]
            length = rsi_cfg.get("length", 14)
            overbought = rsi_cfg.get("overbought", 70)
            oversold = rsi_cfg.get("oversold", 30)
            self.indicators["RSI"] = RSIIndicator(
                length=length,
                overbought=overbought,
                oversold=oversold
            )

        if "Filter" in self.config:
            f_cfg = self.config["Filter"]
            x_val = f_cfg.get("x", 0.05)
            y_val = f_cfg.get("y", 0.05)
            window = f_cfg.get("window", 10)
            d_val = f_cfg.get("d", 0)  # 혹은 제거 가능
            self.indicators["Filter"] = FilterIndicator(
                x=x_val,
                y=y_val,
                window=window,
                d=d_val
            )

        if "ChannelBreakout" in self.config:
            cb_cfg = self.config["ChannelBreakout"]
            win = cb_cfg.get("window", 20)
            c_val = cb_cfg.get("c", 0.05)
            self.indicators["ChannelBreakout"] = ChannelBreakoutIndicator(
                window=win,
                c=c_val
            )

        if "OBV" in self.config:
            obv_cfg = self.config["OBV"]
            sp = obv_cfg.get("short_period", 2)
            lp = obv_cfg.get("long_period", 6)
            self.indicators["OBV"] = OBVIndicator(
                short_period=sp,
                long_period=lp
            )

        if "Support_Resistance" in self.config:
            sr_cfg = self.config["Support_Resistance"]
            win = sr_cfg.get("window", 20)
            self.indicators["Support_Resistance"] = SupportResistanceIndicator(
                window=win
            )

    def default_combiner(self, signals_df):
        """
        기본 신호 결합 방식: 각 인디케이터 신호의 합을 구한 후,
        양수이면 1, 음수이면 -1, 0이면 0 반환.
        """
        combined = signals_df.sum(axis=1)
        return combined.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    def generate_combined_signal(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        1) 각 인디케이터의 generate_signals()를 호출
        2) 'signal' 컬럼만 추출하여 signals_df에 모아둠
        3) default_combiner(또는 사용자 지정 combiner)로 최종 final_signal 생성
        4) 원본 df에 합쳐 반환
        """
        signals = pd.DataFrame(index=df.index)

        for name, indicator in self.indicators.items():
            ind_signals = indicator.generate_signals(df)
            signals[f"{name}_signal"] = ind_signals["signal"]

        signals["final_signal"] = self.combiner(signals)

        result = df.copy()
        result = result.join(signals)
        return result


if __name__ == "__main__":
    import numpy as np

    # 샘플 데이터
    dates = pd.date_range(start="2025-01-01", periods=10, freq="D")
    sample_df = pd.DataFrame({
        "open": np.random.uniform(30000,40000,10),
        "high": np.random.uniform(40000,50000,10),
        "low": np.random.uniform(20000,30000,10),
        "close": np.random.uniform(30000,40000,10),
        "volume": np.random.uniform(100,1000,10)
    }, index=dates)

    # 예: config
    config = {
        "MA": {"short_period": 5, "long_period": 10},
        "RSI": {"length": 14, "overbought": 70, "oversold": 30},
        "Filter": {"x": 0.05, "y": 0.05, "window": 10, "d": 0},
        # ...
        "transaction_fee_rate": 0.0004,  # 이건 strategy용. 굳이 signal_generator가 쓸 필드는 아님
    }

    sg = SignalGenerator(config)
    combined_df = sg.generate_combined_signal(sample_df)
    print(combined_df)
