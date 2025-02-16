# indicators/support_resistance.py

import pandas as pd
from indicators.IndicatorBase import IndicatorBase

class SupportResistanceIndicator(IndicatorBase):
    def __init__(self, window=20):
        """
        :param window: 롤링 윈도우 길이
        """
        super().__init__()
        self.window = int(window)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        support = rolling min(low)
        resistance = rolling max(high)
        - 종가가 이전 bar의 resistance 초과 -> 매수
        - 종가가 이전 bar의 support 미만 -> 매도
        """
        df = df.copy()
        df["low"] = pd.to_numeric(df["low"], errors="coerce").ffill().bfill()
        df["high"] = pd.to_numeric(df["high"], errors="coerce").ffill().bfill()
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill().bfill()

        df["support"] = df["low"].rolling(window=self.window, min_periods=1).min()
        df["resistance"] = df["high"].rolling(window=self.window, min_periods=1).max()

        df["support_prev"] = df["support"].shift(1)
        df["resistance_prev"] = df["resistance"].shift(1)

        df["signal"] = 0
        df.loc[df["close"] > df["resistance_prev"], "signal"] = 1
        df.loc[df["close"] < df["support_prev"], "signal"] = -1

        return df[["support", "resistance", "signal"]]


if __name__ == "__main__":
    import numpy as np
    dates = pd.date_range(start="2025-01-01", periods=20, freq="D")
    sample_df = pd.DataFrame({
        "low": np.random.uniform(20000,30000,20),
        "high": np.random.uniform(40000,50000,20),
        "close": np.random.uniform(30000,40000,20)
    }, index=dates)

    sr_indicator = SupportResistanceIndicator(window=20)
    result = sr_indicator.generate_signals(sample_df)
    print(result)
