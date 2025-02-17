# indicators/filter.py

import pandas as pd
from backup.multi_inticator.indicators.IndicatorBase import IndicatorBase

class FilterIndicator(IndicatorBase):
    def __init__(self, x=0.05, y=0.05, window=10, d=0):
        """
        Filter rule:
        :param x: 매수 필터 비율
        :param y: 매도 필터 비율
        :param window: rolling window
        :param d: (옵션) 추가 지연 파라미터(현 로직에는 사용X)
        """
        super().__init__()
        self.x = float(x)
        self.y = float(y)
        self.window = int(window)
        self.d = d

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["low"] = pd.to_numeric(df["low"], errors="coerce").ffill().bfill()
        df["high"] = pd.to_numeric(df["high"], errors="coerce").ffill().bfill()
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill().bfill()

        df["rolling_min"] = df["low"].rolling(window=self.window, min_periods=1).min()
        df["rolling_max"] = df["high"].rolling(window=self.window, min_periods=1).max()

        df["signal"] = 0
        df.loc[df["close"] > df["rolling_min"]*(1.0 + self.x), "signal"] = 1
        df.loc[df["close"] < df["rolling_max"]*(1.0 - self.y), "signal"] = -1
        return df[["rolling_min", "rolling_max", "signal"]]
