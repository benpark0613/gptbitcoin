# indicators/channel_breakout.py

import pandas as pd
from backup.multi_inticator.indicators.IndicatorBase import IndicatorBase

class ChannelBreakoutIndicator(IndicatorBase):
    def __init__(self, window=20, c=0.05):
        super().__init__()
        self.window = int(window)
        self.c = float(c)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["low"] = pd.to_numeric(df["low"], errors="coerce").ffill().bfill()
        df["high"] = pd.to_numeric(df["high"], errors="coerce").ffill().bfill()
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill().bfill()

        df["channel_min"] = df["low"].rolling(window=self.window, min_periods=1).min()
        df["channel_max"] = df["high"].rolling(window=self.window, min_periods=1).max()

        df["channel_exists"] = ((df["channel_max"] - df["channel_min"]) / df["channel_min"]) <= self.c

        df["signal"] = 0
        df.loc[(df["channel_exists"]) & (df["close"] > df["channel_max"]), "signal"] = 1
        df.loc[(df["channel_exists"]) & (df["close"] < df["channel_min"]), "signal"] = -1
        return df[["channel_min","channel_max","signal"]]
