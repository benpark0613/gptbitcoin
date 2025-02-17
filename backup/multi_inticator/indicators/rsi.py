# indicators/rsi.py

import pandas as pd
import pandas_ta as ta
from backup.multi_inticator.indicators.IndicatorBase import IndicatorBase

class RSIIndicator(IndicatorBase):
    def __init__(self, length=14, overbought=70, oversold=30):
        super().__init__()
        self.length = int(length)
        self.overbought = float(overbought)
        self.oversold = float(oversold)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill().bfill().astype(float)
        df["rsi"] = ta.rsi(df["close"], length=self.length)

        df["signal"] = 0
        valid = df["rsi"].notna()
        df.loc[valid & (df["rsi"] < self.oversold), "signal"] = 1
        df.loc[valid & (df["rsi"] > self.overbought), "signal"] = -1
        return df[["rsi", "signal"]]
