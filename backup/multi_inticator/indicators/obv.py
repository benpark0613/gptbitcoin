# indicators/obv.py

import pandas as pd
import pandas_ta as ta
from backup.multi_inticator.indicators.IndicatorBase import IndicatorBase

class OBVIndicator(IndicatorBase):
    def __init__(self, short_period=2, long_period=6):
        super().__init__()
        self.short_period = int(short_period)
        self.long_period = int(long_period)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill().bfill().astype(float)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").ffill().bfill().astype(float)

        df["obv"] = ta.obv(df["close"], df["volume"])
        df["obv_short"] = ta.sma(df["obv"], length=self.short_period)
        df["obv_long"] = ta.sma(df["obv"], length=self.long_period)

        df["signal"] = 0
        valid = df["obv_short"].notna() & df["obv_long"].notna()
        df.loc[valid & (df["obv_short"] > df["obv_long"]), "signal"] = 1
        df.loc[valid & (df["obv_short"] < df["obv_long"]), "signal"] = -1

        return df[["obv","obv_short","obv_long","signal"]]
