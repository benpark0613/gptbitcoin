# indicators/ma.py

import pandas as pd
import pandas_ta as ta
from backup.multi_inticator.indicators.IndicatorBase import IndicatorBase


class MAIndicator(IndicatorBase):
    def __init__(self, short_period=12, long_period=26, price="close"):
        """
        :param short_period: 단기 이동평균 기간
        :param long_period: 장기 이동평균 기간
        :param price: 사용할 가격 컬럼명 (기본 "close")
        """
        super().__init__()
        self.short_period = int(short_period)
        self.long_period = int(long_period)
        self.price = price

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        price_series = pd.to_numeric(df[self.price], errors="coerce").ffill().bfill().astype(float)

        if price_series.dropna().shape[0] < self.short_period:
            df["short_sma"] = pd.NA
        else:
            df["short_sma"] = ta.sma(price_series, length=self.short_period)

        if price_series.dropna().shape[0] < self.long_period:
            df["long_sma"] = pd.NA
        else:
            df["long_sma"] = ta.sma(price_series, length=self.long_period)

        df["signal"] = 0
        valid = df["short_sma"].notna() & df["long_sma"].notna()
        df.loc[valid & (df["short_sma"] > df["long_sma"]), "signal"] = 1
        df.loc[valid & (df["short_sma"] < df["long_sma"]), "signal"] = -1

        return df[["short_sma", "long_sma", "signal"]]
