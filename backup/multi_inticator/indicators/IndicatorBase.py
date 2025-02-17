# indicators/IndicatorBase.py

from abc import ABC, abstractmethod
import pandas as pd


class IndicatorBase(ABC):
    """
    모든 보조지표 클래스가 상속할 추상 기본 클래스입니다.
    각 보조지표 클래스는 generate_signals 메서드를 구현하여,
    주어진 OHLCV DataFrame에서 해당 지표를 계산하고 거래 신호를 생성해야 합니다.
    """

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        주어진 OHLCV DataFrame에서 해당 보조지표를 계산하고 거래 신호를 생성합니다.

        Parameters:
            df (pd.DataFrame): OHLCV 데이터를 포함하는 DataFrame (필수 컬럼: open, high, low, close, volume)

        Returns:
            pd.DataFrame: 계산된 지표 및 거래 신호를 포함하는 DataFrame.
        """
        pass
