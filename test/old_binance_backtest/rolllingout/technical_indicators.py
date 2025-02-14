# technical_indicators.py

import pandas as pd
import pandas_ta as ta

"""
연구 논문(“Are simple technical trading rules profitable in Bitcoin markets?”)에서 다룬
주요 지표(또는 룰)들은 다음과 같습니다.

1) Moving Averages (MA)
2) Filter Rules
3) Support & Resistance (S&R)
4) Channel Breakouts (CB)
5) On-Balance Volume (OBV)
6) Relative Strength Indicator (RSI)

아래 코드는 논문에 없는 Bollinger Bands 등은 제거하고,
논문에서 다룬 MA, RSI, OBV에 더해 S&R, Filter, CB에 필요한
기본적인 rolling max/min 등을 계산해주는 함수를 간단히 추가한 예시입니다.

※ 실제 매매 시그널 생성(진입/청산)은 signal_generation.py 내에서
  각 룰별로 조건을 종합해 처리합니다.
"""

def add_ma(
    df: pd.DataFrame,
    short_period: int,
    long_period: int,
    price_col: str = 'close',
    prefix: str = 'MA'
) -> pd.DataFrame:
    """
    (1) Moving Average
    - 논문 부록에서 언급된 MA 전략용.
    - 단순 이동평균(SMA) 2개(short/long)를 계산해 df에 열을 추가합니다.
      short_period < long_period 권장.
    """
    df[f'{prefix}_{short_period}'] = ta.sma(df[price_col], length=short_period)
    df[f'{prefix}_{long_period}'] = ta.sma(df[price_col], length=long_period)
    return df


def add_rsi(
    df: pd.DataFrame,
    period: int = 14,
    price_col: str = 'close',
    col_name: str = 'RSI'
) -> pd.DataFrame:
    """
    (2) Relative Strength Indicator (RSI)
    - 논문에서 h(lookback), v(50±v) 등 다양한 파라미터로
      RSI를 활용하는데, 여기서는 기본 RSI 열만 생성.
    """
    df[col_name] = ta.rsi(df[price_col], length=period)
    return df


def add_obv(
    df: pd.DataFrame,
    price_col: str = 'close',
    volume_col: str = 'volume',
    col_name: str = 'OBV',
    ma_period: int = None,
    ma_prefix: str = 'OBV_MA'
) -> pd.DataFrame:
    """
    (3) On-Balance Volume (OBV)
    - 가격 상승이면 거래량(Volume)을 누적으로 더하고, 하락이면 빼며
      추세와 거래량의 상관관계를 파악하는 지표.
    - ma_period가 있으면 OBV의 이동평균을 추가(논문에서 p,q 등).
    """
    df[col_name] = ta.obv(df[price_col], df[volume_col])
    if ma_period is not None and ma_period > 1:
        df[f'{ma_prefix}_{ma_period}'] = ta.sma(df[col_name], length=ma_period)
    return df


def add_sr(
    df: pd.DataFrame,
    window: int = 20,
    prefix: str = 'SR'
) -> pd.DataFrame:
    """
    (4) Support & Resistance (S&R)을 위한 기본 데이터:
    - 최근 window 기간의 최고가/최저가(rolling max/min)를 저장.
    - 실제 매매 신호는 signal_generation.py 등에서
      close > max*(1+x) 시 매수 등으로 처리.
    """
    df[f'{prefix}_high'] = df['high'].rolling(window).max()
    df[f'{prefix}_low'] = df['low'].rolling(window).min()
    return df


def add_filter(
    df: pd.DataFrame,
    window: int = 20,
    prefix: str = 'FL'
) -> pd.DataFrame:
    """
    (5) Filter Rules
    - 논문에서는 '가장 최근 low 대비 x% 상승' 등으로 진입,
      '가장 최근 high 대비 y% 하락'으로 청산 등 다양한 변형을 다룸.
    - 여기서는 필수 기본값인 최근 window 기간의 rolling min/max만 추가해둠.
    - 구체적 매수/매도 로직은 signal_generation.py 등에서 구현.
    """
    df[f'{prefix}_min'] = df['low'].rolling(window).min()
    df[f'{prefix}_max'] = df['high'].rolling(window).max()
    return df


def add_cb(
    df: pd.DataFrame,
    window: int = 20,
    prefix: str = 'CB'
) -> pd.DataFrame:
    """
    (6) Channel Breakout (CB)을 위한 기본 데이터:
    - 최근 window 기간의 high/low를 이용해 채널 폭 계산.
    - 논문에서 c% 미만의 좁은 채널 형성 + high 돌파 시 매수 등.
    - 여기서는 rolling high/low만 추가.
    """
    df[f'{prefix}_high'] = df['high'].rolling(window).max()
    df[f'{prefix}_low'] = df['low'].rolling(window).min()
    # 추가로 채널 폭 계산 컬럼
    df[f'{prefix}_width'] = (df[f'{prefix}_high'] - df[f'{prefix}_low']).abs()
    return df


def add_all_indicators():
    return None