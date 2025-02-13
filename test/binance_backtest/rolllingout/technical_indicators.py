# technical_indicators.py

import pandas as pd
import pandas_ta as ta


def add_ma(
    df: pd.DataFrame,
    short_period: int,
    long_period: int,
    price_col: str = 'close',
    prefix: str = 'MA',
    x: float = 0.0
) -> pd.DataFrame:
    """
    논문 부록의 MA 파라미터에 맞춰, 단기/장기 이동평균(SMA)을 pandas_ta로 계산.
    - short_period < long_period 권장
    - x != 0 이면 퍼센트 밴드를 적용한 '단기 SMA * (1 + x)' 같은 계산을 지표로 활용할 수도 있으나,
      여기서는 단순히 SMA 열만 생성. (퍼센트 밴드는 매매 시그널 단계에서 참고 가능)
    """
    # 단기 SMA
    df[f'{prefix}_{short_period}'] = ta.sma(df[price_col], length=short_period)
    # 장기 SMA
    df[f'{prefix}_{long_period}'] = ta.sma(df[price_col], length=long_period)
    return df


def add_rsi(
    df: pd.DataFrame,
    period: int = 14,
    price_col: str = 'close',
    col_name: str = 'RSI'
) -> pd.DataFrame:
    """
    RSI를 계산해 df[col_name] 열로 추가. (논문에서는 h, v 등 매매 관련 파라미터를 추가로 사용)
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
    On-Balance Volume(OBV)을 계산해 df[col_name] 열에 추가.
    - ma_period가 주어지면, OBV에 대한 이동평균도 추가 (OBV 교차전략 등 활용 가능)
    """
    df[col_name] = ta.obv(df[price_col], df[volume_col])
    if ma_period is not None and ma_period > 1:
        df[f'{ma_prefix}_{ma_period}'] = ta.sma(df[col_name], length=ma_period)
    return df


def add_bbands(
    df: pd.DataFrame,
    period: int = 20,
    std: float = 2.0,
    price_col: str = 'close',
    prefix: str = 'BB'
) -> pd.DataFrame:
    """
    볼린저 밴드(Bollinger Bands)를 pandas_ta로 계산.
    - period, std 파라미터 (논문 기본 20,2.0)
    """
    bb_df = ta.bbands(df[price_col], length=period, std=std)
    if bb_df is None or bb_df.empty:
        print("WARNING: Bollinger bands returned None or empty.")
        return df

    # pandas_ta 기본: 'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0'
    low_col = f'BBL_{period}_{std}.0'
    mid_col = f'BBM_{period}_{std}.0'
    up_col  = f'BBU_{period}_{std}.0'

    if low_col not in bb_df.columns:
        # 간혹 float 문자열 변환 차이가 있을 수 있음
        # ex: 'BBL_20_2.0' vs 'BBL_20_2'
        # 필요 시 bb_df.columns를 확인해 매칭 로직 구현
        found_low_col = [c for c in bb_df.columns if c.startswith('BBL')]
        found_mid_col = [c for c in bb_df.columns if c.startswith('BBM')]
        found_up_col  = [c for c in bb_df.columns if c.startswith('BBU')]
        if found_low_col:
            low_col = found_low_col[0]
        if found_mid_col:
            mid_col = found_mid_col[0]
        if found_up_col:
            up_col = found_up_col[0]

    df[f'{prefix}_low'] = bb_df[low_col]
    df[f'{prefix}_mid'] = bb_df[mid_col]
    df[f'{prefix}_up']  = bb_df[up_col]
    return df


def add_all_indicators(
    df: pd.DataFrame,
    ma_short: int = 9,
    ma_long: int = 20,
    ma_x: float = 0.0,          # 퍼센트 밴드용
    rsi_period: int = 14,
    obv_ma_period: int = None, # OBV 이동평균
    bb_period: int = 20,
    bb_std: float = 2.0
) -> pd.DataFrame:
    """
    연구논문에서 사용하는 대표 지표(MA, RSI, OBV, BollBand)를
    한 번에 붙이는 함수. 파라미터를 지정 가능.
    - ma_short, ma_long, ma_x: MA
    - rsi_period: RSI
    - obv_ma_period: OBV 이동평균
    - bb_period, bb_std: 볼린저밴드
    """
    # (1) MA
    df = add_ma(df, short_period=ma_short, long_period=ma_long,
                x=ma_x, price_col='close', prefix='MA')

    # (2) RSI
    df = add_rsi(df, period=rsi_period, price_col='close', col_name=f'RSI_{rsi_period}')

    # (3) OBV (+optional 이동평균)
    df = add_obv(df, price_col='close', volume_col='volume',
                 col_name='OBV', ma_period=obv_ma_period)

    # (4) Bollinger Bands
    df = add_bbands(df, period=bb_period, std=bb_std, price_col='close', prefix='BB')

    return df


if __name__ == "__main__":
    # 예시: 임의의 작은 데이터로 지표를 계산해 본다.
    from io import StringIO
    data_str = """open_time_dt,open,high,low,close,volume
2024-01-01,42314.00,44266.00,42207.90,44230.20,206424.144
2024-01-02,44230.30,45950.00,44200.90,44979.80,459798.523
2024-01-03,44979.70,45582.30,40333.00,42849.50,595855.225
2024-01-04,42849.50,44840.80,42625.00,44143.80,333923.098
2024-01-05,44143.80,44500.00,42300.00,44145.40,374967.791
2024-01-06,44145.30,44214.60,43391.30,43956.70,138542.797
2024-01-07,43956.60,44486.90,43557.50,43916.90,180710.714
"""
    df_test = pd.read_csv(StringIO(data_str), parse_dates=['open_time_dt'])
    df_test.set_index('open_time_dt', inplace=True)

    # 지표 계산 예: MA(9,20), RSI(14), OBV(이동평균 없음), BB(20,2.0)
    df_test = add_all_indicators(
        df_test,
        ma_short=9,
        ma_long=20,
        ma_x=0.05,        # 예) 퍼센트 밴드, 여기서는 단순히 지표명만 기록
        rsi_period=14,
        obv_ma_period=None,
        bb_period=20,
        bb_std=2.0
    )

    print("=== Data with Indicators ===")
    print(df_test)
    print("\n=== Summary ===")
    print(df_test.describe(include='all'))
