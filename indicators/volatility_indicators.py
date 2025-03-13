# gptbitcoin/indicators/volatility_indicators.py
# 변동성 기반 보조지표 계산 모듈 (OHLC 컬럼 소문자 사용: "close", etc.)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_boll(df: pd.DataFrame, lookback: int, stddev_mult: float) -> pd.DataFrame:
    """
    볼린저 밴드(Bollinger Bands)를 계산한다.
    df["close"]가 필요.
    반환 예시 컬럼:
     - BOLL_L_{lookback}_{stddev_mult} (Lower Band)
     - BOLL_M_{lookback}_{stddev_mult} (Middle Band)
     - BOLL_U_{lookback}_{stddev_mult} (Upper Band)
    """
    bb_df = ta.bbands(
        close=df["close"],
        length=lookback,
        std=stddev_mult
    )
    # pandas_ta.bb 에는 BBL, BBM, BBU, BBB, BBP 등이 반환됨
    if bb_df is None or bb_df.empty:
        return pd.DataFrame({
            f"BOLL_L_{lookback}_{stddev_mult}": [np.nan] * len(df),
            f"BOLL_M_{lookback}_{stddev_mult}": [np.nan] * len(df),
            f"BOLL_U_{lookback}_{stddev_mult}": [np.nan] * len(df)
        }, index=df.index)

    rename_map = {}
    for c in bb_df.columns:
        c_up = c.upper()
        if "BBL" in c_up:
            rename_map[c] = f"BOLL_L_{lookback}_{stddev_mult}"
        elif "BBM" in c_up:
            rename_map[c] = f"BOLL_M_{lookback}_{stddev_mult}"
        elif "BBU" in c_up:
            rename_map[c] = f"BOLL_U_{lookback}_{stddev_mult}"
        else:
            # BBB, BBP 등은 사용하지 않을 경우 넘어감
            rename_map[c] = c

    bb_df.rename(columns=rename_map, inplace=True)
    result_cols = [col for col in bb_df.columns if col in rename_map.values()]
    return bb_df[result_cols]
