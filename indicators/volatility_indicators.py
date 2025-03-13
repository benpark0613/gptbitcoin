# gptbitcoin/indicators/volatility_indicators.py
# 변동성 기반 보조지표 계산 모듈 (소문자 컬럼명 사용: "close" 등)

import pandas as pd
import numpy as np
import pandas_ta as ta


def calc_boll(df: pd.DataFrame, lookback: int, stddev_mult: float) -> pd.DataFrame:
    """
    볼린저 밴드(Bollinger Bands) 계산.
    - df["close"]가 있어야 함.
    - 반환 예시 컬럼:
      - boll_mid_{lookback}_{stddev_mult}
      - boll_upper_{lookback}_{stddev_mult}
      - boll_lower_{lookback}_{stddev_mult}
    """
    bb_df = ta.bbands(
        close=df["close"],
        length=lookback,
        std=stddev_mult
    )
    # pandas_ta.bbands → BBL, BBM, BBU, BBB, BBP 등 반환 가능
    if bb_df is None or bb_df.empty:
        return pd.DataFrame({
            f"boll_mid_{lookback}_{stddev_mult}": [np.nan] * len(df),
            f"boll_upper_{lookback}_{stddev_mult}": [np.nan] * len(df),
            f"boll_lower_{lookback}_{stddev_mult}": [np.nan] * len(df)
        }, index=df.index)

    rename_map = {}
    for c in bb_df.columns:
        c_up = c.upper()
        if "BBM" in c_up:
            rename_map[c] = f"boll_mid_{lookback}_{stddev_mult}"
        elif "BBU" in c_up:
            rename_map[c] = f"boll_upper_{lookback}_{stddev_mult}"
        elif "BBL" in c_up:
            rename_map[c] = f"boll_lower_{lookback}_{stddev_mult}"
        else:
            # BBB, BBP 등은 사용 안 하면 그냥 냅둠
            rename_map[c] = c

    bb_df.rename(columns=rename_map, inplace=True)

    # 우리가 필요한 컬럼만 추출 (mid, upper, lower)
    needed_cols = [
        f"boll_mid_{lookback}_{stddev_mult}",
        f"boll_upper_{lookback}_{stddev_mult}",
        f"boll_lower_{lookback}_{stddev_mult}"
    ]
    for col in needed_cols:
        if col not in bb_df.columns:
            bb_df[col] = np.nan

    return bb_df[needed_cols]
