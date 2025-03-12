# test.py
"""
pandas-ta의 ichimoku, psar 계산 결과를 직접 확인하는 테스트 스크립트.
 - dummy df(OHLC) 생성
 - ta.ichimoku, ta.psar 호출
 - 결과(tuples/dataframe)에 어떤 컬럼이 있는지 print
"""

import pandas as pd
import numpy as np
import pandas_ta as ta

def make_dummy_ohlc_data(size: int = 100) -> pd.DataFrame:
    """
    size 길이의 가상 데이터프레임을 생성한다.
    - open, high, low, close, volume 칼럼
    """
    rng = np.arange(size)
    df = pd.DataFrame({
        "open":  1000 + np.sin(rng) * 10,
        "high":  1000 + np.sin(rng) * 10 + 5,  # high은 open보다 약간 높게
        "low":   1000 + np.sin(rng) * 10 - 5,  # low는 open보다 약간 낮게
        "close": 1000 + np.sin(rng) * 10 + np.random.randn(size) * 2,
        "volume": np.random.randint(100, 1000, size=size)
    })
    return df

def test_ichimoku(df: pd.DataFrame, tenkan=9, kijun=26, span_b=52):
    """
    pandas-ta의 ichimoku 호출 및 결과 확인
    """
    print(f"\n[TEST] Ichimoku(tenkan={tenkan}, kijun={kijun}, span_b={span_b})")

    # pandas-ta 는 통상적으로 (conversion/tenkan, base/kijun, spanA, spanB, lagging) 의 튜플/df를 반환
    ichi_res = ta.ichimoku(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        tenkan=tenkan,
        kijun=kijun,
        senkou=span_b,
        lookahead=False
    )

    print("[ichimoku] type of returned value:", type(ichi_res))
    if isinstance(ichi_res, tuple):
        # tuple이면 각 element를 순회
        for i, part_df in enumerate(ichi_res, start=1):
            print(f"  -> Part {i} columns: {list(part_df.columns)}")
            print(part_df.tail(3))  # 마지막 3행만 출력
    else:
        # DataFrame이면 그냥 컬럼과 tail()을 출력
        print("[ichimoku] single DataFrame columns:", list(ichi_res.columns))
        print(ichi_res.tail(3))


def test_psar(df: pd.DataFrame, af=0.02, max_af=0.2):
    """
    pandas-ta의 psar 호출 및 결과 확인
    """
    print(f"\n[TEST] PSAR(af={af}, max_af={max_af})")
    psar_res = ta.psar(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        af=af,
        max_af=max_af
    )
    print("[psar] type of returned value:", type(psar_res))
    if psar_res is None or psar_res.empty:
        print("  -> Returned None or empty DataFrame!")
    else:
        print("  -> columns:", list(psar_res.columns))
        print(psar_res.tail(3))


def main():
    df_dummy = make_dummy_ohlc_data(120)  # 120개 봉
    print("[INFO] Dummy OHLC data (tail):\n", df_dummy.tail())

    # Ichimoku 테스트
    test_ichimoku(df_dummy, tenkan=9, kijun=26, span_b=52)

    # PSAR 테스트
    test_psar(df_dummy, af=0.01, max_af=0.2)


if __name__ == "__main__":
    main()
