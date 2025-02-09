# test/add_indicators/price_action/price_action_indicator.py

import pandas as pd


def add_price_action_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    전통적 '스윙 피벗'을 이용한 HH, HL, LH, LL 시그널을 df['signal'] 컬럼에 추가합니다.

    - Pivot High(국소 고점): 현재 high가 이전/다음 high보다 모두 클 때
    - Pivot Low(국소 저점):  현재 low가  이전/다음 low보다 모두 작을 때
    - 마지막 pivot high와 비교해 더 높으면 HH, 더 낮으면 LH
    - 마지막 pivot low 와 비교해 더 높으면 HL, 더 낮으면 LL

    ※ 첫 번째, 마지막 인덱스는 이전/다음 봉이 없으므로 pivot 계산에서 제외됩니다.
    """

    # high, low가 없으면 경고하고 종료
    if 'high' not in df.columns or 'low' not in df.columns:
        print("[WARN] 'high' 또는 'low' 컬럼이 없습니다.")
        df['signal'] = None
        return df

    # 시그널 컬럼 초기화
    df['signal'] = None

    # 마지막 pivot 값 기억용 (None이면 아직 pivot이 한 번도 나오지 않은 상태)
    last_pivot_high_value = None
    last_pivot_low_value = None

    # 스윙 피벗 계산을 위해, 1번째 ~ (len-2)번째까지 반복
    # (i-1, i, i+1)를 비교하기 때문
    for i in range(1, len(df) - 1):
        prev_high = df.loc[i - 1, 'high']
        curr_high = df.loc[i, 'high']
        next_high = df.loc[i + 1, 'high']

        prev_low = df.loc[i - 1, 'low']
        curr_low = df.loc[i, 'low']
        next_low = df.loc[i + 1, 'low']

        # 국소 고점(현재 high가 이전/다음보다 큰 경우)
        if curr_high > prev_high and curr_high > next_high:
            # 이전 pivot high와 비교
            if last_pivot_high_value is not None:
                if curr_high > last_pivot_high_value:
                    df.loc[i, 'signal'] = 'HH'  # Higher High
                else:
                    df.loc[i, 'signal'] = 'LH'  # Lower High
            # pivot high 갱신
            last_pivot_high_value = curr_high

        # 국소 저점(현재 low가 이전/다음보다 작은 경우)
        elif curr_low < prev_low and curr_low < next_low:
            # 이전 pivot low와 비교
            if last_pivot_low_value is not None:
                if curr_low > last_pivot_low_value:
                    df.loc[i, 'signal'] = 'HL'  # Higher Low
                else:
                    df.loc[i, 'signal'] = 'LL'  # Lower Low
            # pivot low 갱신
            last_pivot_low_value = curr_low

    return df


# 테스트 코드 예시
if __name__ == "__main__":
    data = {
        'open': [100, 102, 103, 105, 104, 102, 101],
        'high': [102, 105, 106, 107, 106, 103, 102],
        'low': [99, 100, 101, 103, 102, 100, 99],
        'close': [101, 104, 105, 106, 103, 102, 100],
    }

    df_test = pd.DataFrame(data)
    print("=== 원본 테스트 DataFrame ===")
    print(df_test)

    df_with_signals = add_price_action_signals(df_test.copy())
    print("\n=== Price Action Signals 적용 결과 ===")
    print(df_with_signals[['high', 'low', 'signal']])
