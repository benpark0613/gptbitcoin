# ma_cross.py

import pandas as pd

def ma_cross_signal(df, short_period, long_period):
    df['ma_short'] = df['close'].rolling(short_period).mean()
    df['ma_long'] = df['close'].rolling(long_period).mean()

    # 이전 값 (shift(1))과 현재 값을 비교하여,
    # 단기MA가 장기MA를 아래→위로 교차하면 +1, 위→아래로 교차하면 -1, 유지면 0
    df['ma_short_prev'] = df['ma_short'].shift(1)
    df['ma_long_prev'] = df['ma_long'].shift(1)

    signals = []
    for i in range(len(df)):
        s_now = df['ma_short'].iloc[i]
        l_now = df['ma_long'].iloc[i]
        s_prev = df['ma_short_prev'].iloc[i]
        l_prev = df['ma_long_prev'].iloc[i]

        if pd.isna(s_now) or pd.isna(l_now) or pd.isna(s_prev) or pd.isna(l_prev):
            signals.append(0)
        else:
            cross_up = (s_prev <= l_prev) and (s_now > l_now)
            cross_down = (s_prev >= l_prev) and (s_now < l_now)

            if cross_up:
                signals.append(1)
            elif cross_down:
                signals.append(-1)
            else:
                signals.append(0)

    df['signal'] = signals
    return df.drop(['ma_short','ma_long','ma_short_prev','ma_long_prev'], axis=1)
