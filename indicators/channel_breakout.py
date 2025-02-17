# channel_breakout.py

import pandas as pd

def channel_breakout_signal(df, window, c):
    df['roll_high'] = df['high'].rolling(window).max()
    df['roll_low'] = df['low'].rolling(window).min()
    df['roll_high_prev'] = df['roll_high'].shift(1)
    df['roll_low_prev'] = df['roll_low'].shift(1)

    signals = []
    for i in range(len(df)):
        c_close = df['close'].iloc[i]
        hi_p = df['roll_high_prev'].iloc[i]
        lo_p = df['roll_low_prev'].iloc[i]
        if pd.isna(hi_p) or pd.isna(lo_p):
            signals.append(0)
        else:
            # 채널 상단 = hi_p * (1 + c), 하단 = lo_p * (1 - c)
            up_thr = hi_p * (1 + c)
            dn_thr = lo_p * (1 - c)
            cross_up = (c_close > up_thr)
            cross_down = (c_close < dn_thr)
            if cross_up:
                signals.append(1)
            elif cross_down:
                signals.append(-1)
            else:
                signals.append(0)
    df['signal'] = signals
    return df.drop(['roll_high','roll_low','roll_high_prev','roll_low_prev'], axis=1)
