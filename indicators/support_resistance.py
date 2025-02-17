# support_resistance.py

import pandas as pd

def support_resistance_signal(df, window):
    df['roll_min'] = df['close'].rolling(window).min()
    df['roll_max'] = df['close'].rolling(window).max()
    df['roll_min_prev'] = df['roll_min'].shift(1)
    df['roll_max_prev'] = df['roll_max'].shift(1)

    signals = []
    for i in range(len(df)):
        c = df['close'].iloc[i]
        mn_p = df['roll_min_prev'].iloc[i]
        mx_p = df['roll_max_prev'].iloc[i]
        if pd.isna(mn_p) or pd.isna(mx_p):
            signals.append(0)
        else:
            cross_up = (c > mx_p)
            cross_down = (c < mn_p)
            if cross_up:
                signals.append(1)
            elif cross_down:
                signals.append(-1)
            else:
                signals.append(0)
    df['signal'] = signals
    return df.drop(['roll_min','roll_max','roll_min_prev','roll_max_prev'], axis=1)
