# obv.py

import pandas as pd

def obv_signal(df, short_p, long_p):
    obv = [0]
    for i in range(1, len(df)):
        if df['close'].iloc[i] > df['close'].iloc[i-1]:
            obv.append(obv[-1] + df['volume'].iloc[i])
        elif df['close'].iloc[i] < df['close'].iloc[i-1]:
            obv.append(obv[-1] - df['volume'].iloc[i])
        else:
            obv.append(obv[-1])
    df['obv'] = obv
    df['obv_sma_short'] = df['obv'].rolling(short_p).mean()
    df['obv_sma_long'] = df['obv'].rolling(long_p).mean()

    # 크로스 발생 로직
    df['obv_sma_short_prev'] = df['obv_sma_short'].shift(1)
    df['obv_sma_long_prev'] = df['obv_sma_long'].shift(1)

    signals = []
    for i in range(len(df)):
        s_now = df['obv_sma_short'].iloc[i]
        l_now = df['obv_sma_long'].iloc[i]
        s_prev = df['obv_sma_short_prev'].iloc[i]
        l_prev = df['obv_sma_long_prev'].iloc[i]
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
    return df.drop(['obv','obv_sma_short','obv_sma_long',
                    'obv_sma_short_prev','obv_sma_long_prev'], axis=1)
