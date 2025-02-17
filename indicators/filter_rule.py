# rsi.py

import pandas as pd

def rsi_signal(df, length, overbought, oversold):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # RSI가 oversold 아래→위로 돌파 시 +1, overbought 위→아래 돌파 시 -1
    df['rsi_prev'] = df['rsi'].shift(1)
    signals = []
    for i in range(len(df)):
        curr = df['rsi'].iloc[i]
        prev = df['rsi_prev'].iloc[i] if not pd.isna(df['rsi_prev'].iloc[i]) else None
        if pd.isna(curr) or prev is None:
            signals.append(0)
        else:
            cross_up = (prev <= oversold) and (curr > oversold)
            cross_down = (prev >= overbought) and (curr < overbought)
            if cross_up:
                signals.append(1)
            elif cross_down:
                signals.append(-1)
            else:
                signals.append(0)
    df['signal'] = signals
    return df.drop(['rsi','rsi_prev'], axis=1)
