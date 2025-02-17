# inout_strategy.py

def apply_inout_strategy(df):
    df['position'] = df['signal'].apply(lambda s: 1 if s > 0 else 0)
    return df
