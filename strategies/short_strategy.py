# short_strategy.py

def apply_short_strategy(df):
    def map_signal(s):
        if s > 0:
            return 1  # 매수
        elif s < 0:
            return -1  # 숏
        else:
            return 0    # 중립
    df['position'] = df['signal'].apply(map_signal)
    return df
