# signal_generation.py

import pandas as pd
import numpy as np


def generate_signals_from_rule(df: pd.DataFrame, rule: dict) -> pd.Series:
    """
    df : OHLCV DataFrame (index=DateTime, columns=['open','high','low','close','volume'])
    rule: {'rule_type': 'MA'/'RSI'/'S&R'/'Filter'/'CB'/'OBV', ... 다양한 파라미터 ...}

    반환: 시그널(1=매수/보유, 0=현금)의 시계열(pd.Series).
    """
    rule_type = str(rule.get("rule_type", "")).upper().strip()
    if rule_type == "MA":
        return _signals_ma(df, rule)
    elif rule_type == "RSI":
        return _signals_rsi(df, rule)
    elif rule_type == "S&R":
        return _signals_sr(df, rule)
    elif rule_type == "FILTER":
        filter_set = str(rule.get("filter_set", "A")).upper()
        if filter_set == "A":
            return _signals_filter_a(df, rule)
        elif filter_set == "B":
            return _signals_filter_b(df, rule)
        else:
            raise ValueError(f"Unknown filter_set: {filter_set}")
    elif rule_type == "CB":
        return _signals_cb(df, rule)
    elif rule_type == "OBV":
        return _signals_obv(df, rule)
    else:
        raise ValueError(f"Unknown rule_type: {rule_type}")


# =============================
# 공통 d, k 처리 함수 (인/아웃)
# =============================
def apply_delay(raw_signal: pd.Series, d: int) -> pd.Series:
    """
    d>0이면, d bar 연속 1이어야 최종 1로 확정. 0이면 그대로.
    """
    if d <= 0:
        return raw_signal.copy()
    roll_sum = raw_signal.rolling(d).sum()
    return (roll_sum == d).astype(int)


def apply_holding_period(raw_signal: pd.Series, k) -> pd.Series:
    """
    k이 정수면, 매수확정 후 k bar 동안 1 유지 후 0.
    k='∞'이면 다음에 raw_signal=0 뜰 때까지 1 유지.
    """
    signals = pd.Series(data=0, index=raw_signal.index, dtype=int)
    if str(k) == '∞':
        holding = False
        for idx in raw_signal.index:
            if not holding:
                if raw_signal[idx] == 1:
                    holding = True
                    signals[idx] = 1
                else:
                    signals[idx] = 0
            else:
                # 보유중
                if raw_signal[idx] == 0:
                    holding = False
                    signals[idx] = 0
                else:
                    signals[idx] = 1
    else:
        k_int = int(k)
        hold_flag = False
        hold_count = 0
        for idx in raw_signal.index:
            if not hold_flag:
                if raw_signal[idx] == 1:
                    hold_flag = True
                    hold_count = 1
                    signals[idx] = 1
                else:
                    signals[idx] = 0
            else:
                signals[idx] = 1
                hold_count += 1
                if hold_count >= k_int:
                    hold_flag = False
                    hold_count = 0
    return signals


# =============================
# 1) MA (p,q,x,d,k)
# =============================
def _signals_ma(df: pd.DataFrame, rule: dict) -> pd.Series:
    p = rule["p"]
    q = rule["q"]
    x = rule.get("x", 0.0)
    d = rule.get("d", 0)
    k = rule.get("k", 6)

    df["_ma_p"] = df["close"].rolling(p).mean()
    df["_ma_q"] = df["close"].rolling(q).mean()
    df["_ma_p_adj"] = df["_ma_p"] * (1 + x)  # 퍼센트 밴드

    raw_signal = (df["_ma_p_adj"] > df["_ma_q"]).astype(int)
    delayed = apply_delay(raw_signal, d)
    signals = apply_holding_period(delayed, k)

    df.drop(columns=["_ma_p", "_ma_q", "_ma_p_adj"], inplace=True)
    return signals


# =============================
# 2) RSI (h, v, d, k)
# =============================
def _signals_rsi(df: pd.DataFrame, rule: dict) -> pd.Series:
    h = rule["h"]
    v = rule["v"]
    d = rule.get("d", 0)
    k = rule.get("k", 6)

    df["_delta"] = df["close"].diff()
    df["_up"] = df["_delta"].where(df["_delta"] > 0, 0.0)
    df["_down"] = -df["_delta"].where(df["_delta"] < 0, 0.0)
    roll_up = df["_up"].rolling(h).mean()
    roll_down = df["_down"].rolling(h).mean()
    rs = roll_up / (roll_down + 1e-9)
    df["_rsi"] = 100 - (100 / (1 + rs))

    raw_signal = (df["_rsi"] < (50 - v)).astype(int)
    delayed = apply_delay(raw_signal, d)
    signals = apply_holding_period(delayed, k)

    df.drop(columns=["_delta", "_up", "_down", "_rsi"], inplace=True)
    return signals


# =============================
# 3) S&R (j, x, d, k)
# =============================
def _signals_sr(df: pd.DataFrame, rule: dict) -> pd.Series:
    j = rule["j"]
    x = rule.get("x", 0.0)
    d = rule.get("d", 0)
    k = rule.get("k", 6)

    df["_sr_high"] = df["high"].rolling(j).max()
    df["_sr_low"] = df["low"].rolling(j).min()

    df["_sr_high_adj"] = df["_sr_high"] * (1 + x)
    df["_sr_low_adj"] = df["_sr_low"] * (1 - x)

    raw_signal = pd.Series(0, index=df.index, dtype=int)
    cond_buy = (df["close"] > df["_sr_high_adj"])
    cond_sell = (df["close"] < df["_sr_low_adj"])
    raw_signal[cond_buy] = 1
    raw_signal[cond_sell] = 0

    delayed = apply_delay(raw_signal, d)
    signals = apply_holding_period(delayed, k)

    df.drop(columns=["_sr_high", "_sr_low", "_sr_high_adj", "_sr_low_adj"], inplace=True)
    return signals


# =============================
# 4) Filter (A/B)
# =============================
def _signals_filter_a(df: pd.DataFrame, rule: dict) -> pd.Series:
    """
    Filter A (논문에 따르면 j, x, d, k 등)
    j= {1,2,6,12,24}, x= {0.05,0.1,0.5,1,5,10,20}, d= {0,1,2,3,4}, k= {6,12,18,20,24,'∞','kX','kY','kZ'}
    여기선 인자로 넘어온 j,x,d,k만 처리
    """
    j = rule["j"]
    x = rule.get("x", 0.05)
    d = rule.get("d", 0)
    k = rule.get("k", 6)

    df["_f_min"] = df["low"].rolling(j).min()
    df["_f_max"] = df["high"].rolling(j).max()
    raw_signal = pd.Series(0, index=df.index, dtype=int)

    # 논문 예시: close > f_min*(1+x) => 1(매수), else 0
    cond_buy = (df["close"] > df["_f_min"] * (1 + x))
    # 고점 돌파 등 추가 조건 가능. 여기서는 단순화
    raw_signal[cond_buy] = 1

    delayed = apply_delay(raw_signal, d)
    signals = apply_holding_period(delayed, k)

    df.drop(columns=["_f_min", "_f_max"], inplace=True)
    return signals


def _signals_filter_b(df: pd.DataFrame, rule: dict) -> pd.Series:
    """
    Filter B (x>=y, dx, dy 등)
    ex) j={1,2,6,12,24}, x,y={0.05,...}, d_x={0..5}, d_y={0..4}, k={6,12,18,20,24,'∞'} 등
    """
    j = rule["j"]
    x = rule.get("x", 0.05)
    y = rule.get("y", 0.05)
    d_x = rule.get("d_x", 0)
    d_y = rule.get("d_y", 0)
    k = rule.get("k", 6)

    # j기간 최저/최고
    df["_fB_min"] = df["low"].rolling(j).min()
    df["_fB_max"] = df["high"].rolling(j).max()

    raw_signal = pd.Series(0, index=df.index, dtype=int)

    # 예: x>=y 조건. 만약 rule 주어진 x<y이면 사용자가 잘못 지정
    if x < y:
        # 강제로 swap하거나, 0신호로 처리
        pass

    # buy조건: close > fB_min*(1 + x) d_x일 연속
    # sell조건: close < fB_max*(1 - y) d_y일 연속 => 0
    # 간단히 (close>fB_min*(1+x))=>1, (close<fB_max*(1-y))=>0
    cond_buy = (df["close"] > df["_fB_min"] * (1 + x))
    cond_sell = (df["close"] < df["_fB_max"] * (1 - y))

    # d_x 적용: d_x일 연속 cond_buy => 최종1
    buy_sig = _apply_subdelay(cond_buy, d_x)
    # d_y 적용: cond_sell => 0
    sell_sig = _apply_subdelay(cond_sell, d_y)

    # 한 번 buy 발생하면 1, sell 발생하면 0
    # 여기서는 “buy_sig=1이면 1, sell_sig=1이면 0” 우선순위 later
    # 간단히: raw_signal[buy_sig] = 1, raw_signal[sell_sig] = 0
    # 시점별로 buy/sell 동시 발생 가능성은 논문에서도 특별케이스
    for i in df.index:
        if buy_sig[i] == 1:
            raw_signal[i] = 1
        if sell_sig[i] == 1:
            raw_signal[i] = 0

    # k 보유
    signals = apply_holding_period(raw_signal, k)

    df.drop(columns=["_fB_min", "_fB_max"], inplace=True)
    return signals


def _apply_subdelay(cond_series: pd.Series, d_sub: int) -> pd.Series:
    """
    cond_series: True/False
    d_sub: cond가 d_sub bar 연속 True면 최종 True
    """
    out = pd.Series(False, index=cond_series.index)
    if d_sub <= 0:
        return cond_series.astype(int)
    rolling_sum = cond_series.astype(int).rolling(d_sub).sum()
    out[rolling_sum == d_sub] = True
    return out.astype(int)


# =============================
# 5) CB (j,c,x,d,k)
# =============================
def _signals_cb(df: pd.DataFrame, rule: dict) -> pd.Series:
    j = rule["j"]
    c = rule.get("c", 1.0)
    x = rule.get("x", 0.05)
    d = rule.get("d", 0)
    k = rule.get("k", 6)

    df["_cb_high"] = df["high"].rolling(j).max()
    df["_cb_low"] = df["low"].rolling(j).min()
    df["_cb_size"] = (df["_cb_high"] - df["_cb_low"]) / (df["_cb_low"].abs().replace(0, 1e-9))

    # cond_ch_break: 채널폭< c & close> high*(1+x)
    cond_ch_break = (df["_cb_size"] < c) & (df["close"] > df["_cb_high"] * (1 + x))

    raw_signal = cond_ch_break.astype(int)
    delayed = apply_delay(raw_signal, d)
    signals = apply_holding_period(delayed, k)

    df.drop(columns=["_cb_high", "_cb_low", "_cb_size"], inplace=True)
    return signals


# =============================
# 6) OBV (p,q,x,d,k)
# =============================
def _signals_obv(df: pd.DataFrame, rule: dict) -> pd.Series:
    p = rule["p"]
    q = rule["q"]
    x = rule.get("x", 0.0)
    d = rule.get("d", 0)
    k = rule.get("k", 6)

    df["_obv_delta"] = np.sign(df["close"].diff()) * df["volume"]
    df["_obv"] = df["_obv_delta"].fillna(0).cumsum()

    df["_obv_ma_p"] = df["_obv"].rolling(p).mean()
    df["_obv_ma_q"] = df["_obv"].rolling(q).mean()

    df["_obv_ma_p_adj"] = df["_obv_ma_p"] * (1 + x)
    raw_signal = (df["_obv_ma_p_adj"] > df["_obv_ma_q"]).astype(int)

    delayed = apply_delay(raw_signal, d)
    signals = apply_holding_period(delayed, k)

    df.drop(columns=["_obv_delta", "_obv", "_obv_ma_p", "_obv_ma_q", "_obv_ma_p_adj"], inplace=True)
    return signals
