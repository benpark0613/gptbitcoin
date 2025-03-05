# gptbitcoin/signals/generate_signals.py

import pandas as pd
from settings import config
from indicators.ma_cross import ma_cross
from indicators.rsi import rsi_signal
from indicators.support_resistance import support_resistance
from indicators.filter_rule import filter_rule
from indicators.channel_breakout import channel_breakout
from indicators.obv import obv_signal
from strategies.short_strategy import short_strategy
from strategies.inout_strategy import inout_strategy

def generate_signals_func(df: pd.DataFrame, combo: dict) -> pd.Series:
    ind = combo["indicator"]

    if ind == "MA":
        raw_sig = ma_cross(
            df,
            short_period=combo["short_period"],
            long_period=combo["long_period"],
            band_filter=combo["band_filter"],
            delay_filter=combo["delay_filter"],
            holding_period=combo["holding_period"]
        )
    elif ind == "RSI":
        raw_sig = rsi_signal(
            df,
            length=combo["length"],
            overbought=combo["overbought"],
            oversold=combo["oversold"],
            band_filter=combo["band_filter"],
            delay_filter=combo["delay_filter"],
            holding_period=combo["holding_period"]
        )
    elif ind == "S&R":
        raw_sig = support_resistance(
            df,
            window=combo["window"],
            band_filter=combo["band_filter"],
            delay_filter=combo["delay_filter"],
            holding_period=combo["holding_period"]
        )
    elif ind == "Filter":
        raw_sig = filter_rule(
            df,
            window=combo["window"],
            x=combo["x"],
            y=combo["y"],
            band_filter=combo["band_filter"],
            delay_filter=combo["delay_filter"],
            holding_period=combo["holding_period"]
        )
    elif ind == "CB":
        raw_sig = channel_breakout(
            df,
            window=combo["window"],
            c_value=combo["c_value"],
            band_filter=combo["band_filter"],
            delay_filter=combo["delay_filter"],
            holding_period=combo["holding_period"]
        )
    elif ind == "OBV":
        raw_sig = obv_signal(
            df,
            short_period=combo["short_period"],
            long_period=combo["long_period"],
            band_filter=combo["band_filter"],
            delay_filter=combo["delay_filter"],
            holding_period=combo["holding_period"]
        )
    else:
        return pd.Series([0]*len(df), index=df.index)

    if config.ALLOW_SHORT:
        return short_strategy(raw_sig)
    else:
        return inout_strategy(raw_sig)
