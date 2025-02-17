# config.py

SYMBOL = "BTCUSDT"
FEE_RATE = 0.0004
BID_ASK_SPREAD = 0.0005
MARGIN_INTEREST = 0.0001
LEVERAGE = 1
ALLOW_SHORT = True
INIT_CAPITAL = 100000

START_DATE = "2024-01-01"
END_DATE   = "2024-12-31"

TRAIN_RATIO = 0.7
SLIPPAGE_RATE = 0.0002

DATA_PATH    = "./data/"
RESULTS_PATH = "./results/"

POSSIBLE_TIMEFRAMES = ["5m","15m","1h","4h","1d","1w","1M"]
TIMEFRAMES = ["4h"]

INDICATOR_PARAMS = {
    "MA": {
        "short_periods": [5,10,20,50,100],
        "long_periods":  [50,100,150,200,300],
        "band_filter":   [0, 0.01, 0.02, 0.05],
        "delay_filter":  [0,1,2,3],
        "holding_period":[0,3,5,10,30]
    },
    "RSI": {
        "lengths":       [7,14,21,28],
        "overbought_values": [65,70,75,80],
        "oversold_values":   [20,25,30,35],
        "band_filter":   [0, 0.01, 0.02],
        "delay_filter":  [0,1,2],
        "holding_period":[0,3,5,10,30]
    },
    "Support_Resistance": {
        "windows":       [5,10,20,30],
        "band_filter":   [0, 0.01, 0.02, 0.05],
        "delay_filter":  [0,1,2,3],
        "holding_period":[0,3,5,10]
    },
    "Filter_Rule": {
        "windows":       [5,10,20],
        "x_values":      [0.05,0.1,0.15],
        "y_values":      [0.05,0.1,0.15],
        "band_filter":   [0, 0.01, 0.02],
        "delay_filter":  [0,1,2],
        "holding_period":[0,3,5,10]
    },
    "Channel_Breakout": {
        "windows":       [10,20,30,60],
        "c_values":      [0.05,0.1,0.2],
        "band_filter":   [0, 0.01, 0.02],
        "delay_filter":  [0,1,2],
        "holding_period":[0,3,5,10]
    },
    "OBV": {
        "short_periods": [5,10,20],
        "long_periods":  [50,100,150,200],
        "band_filter":   [0, 0.01, 0.02],
        "delay_filter":  [0,1,2],
        "holding_period":[0,3,5,10]
    }
}
