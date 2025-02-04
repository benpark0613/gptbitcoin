import os
import pandas as pd
import numpy as np
from binance.client import Client
from dotenv import load_dotenv
import itertools

# 환경 변수 로드
load_dotenv()
access = os.getenv("BINANCE_ACCESS_KEY")
secret = os.getenv("BINANCE_SECRET_KEY")

# Binance API 연결
client = Client(access, secret)

# 설정 값
symbol = 'BTCUSDT'
interval = "30m"
limit = 300  # 가져올 캔들 개수

# 바이낸스 선물 데이터 가져오기
klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)

# 데이터프레임 변환
df = pd.DataFrame(klines, columns=[
    "timestamp", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
])

# 필요한 데이터 변환
df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
df["close"] = df["close"].astype(float)

# pandas로 EMA 계산
df["EMA_9"] = df["close"].ewm(span=9, adjust=False).mean()
df["EMA_21"] = df["close"].ewm(span=21, adjust=False).mean()
df["EMA_200"] = df["close"].ewm(span=200, adjust=False).mean()

# 매매 시그널 생성
df["long_entry"] = (df["close"] > df["EMA_200"]) & (df["EMA_9"] > df["EMA_21"])
df["short_entry"] = (df["close"] < df["EMA_200"]) & (df["EMA_21"] > df["EMA_9"])

# 초기 자본 설정
initial_balance = 1000
balance = initial_balance
position = None
entry_price = None
returns = []

# 상세 거래 기록 저장
detail_results = []

# 트레이딩 시뮬레이션
for i in range(1, len(df)):
    if df.loc[i, "long_entry"] and position is None:
        position = "long"
        entry_price = df.loc[i, "close"]
    elif df.loc[i, "short_entry"] and position is None:
        position = "short"
        entry_price = df.loc[i, "close"]
    elif position == "long" and df.loc[i, "short_entry"]:
        profit = (df.loc[i, "close"] - entry_price) / entry_price
        returns.append(profit)
        position = None
    elif position == "short" and df.loc[i, "long_entry"]:
        profit = (entry_price - df.loc[i, "close"]) / entry_price
        returns.append(profit)
        position = None

# 성과 지표 계산 (이제 변수들이 정의됨)
win_rate = sum(1 for r in returns if r > 0) / len(returns) if returns else 0
risk_reward_ratio = np.mean([r for r in returns if r > 0]) / abs(np.mean([r for r in returns if r < 0])) if returns else 0
drawdown = min(returns) if returns else 0
sharpe_ratio = np.mean(returns) / np.std(returns) if np.std(returns) != 0 else 0
expected_return = np.mean(returns) if returns else 0
max_consecutive_losses = max(sum(1 for _ in group) for key, group in itertools.groupby(returns, key=lambda x: x < 0) if key) if returns else 0
trade_frequency = len(returns)
final_balance = initial_balance * (1 + sum(returns))

# 상세 거래 기록 저장 (이제 변수를 참조 가능)
for i in range(1, len(df)):
    detail_results.append([
        df.loc[i, "timestamp"], df.loc[i, "open"], df.loc[i, "high"], df.loc[i, "low"], df.loc[i, "close"],
        df.loc[i, "volume"], df.loc[i, "close_time"], df.loc[i, "quote_asset_volume"], df.loc[i, "number_of_trades"],
        df.loc[i, "taker_buy_base_asset_volume"], df.loc[i, "taker_buy_quote_asset_volume"], df.loc[i, "ignore"],
        df.loc[i, "EMA_9"], df.loc[i, "EMA_21"], df.loc[i, "EMA_200"], df.loc[i, "long_entry"], df.loc[i, "short_entry"],
        win_rate, risk_reward_ratio, drawdown, sharpe_ratio, expected_return, max_consecutive_losses, trade_frequency, final_balance
    ])

# 결과 데이터프레임 생성
results_df = pd.DataFrame({
    "Win Rate": [win_rate],
    "Risk-Reward Ratio": [risk_reward_ratio],
    "Drawdown": [drawdown],
    "Sharpe Ratio": [sharpe_ratio],
    "Expected Return": [expected_return],
    "Maximum Consecutive Losses": [max_consecutive_losses],
    "Trade Frequency": [trade_frequency],
    "Final Balance": [final_balance]
})

# CSV 저장 (backtest 폴더 내 저장)
os.makedirs("backtest", exist_ok=True)
results_df.to_csv("backtest/trading_results.csv", index=False)

# 상세 거래 기록 저장
detail_df = pd.DataFrame(detail_results, columns=[
    "timestamp", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore", "EMA_9", "EMA_21", "EMA_200", "long_entry", "short_entry",
    "Win Rate", "Risk-Reward Ratio", "Drawdown", "Sharpe Ratio", "Expected Return", "Maximum Consecutive Losses", "Trade Frequency", "Final Balance"
])
detail_df.to_csv("backtest/detail_results.csv", index=False)

# 결과 출력
print(results_df)
