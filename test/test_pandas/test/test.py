import os

from binance.um_futures import UMFutures
from dotenv import load_dotenv

load_dotenv()
access = os.getenv("BINANCE_ACCESS_KEY")
secret = os.getenv("BINANCE_SECRET_KEY")
client = UMFutures(access, secret)
klines = client.klines("BTCUSDT", interval="1m", limit=2)
print(klines)