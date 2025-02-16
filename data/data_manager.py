# data/data_manager.py

import os
import pandas as pd
from binance.client import Client
from data.collector import fetch_futures_ohlcv
from utils.helper import save_csv, prepare_futures_df

class DataManager:
    def __init__(self, client, symbol, intervals, start_date, end_date, save_folder, warmup_period=26):
        self.client = client
        self.symbol = symbol
        self.intervals = intervals
        self.save_folder = save_folder
        self.warmup_period = warmup_period

        # start_date, end_date를 Timestamp로 변환
        self.requested_start = pd.to_datetime(start_date)
        self.requested_end   = pd.to_datetime(end_date)

        # 바이낸스 API에 전달할 ms
        self.start_ms = int(self.requested_start.timestamp()*1000)
        self.end_ms   = int(self.requested_end.timestamp()*1000)

    def _interval_to_ms(self, interval: str) -> int:
        num = int("".join(filter(str.isdigit, interval)))
        unit= "".join(filter(str.isalpha, interval)).lower()
        if unit=='m':
            return num*60*1000
        elif unit=='h':
            return num*3600*1000
        elif unit=='d':
            return num*86400*1000
        else:
            raise ValueError(f"Unsupported interval: {interval}")

    def data_exists_and_complete(self, interval):
        """
        CSV 파일이 존재하고, open_time_dt의 min/max가
        [self.requested_start, self.requested_end] 범위를 커버하는지 확인
        """
        file_name= f"{self.symbol}_{interval}.csv"
        file_path= os.path.join(self.save_folder, file_name)
        if not os.path.exists(file_path):
            return False, file_path
        try:
            df= pd.read_csv(file_path, parse_dates=["open_time_dt"])
            existing_min= df["open_time_dt"].min()
            existing_max= df["open_time_dt"].max()

            # Timestamp vs Timestamp 비교
            if existing_min <= self.requested_start and existing_max >= self.requested_end:
                return True, file_path
            else:
                return False, file_path
        except Exception as e:
            print(f"[WARN] Error reading {file_path}: {e}")
            return False, file_path

    def update_data_for_interval(self, interval):
        exists, file_path= self.data_exists_and_complete(interval)
        if exists:
            # 이미 충분히 커버
            return
        else:
            if os.path.exists(file_path):
                os.remove(file_path)

        interval_ms= self._interval_to_ms(interval)
        extra_ms   = self.warmup_period*interval_ms
        warmup_start_ms= max(0, self.start_ms - extra_ms)

        print(f"[INFO] Updating data for {self.symbol}-{interval} from {pd.to_datetime(warmup_start_ms, unit='ms')} to {self.requested_end}")
        df= fetch_futures_ohlcv(self.client, self.symbol, interval, warmup_start_ms, self.end_ms)
        df= prepare_futures_df(df)
        save_csv(df, file_path)
        print(f"[INFO] {self.symbol}-{interval}: {len(df)} rows => {file_path}")

    def update_all(self):
        if not os.path.exists(self.save_folder):
            os.makedirs(self.save_folder, exist_ok=True)
        for iv in self.intervals:
            self.update_data_for_interval(iv)
