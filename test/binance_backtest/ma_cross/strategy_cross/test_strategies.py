import os
import shutil
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from binance.client import Client
from dotenv import load_dotenv

# backtesting 라이브러리
# pip install backtesting
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA


#----------------------------------------------------------------#
# 1) 폴더 정리 함수
#----------------------------------------------------------------#
def prep_folder(folder_path):
    """
    folder_path가 이미 있으면 내부 파일/폴더를 모두 삭제하고,
    없으면 새로 생성합니다.
    """
    if os.path.exists(folder_path):
        for item in os.listdir(folder_path):
            target = os.path.join(folder_path, item)
            if os.path.isfile(target):
                os.remove(target)
            else:
                shutil.rmtree(target)
    else:
        os.makedirs(folder_path)
    return folder_path


#----------------------------------------------------------------#
# 2) Binance 선물 OHLCV 데이터 가져오기 (한 번만 호출)
#----------------------------------------------------------------#
def fetch_data(symbol, interval, api_key, api_secret, limit=1500):
    client = Client(api_key, api_secret)

    raw = client.futures_klines(
        symbol=symbol,
        interval=interval,
        limit=limit  # 최대한 최근 데이터 (기본 1500봉)
    )

    df = pd.DataFrame(
        raw,
        columns=[
            "open_time", "open_price", "high_price", "low_price",
            "close_price", "volume", "close_time", "quote_asset_vol",
            "num_trades", "taker_base_vol", "taker_quote_vol", "ignore_field"
        ]
    )
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

    for col in ["open_price", "high_price", "low_price", "close_price", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.sort_values("open_time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


#----------------------------------------------------------------#
# 3) Strategy 클래스 (long_only, short_only, both) 모드 지원
#----------------------------------------------------------------#
class SmaCross(Strategy):
    n1 = 5
    n2 = 20
    mode = "both"  # 기본값: 롱/숏 모두

    def init(self):
        self.sma1 = self.I(SMA, self.data.Close, self.n1)
        self.sma2 = self.I(SMA, self.data.Close, self.n2)

    def next(self):
        # 골든크로스(단기 > 장기)
        if crossover(self.sma1, self.sma2):
            if self.mode in ("both", "long_only"):
                if self.position.is_short:
                    self.position.close()
                self.buy()

        # 데드크로스(장기 > 단기)
        elif crossover(self.sma2, self.sma1):
            if self.mode in ("both", "short_only"):
                if self.position.is_long:
                    self.position.close()
                self.sell()


#----------------------------------------------------------------#
# 4) 백테스트 함수들
#----------------------------------------------------------------#
def run_backtest_details(df, short_ma, long_ma,
                         initial_cash=1_000_000,
                         commission=0.005,
                         trade_mode="both"):
    """
    백테스트 후 누적수익률, 승률, 총거래수 등을 딕셔너리로 반환
    """
    # Strategy 파라미터 설정
    SmaCross.n1 = short_ma
    SmaCross.n2 = long_ma
    SmaCross.mode = trade_mode

    bt_df = df.rename(columns={
        "open_time": "Date",
        "open_price": "Open",
        "high_price": "High",
        "low_price": "Low",
        "close_price": "Close",
        "volume": "Volume"
    }).copy()
    bt_df.set_index("Date", inplace=True)
    bt_df.dropna(inplace=True)

    bt = Backtest(
        bt_df,
        SmaCross,
        cash=initial_cash,
        commission=commission,
        trade_on_close=True,
        exclusive_orders=True
    )
    result = bt.run()

    final_equity = result["Equity Final [$]"]
    total_return = (final_equity / initial_cash - 1) * 100.0
    win_rate = result["Win Rate [%]"]
    total_trades = result["# Trades"]

    info = {
        "short_ma": short_ma,
        "long_ma": long_ma,
        "total_return": total_return,
        "win_rate": win_rate,
        "total_trades": total_trades
    }
    return info


def run_backtest(df, short_ma, long_ma,
                 initial_cash=1_000_000,
                 commission=0.005,
                 trade_mode="both"):
    """
    (scan_cr_mode)에서 쓰는 간단 버전 (누적수익률만 반환)
    """
    details = run_backtest_details(
        df, short_ma, long_ma,
        initial_cash=initial_cash,
        commission=commission,
        trade_mode=trade_mode
    )
    return details["total_return"]


#----------------------------------------------------------------#
# 5) (short_ma, long_ma) 조합별 백테스트 스캔
#----------------------------------------------------------------#
def scan_cr_mode(df, x_list, y_list, trade_mode):
    matrix = []
    for long_ma in y_list:
        row = []
        for short_ma in x_list:
            # long_ma <= short_ma인 경우는 스킵
            if long_ma <= short_ma or short_ma <= 0 or long_ma <= 0:
                row.append(np.nan)
                continue
            cr_val = run_backtest(
                df,
                short_ma,
                long_ma,
                trade_mode=trade_mode
            )
            row.append(cr_val)
        matrix.append(row)
    return pd.DataFrame(matrix, index=y_list, columns=x_list)


#----------------------------------------------------------------#
# 6) Heatmap 저장 함수
#----------------------------------------------------------------#
def save_hm(df, title, folder_path, file_name, market_ret=np.nan):
    sns.set(font_scale=0.7, style="white")
    fig, ax = plt.subplots(figsize=(10, 8))

    sns.heatmap(
        df,
        annot=True,
        fmt=".2f",
        cmap="RdYlGn",  # 음수=붉은색 ~ 양수=녹색
        center=0,      # 0 기준
        cbar_kws={"shrink": 0.8},
        annot_kws={"size": 6},
        ax=ax
    )

    ax.set_xlabel("Short MA", fontsize=9)
    ax.set_ylabel("Long MA", fontsize=9)

    if not np.isnan(market_ret):
        title += f" (Market: {market_ret:.2f}%)"
    ax.set_title(title, fontsize=11)

    # Y축이 위에서 아래로 증가하는 형태 → 뒤집어서 (작은 MA가 위) 보이도록
    ax.invert_yaxis()
    plt.tight_layout()

    save_path = os.path.join(folder_path, file_name)
    plt.savefig(save_path, dpi=300)
    plt.close()


#----------------------------------------------------------------#
# 7) 모드별로 백테스트 + 결과 저장
#----------------------------------------------------------------#
def run_mode(df, trade_mode, base_out_dir,
             initial_cash=1_000_000, commission=0.005):
    """
    test_result_cr/{long_only|short_only|both} 폴더를 만들고
    해당 모드 백테스트 결과물을 저장
    """
    # 서브 폴더 지정
    sub_dir = os.path.join(base_out_dir, trade_mode)

    # 해당 서브 폴더만 비움
    prep_folder(sub_dir)

    # short/long MA 범위
    x_list = list(range(0, 65, 5))   # short
    y_list = list(range(0, 180, 5))  # long

    # 백테스트 스캔
    cr_df = scan_cr_mode(df, x_list, y_list, trade_mode)

    # 히트맵 csv
    hm_csv_path = os.path.join(sub_dir, "cr_heatmap.csv")
    cr_df.to_csv(hm_csv_path, float_format="%.2f")

    # 최대값 탐색
    max_val = np.nanmax(cr_df.values)
    best_x, best_y = None, None
    if not np.isnan(max_val):
        coords = np.where(np.isclose(cr_df.values, max_val, atol=1e-7))
        if coords[0].size > 0:
            i_idx = coords[0][0]
            j_idx = coords[1][0]
            best_y = cr_df.index[i_idx]
            best_x = cr_df.columns[j_idx]

    # 시장 바이앤홀드
    market_ret = np.nan
    if len(df) > 1:
        start_p = df.iloc[0]["close_price"]
        end_p = df.iloc[-1]["close_price"]
        market_ret = (end_p / start_p - 1.0) * 100.0

    # 히트맵 png
    title_text = f"Cumulative Return (%) - Mode: {trade_mode}"
    extras = []
    if not np.isnan(market_ret):
        extras.append(f"Market: {market_ret:.2f}%")
    if best_x is not None and best_y is not None:
        extras.append(f"Best: {max_val:.2f} (x={best_x}, y={best_y})")
    if extras:
        title_text += " (" + ", ".join(extras) + ")"

    save_hm(cr_df, title_text, sub_dir, "cr_heatmap.png", market_ret)

    # Best combo -> txt & csv
    if best_x is not None and best_y is not None:
        details = run_backtest_details(
            df,
            best_x, best_y,
            initial_cash=initial_cash,
            commission=commission,
            trade_mode=trade_mode
        )
        details["buy_and_hold_return"] = market_ret

        # csv
        csv_columns = [
            "buy_and_hold_return",
            "short_ma",
            "long_ma",
            "total_return",
            "win_rate",
            "total_trades",
        ]
        row_data = {c: details[c] for c in csv_columns}
        df_best = pd.DataFrame([row_data], columns=csv_columns)

        best_csv_path = os.path.join(sub_dir, "best_combo_details.csv")
        df_best.to_csv(best_csv_path, index=False, float_format="%.2f")

        # txt
        best_txt_path = os.path.join(sub_dir, "best_combo.txt")
        with open(best_txt_path, "w", encoding="utf-8") as f:
            f.write(f"Best CR: {max_val:.2f}% at (x={best_x}, y={best_y})\n")
            if not np.isnan(market_ret):
                f.write(f"Same Period Market(BTC) Return: {market_ret:.2f}%\n")


#----------------------------------------------------------------#
# 8) 메인 실행 로직
#----------------------------------------------------------------#
def main():
    load_dotenv()
    api_key = os.getenv("BINANCE_ACCESS_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_5MINUTE
    df = fetch_data(symbol, interval, api_key, api_secret, limit=1500)

    # 세 가지 모드 순차 실행
    run_mode(df, "long_only", base_out_dir)
    run_mode(df, "short_only", base_out_dir)
    run_mode(df, "both", base_out_dir)


if __name__ == "__main__":
    main()
