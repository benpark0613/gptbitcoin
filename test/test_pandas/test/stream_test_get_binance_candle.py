import json
import os
import datetime
import time
import platform

import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv

# binance-futures-connector
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient


# ===========================
# 1) OS별 알림 함수
# ===========================
def cross_platform_alert(msg="새 캔들이 완성되었습니다!"):
    """
    Windows 10 → win10toast
    그 외(Mac, Linux 등) → tkinter.messagebox
    """
    system_name = platform.system()
    if system_name == "Windows":
        try:
            from win10toast import ToastNotifier
            toaster = ToastNotifier()
            toaster.show_toast("알림", msg, duration=5, threaded=True)
            while toaster.notification_active():
                time.sleep(0.1)
        except Exception as e:
            # win10toast가 없거나 에러 시, fallback으로 tkinter
            print("[WARN] win10toast 실패, tkinter messagebox fallback:", e)
            import tkinter
            from tkinter import messagebox
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showinfo("알림", msg)
            root.destroy()
    else:
        # Mac, Linux 등
        import tkinter
        from tkinter import messagebox
        root = tkinter.Tk()
        root.withdraw()
        messagebox.showinfo("알림", msg)
        root.destroy()


# ===========================
# 2) UMFutures 클라이언트 초기화
# ===========================
def init_um_futures_client(testnet=False):
    """
    .env에서 API Key/Secret 로드. testnet=True → 테스트넷, False → 메인넷
    """
    load_dotenv()
    access = os.getenv("BINANCE_ACCESS_KEY")
    secret = os.getenv("BINANCE_SECRET_KEY")

    if testnet:
        return UMFutures(key=access, secret=secret, base_url="https://testnet.binancefuture.com")
    else:
        return UMFutures(key=access, secret=secret)


# ===========================
# 3) 과거 OHLCV (REST)
# ===========================
def get_futures_ohlcv(client: UMFutures, symbol: str, interval: str, limit=1500):
    """
    'limit' 개수의 최근 캔들
    """
    klines = client.klines(symbol, interval=interval, limit=limit)
    return klines

# ===========================
# 4) Timestamp -> KST
# ===========================
def convert_to_kst(timestamp_ms):
    dt_utc = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000)
    dt_kst = dt_utc + datetime.timedelta(hours=9)
    return dt_kst.strftime("%Y-%m-%d %H:%M:%S")


# ===========================
# 5) klines -> DataFrame
# ===========================
def create_dataframe(klines):
    columns = [
        "open_time",
        "open", "high", "low", "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore"
    ]
    df = pd.DataFrame(klines, columns=columns)

    # opentime (KST)
    df["opentime"] = df["open_time"].apply(convert_to_kst)

    if "ignore" in df.columns:
        df.drop("ignore", axis=1, inplace=True)

    numeric_cols = ["open","high","low","close","volume","quote_asset_volume",
                    "taker_buy_base_asset_volume","taker_buy_quote_asset_volume"]
    for c in numeric_cols:
        df[c] = df[c].astype(float)

    new_columns_order = ["opentime","open_time","open","high","low","close","volume",
                         "close_time","quote_asset_volume","number_of_trades",
                         "taker_buy_base_asset_volume","taker_buy_quote_asset_volume"]
    df = df[new_columns_order]
    return df

# ===========================
# 6) 보조지표 추가
# ===========================
def add_trend_indicators(df):
    """
    5분봉 기준 민감도 높인 예시 지표 설정
    """
    # (1) 컬럼명 변경(pandas_ta 표준)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    # ------------------------------
    # (A) 최적화 예시 지표들
    # ------------------------------

    # 1. ADX: length=6
    adx_df = ta.adx(high=df["High"], low=df["Low"], close=df["Close"], length=6)
    df = pd.concat([df, adx_df], axis=1)

    # 2. AMAT: fast=12, slow=30
    amat_df = ta.amat(close=df["Close"], fast=12, slow=30)
    df = pd.concat([df, amat_df], axis=1)

    # 3. Aroon: length=7
    aroon_df = ta.aroon(high=df["High"], low=df["Low"], length=7)
    df = pd.concat([df, aroon_df], axis=1)

    # 4. CHOP: length=5
    chop_df = ta.chop(high=df["High"], low=df["Low"], close=df["Close"], length=5)
    df = pd.concat([df, chop_df], axis=1)

    # 5. CKSP: p=5, x=1.0, q=5
    cksp_df = ta.cksp(high=df["High"], low=df["Low"], close=df["Close"], p=5, x=1.0, q=5)
    df = pd.concat([df, cksp_df], axis=1)

    # 6. Decay: length=2
    decay_series = ta.decay(df["Close"], length=2)
    df = pd.concat([df, decay_series], axis=1)

    # 7. Decreasing: length=3
    decreasing_series = ta.decreasing(df["Close"], length=3)
    df = pd.concat([df, decreasing_series], axis=1)

    # 8. DPO: length=5
    dpo_df = ta.dpo(close=df["Close"], length=5)
    df = pd.concat([df, dpo_df], axis=1)

    # 9. Increasing: length=3
    increasing_series = ta.increasing(df["Close"], length=3)
    df = pd.concat([df, increasing_series], axis=1)

    # 10. Long Run: fast=20, slow=50
    long_run_df = ta.long_run(close=df["Close"], fast=20, slow=50)
    df = pd.concat([df, long_run_df], axis=1)

    # 11. PSAR: af=0.05, max_af=0.45
    psar_df = ta.psar(high=df["High"], low=df["Low"], close=df["Close"], af=0.05, max_af=0.45)
    df = pd.concat([df, psar_df], axis=1)

    # 12. QStick: length=3
    qstick_df = ta.qstick(open_=df["Open"], close=df["Close"], length=3)
    df = pd.concat([df, qstick_df], axis=1)

    # 13. Short Run: fast=2, slow=6
    short_run_df = ta.short_run(close=df["Close"], fast=2, slow=6)
    df = pd.concat([df, short_run_df], axis=1)

    # 14. TSignals: length=5
    tsignals_df = ta.tsignals(df["Close"], length=5)
    df = pd.concat([df, tsignals_df], axis=1)

    # 15. VHF: length=5
    vhf_df = ta.vhf(close=df["Close"], length=5)
    df = pd.concat([df, vhf_df], axis=1)

    # 16. Vortex: length=5
    vortex_df = ta.vortex(high=df["High"], low=df["Low"], close=df["Close"], length=5)
    df = pd.concat([df, vortex_df], axis=1)

    # 17. ER: length=5
    er_df = ta.er(df["Close"], length=5)
    df = pd.concat([df, er_df], axis=1)

    # 18. Inertia: length=8, mamode='sma'
    inertia_df = ta.inertia(df["Close"], length=8, mamode='sma')
    df = pd.concat([df, inertia_df], axis=1)

    # 19. STC: fast=13, slow=26
    stc_df = ta.stc(high=df["High"], low=df["Low"], close=df["Close"], fast=13, slow=26)
    df = pd.concat([df, stc_df], axis=1)

    # 20. MACD: fast=8, slow=21, signal=4
    macd_df = ta.macd(df["Close"], fast=8, slow=21, signal=4)
    df = pd.concat([df, macd_df], axis=1)

    # Ichimoku (미래행)
    ichimoku_result = ta.ichimoku(high=df["High"], low=df["Low"], close=df["Close"],
                                  tenkan=6, kijun=18, senkou=36)
    if isinstance(ichimoku_result, tuple):
        ichimoku_df = pd.concat(ichimoku_result, axis=1)
    else:
        ichimoku_df = ichimoku_result
    df = pd.concat([df, ichimoku_df], axis=1)

    # Supertrend: length=6, multiplier=3.0
    st_result = ta.supertrend(high=df["High"], low=df["Low"], close=df["Close"], length=6, multiplier=3.0)
    if isinstance(st_result, tuple):
        st_df = pd.concat(st_result, axis=1)
    else:
        st_df = st_result
    df = pd.concat([df, st_df], axis=1)

    # Bollinger Bands: length=12, std=2.0
    bbands_df = ta.bbands(close=df["Close"], length=12, std=2.0)
    df = pd.concat([df, bbands_df], axis=1)

    # Keltner Channel: length=12, scalar=1.5
    kc_df = ta.kc(high=df["High"], low=df["Low"], close=df["Close"], length=12, scalar=1.5)
    df = pd.concat([df, kc_df], axis=1)

    # Donchian Channel: length=12
    donchian_df = ta.donchian(high=df["High"], low=df["Low"], close=df["Close"],
                              lower_length=12, upper_length=12)
    df = pd.concat([df, donchian_df], axis=1)

    # ATR: length=9
    atr_df = ta.atr(high=df["High"], low=df["Low"], close=df["Close"], length=9)
    df = pd.concat([df, atr_df], axis=1)

    # ------------------------------
    # (B) 추가 요청 지표 (VWAP, TTM_Trend 제거)
    # ------------------------------

    # RSI: length=14
    rsi_df = ta.rsi(df["Close"], length=14)
    df = pd.concat([df, rsi_df], axis=1)

    # Stochastic: k=14, d=3
    stoch_df = ta.stoch(high=df["High"], low=df["Low"], close=df["Close"], k=14, d=3)
    df = pd.concat([df, stoch_df], axis=1)

    # Stoch RSI: length=14, rsi_length=14
    stochrsi_df = ta.stochrsi(df["Close"], length=14, rsi_length=14, k=3, d=3)
    df = pd.concat([df, stochrsi_df], axis=1)

    # CCI: length=20
    cci_df = ta.cci(high=df["High"], low=df["Low"], close=df["Close"], length=20)
    df = pd.concat([df, cci_df], axis=1)

    # OBV
    obv_df = ta.obv(close=df["Close"], volume=df["Volume"])
    df = pd.concat([df, obv_df], axis=1)

    # MFI: length=14
    mfi_df = ta.mfi(high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"], length=14)
    df = pd.concat([df, mfi_df], axis=1)

    # (3) 컬럼명 원복
    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    return df

# ===========================
# 7) CSV 저장
# ===========================
def save_to_csv(df: pd.DataFrame, filename: str):
    df_copy = df.copy()
    df_copy.reset_index(drop=True, inplace=True)

    if isinstance(df_copy.columns, pd.MultiIndex):
        df_copy.columns = [
            "_".join([str(x) for x in col]).rstrip("_") if isinstance(col, tuple) else col
            for col in df_copy.columns
        ]

    float_cols = df_copy.select_dtypes(include="float").columns
    for c in float_cols:
        df_copy[c] = df_copy[c].round(2)

    df_copy.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"[CSV] '{filename}' 저장 완료.")


# ===========================
# 전역
# ===========================
df_main = pd.DataFrame()
folder_name = "trend"

symbol = "BTCUSDT"
interval = "1m"


# ===========================
# 8) WebSocket 콜백
# ===========================
def handle_websocket_message(_, message):
    global df_main

    # 1) 디버그 출력
    print("[DEBUG raw msg]:", message)

    # 2) 만약 message가 str(문자열)이라면 JSON 디코딩
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError as e:
            print("[ERROR] json.loads 실패:", e)
            return

    # 3) dict가 아니면 무시
    if not isinstance(message, dict):
        print("[DEBUG] message가 dict 아님, return")
        return

    # 4) Combined 여부 확인
    if "stream" in message and "data" in message:
        data = message["data"]
    else:
        data = message

    # 5) kline 이벤트인지 확인
    if "e" in data and data["e"] == "kline":
        k = data["k"]
        print("[DEBUG] kline 이벤트, x =", k["x"])
        # x==true → 종가 확정
        if k["x"] is True:
            print("[DEBUG] x==True, 새 캔들 확정 로직 실행")
            open_time = k["t"]
            close_time = k["T"]
            open_ = float(k["o"])
            high_ = float(k["h"])
            low_ = float(k["l"])
            close_ = float(k["c"])
            volume_ = float(k["v"])

            # 마지막행 제거
            if len(df_main) > 0:
                df_main.drop(df_main.index[-1], inplace=True)

            new_row = {
                "opentime": convert_to_kst(open_time),
                "open_time": open_time,
                "open": open_,
                "high": high_,
                "low": low_,
                "close": close_,
                "volume": volume_,
                "close_time": close_time,
                "quote_asset_volume": float(k["q"]),
                "number_of_trades": float(k["n"]),
                "taker_buy_base_asset_volume": float(k["V"]),
                "taker_buy_quote_asset_volume": float(k["Q"])
            }

            df_main = pd.concat([df_main, pd.DataFrame([new_row])], ignore_index=True)

            # 지표
            df_with_indicators = add_trend_indicators(df_main)

            # CSV
            now_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)
            filename = os.path.join(folder_name, f"{now_str}_{symbol}_{interval}_live.csv")
            save_to_csv(df_with_indicators, filename)

            # OS별 팝업
            cross_platform_alert(f"{symbol} {interval} → 새 캔들 확정 & CSV 저장 완료")


# ===========================
# 9) 메인
# ===========================
def main():
    global df_main

    # (1) UMFutures (메인넷)
    client = init_um_futures_client(testnet=False)

    # (2) 과거 데이터 로드
    klines = get_futures_ohlcv(client, symbol, interval, limit=1500)
    df_main = create_dataframe(klines)
    df_main = add_trend_indicators(df_main)
    print(f"[INIT] 과거 {len(df_main)}개 로딩 후 지표 계산 완료.")

    # 폴더
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # 초기 CSV
    init_csv = os.path.join(folder_name, f"init_{symbol}_{interval}.csv")
    save_to_csv(df_main, init_csv)

    # (3) 실시간 WebSocket
    ws_client = UMFuturesWebsocketClient(on_message=handle_websocket_message)
    # 소문자 권장
    ws_client.kline(symbol=symbol.lower(), interval=interval)

    print("[WebSocket] 실시간 연결 중... (Ctrl + C 로 종료)")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\n[EXIT] 사용자 종료 요청.")
    finally:
        ws_client.stop()


if __name__ == "__main__":
    main()
