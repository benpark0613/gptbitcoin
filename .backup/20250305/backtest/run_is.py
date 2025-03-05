# gptbitcoin/backtest/run_is.py
# 주석은 필요한 만큼 한글, docstring은 구글 스타일
# - 인-샘플 구간 백테스트
# - Buy & Hold + 여러 지표 콤보를 모두 테스트
# - joblib 병렬 처리

import csv
import json
import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from joblib import Parallel, delayed

# DB 설정, 날짜 범위 등 불러오기
try:
    from config.config import (
        DB_PATH,
        IS_OOS_BOUNDARY_DATE,
        START_DATE,
        SYMBOL,
        TIMEFRAMES,
        RESULTS_DIR,
    )
except ImportError:
    DB_PATH = "data/db/ohlcv.sqlite"
    IS_OOS_BOUNDARY_DATE = "2024-12-31 00:00:00"
    START_DATE = "2023-01-01 00:00:00"
    SYMBOL = "BTCUSDT"
    TIMEFRAMES = ["1d", "4h"]
    RESULTS_DIR = "results"

# DB 유틸
try:
    from utils.db_utils import connect_db
except ImportError:
    raise ImportError("db_utils.py 경로 확인 필요")

# 전처리
try:
    from data.preprocess import clean_ohlcv, merge_old_recent
except ImportError:
    raise ImportError("data/preprocess.py 경로 확인 필요")

# 백테스트 엔진
try:
    from backtest.engine import run_backtest
except ImportError:
    raise ImportError("engine.py 경로 확인 필요")

# 지표 파라미터 조합
try:
    from backtest.combo_generator import generate_indicator_combos
except ImportError:
    raise ImportError("combo_generator.py 경로 확인 필요")

# 지표 계산 & 시그널 로직 (MA, RSI 등)
try:
    from indicators.indicators import (
        compute_ma,
        compute_rsi,
        compute_obv,
        compute_rolling_min,
        compute_rolling_max,
    )
    from strategies.signal_logic import (
        ma_crossover_signal,
        rsi_signal,
        obv_signal,
        filter_rule_signal,
        support_resistance_signal,
        channel_breakout_signal,
        combine_signals
    )
except ImportError:
    raise ImportError("indicators/ or strategies/ 임포트 실패")


def _select_ohlcv(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int
) -> pd.DataFrame:
    """
    DB에서 [start_ts, end_ts] 구간의 (symbol, timeframe) OHLCV 조회 후 병합
    """
    sql_old = """
        SELECT timestamp_utc AS open_time, open, high, low, close, volume
          FROM old_data
         WHERE symbol=? AND timeframe=? AND timestamp_utc>=? AND timestamp_utc<=?
    """
    sql_recent = """
        SELECT timestamp_utc AS open_time, open, high, low, close, volume
          FROM recent_data
         WHERE symbol=? AND timeframe=? AND timestamp_utc>=? AND timestamp_utc<=?
    """
    df_old = pd.read_sql_query(sql_old, conn, params=(symbol, timeframe, start_ts, end_ts))
    df_recent = pd.read_sql_query(sql_recent, conn, params=(symbol, timeframe, start_ts, end_ts))

    if len(df_old) == 0 and len(df_recent) == 0:
        return pd.DataFrame()

    merged = merge_old_recent(df_old, df_recent)
    return merged


def load_in_sample_data(symbol: str, timeframe: str) -> pd.DataFrame:
    """
    인-샘플(START_DATE ~ IS_OOS_BOUNDARY_DATE) 구간의 OHLCV 데이터 로드 및 전처리
    """
    conn = connect_db(DB_PATH)
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S")
    is_end_dt = datetime.strptime(IS_OOS_BOUNDARY_DATE, "%Y-%m-%d %H:%M:%S")

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(is_end_dt.timestamp() * 1000)

    df_ohlcv = _select_ohlcv(conn, symbol, timeframe, start_ts, end_ts)
    conn.close()

    if df_ohlcv.empty:
        return df_ohlcv

    df_ohlcv = clean_ohlcv(df_ohlcv)
    df_ohlcv.sort_values("open_time", inplace=True)
    df_ohlcv.reset_index(drop=True, inplace=True)
    return df_ohlcv


def apply_indicators_and_signals(
    df: pd.DataFrame,
    combo: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    지표 파라미터(combo)에 따라 보조지표 계산 + 시그널 생성
    최종 df['signal'] 칼럼을 만든다.
    """
    df_work = df.copy()
    signal_cols = []

    for idx, indicator_params in enumerate(combo, start=1):
        ind_type = indicator_params["type"]
        sig_col = f"signal_{ind_type.lower()}_{idx}"
        df_work[sig_col] = 0
        signal_cols.append(sig_col)

        if ind_type == "MA":
            sp = indicator_params["short_period"]
            lp = indicator_params["long_period"]
            df_work = compute_ma(df_work, period=sp, price_col="close", col_name=f"MA_{sp}")
            df_work = compute_ma(df_work, period=lp, price_col="close", col_name=f"MA_{lp}")
            df_work = ma_crossover_signal(df_work, short_ma_col=f"MA_{sp}", long_ma_col=f"MA_{lp}", signal_col=sig_col)

        elif ind_type == "RSI":
            length = indicator_params["length"]
            overbought = indicator_params.get("overbought", 70)
            oversold = indicator_params.get("oversold", 30)
            rsi_col = f"RSI_{length}"
            df_work = compute_rsi(df_work, period=length, price_col="close", col_name=rsi_col)
            df_work = rsi_signal(df_work, rsi_col=rsi_col, lower_bound=oversold, upper_bound=overbought, signal_col=sig_col)

        elif ind_type == "OBV":
            if "OBV" not in df_work.columns:
                df_work = compute_obv(df_work, price_col="close", vol_col="volume", obv_col="OBV")
            df_work = obv_signal(df_work, obv_col="OBV", threshold=0.0, signal_col=sig_col)

        elif ind_type == "Filter":
            w = indicator_params["window"]
            x_p = indicator_params["x_pct"]
            y_p = indicator_params["y_pct"]
            df_work = filter_rule_signal(df_work, close_col="close", window=w, x_pct=x_p, y_pct=y_p, signal_col=sig_col)

        elif ind_type == "Support_Resistance":
            w = indicator_params["window"]
            b_pct = indicator_params.get("band_pct", 0.0)
            min_col = f"min_{w}"
            max_col = f"max_{w}"
            df_work = compute_rolling_min(df_work, period=w, price_col="close", col_name=min_col)
            df_work = compute_rolling_max(df_work, period=w, price_col="close", col_name=max_col)
            df_work = support_resistance_signal(
                df_work, min_col, max_col, price_col="close", band_pct=b_pct, signal_col=sig_col
            )

        elif ind_type == "Channel_Breakout":
            w = indicator_params["window"]
            c_val = indicator_params.get("c_value", 0.0)
            min_col = f"chan_min_{w}"
            max_col = f"chan_max_{w}"
            df_work = compute_rolling_min(df_work, period=w, price_col="close", col_name=min_col)
            df_work = compute_rolling_max(df_work, period=w, price_col="close", col_name=max_col)
            df_work = channel_breakout_signal(
                df_work, min_col, max_col, price_col="close", breakout_pct=c_val, signal_col=sig_col
            )
        else:
            # 알 수 없는 지표
            pass

    # 모든 시그널 합산 -> df['signal']
    df_work = combine_signals(df_work, signal_cols, out_col="signal")
    return df_work


def _run_single_combo(
    combo_idx: int,
    combo: List[Dict[str, Any]],
    df_ohlcv: pd.DataFrame,
    timeframe: str
) -> Dict[str, Any]:
    """
    단일 콤보 백테스트 수행(멀티프로세싱) → raw 결과 반환
    """
    try:
        df_signals = apply_indicators_and_signals(df_ohlcv, combo)
        bt_result = run_backtest(df_signals, result_csv_path=None)
        # used_indicators를 문자열화하기 위해 combo를 json.dumps
        used_indicators = json.dumps(combo)

        return {
            "combo_index": combo_idx,
            "timeframe": timeframe,
            "used_indicators": used_indicators,
            "start_cap": bt_result["start_cap"],
            "final_cap": bt_result["final_cap"],
            "pnl": bt_result["pnl"],
            # 나중에 scoring.py에서 Sharpe/MDD 계산할 때 필요
            # JSON string으로 저장
            "equity_curve": json.dumps(bt_result["equity_curve"]),
            "trade_logs": json.dumps(bt_result["trade_logs"]),
            "error": ""
        }
    except Exception as e:
        return {
            "combo_index": combo_idx,
            "timeframe": timeframe,
            "used_indicators": json.dumps(combo),
            "start_cap": None,
            "final_cap": None,
            "pnl": None,
            "equity_curve": "[]",
            "trade_logs": "[]",
            "error": str(e)
        }


def run_in_sample_backtest() -> None:
    """
    인-샘플 백테스트:
    1) Buy & Hold 실행해 벤치마크 수익률 확인
    2) 지표 콤보를 병렬 실행
    3) 결과를 CSV로 저장
    """
    print("[run_is] 인-샘플 백테스트 시작")
    combos = generate_indicator_combos()  # 모든 지표 파라미터 조합
    print(f"[run_is] 지표 콤보 개수: {len(combos)}")

    out_folder = os.path.join(RESULTS_DIR, "IS")
    os.makedirs(out_folder, exist_ok=True)
    out_csv = os.path.join(out_folder, "is_results.csv")

    # CSV 헤더
    fieldnames = [
        "combo_index",
        "timeframe",
        "used_indicators",
        "start_cap",
        "final_cap",
        "pnl",
        "equity_curve",
        "trade_logs",
        "error",
        # Buy & Hold 컬럼을 여기서 함께 기록할 수도 있으나,
        # 아래에서 별도 행으로 삽입
    ]

    with open(out_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for tf in TIMEFRAMES:
            df_ohlcv = load_in_sample_data(SYMBOL, tf)
            if df_ohlcv.empty:
                print(f"[run_is] {tf} 구간 데이터가 비어있음. 건너뜀.")
                continue

            # 1) Buy & Hold 백테스트
            #    signal=1 (항상 매수) 라고 간단히 만들거나,
            #    또는 engine.py에서 'BuyHoldStrategy'를 따로 만들어도 됨
            df_bh = df_ohlcv.copy()
            df_bh["signal"] = 1  # 모든 시점 매수 (언제든 롱 유지)
            try:
                bh_res = run_backtest(df_bh, result_csv_path=None)
                writer.writerow({
                    "combo_index": 0,  # 0 or -1을 B/H로 사용
                    "timeframe": tf,
                    "used_indicators": "Buy&Hold",
                    "start_cap": bh_res["start_cap"],
                    "final_cap": bh_res["final_cap"],
                    "pnl": bh_res["pnl"],
                    "equity_curve": json.dumps(bh_res["equity_curve"]),
                    "trade_logs": json.dumps(bh_res["trade_logs"]),
                    "error": ""
                })
                print(f"[run_is] Buy&Hold {tf} 완료. (start={bh_res['start_cap']}, final={bh_res['final_cap']})")
            except Exception as e:
                print(f"[run_is] Buy&Hold {tf} 실패: {e}")
                writer.writerow({
                    "combo_index": 0,
                    "timeframe": tf,
                    "used_indicators": "Buy&Hold",
                    "start_cap": None,
                    "final_cap": None,
                    "pnl": None,
                    "equity_curve": "[]",
                    "trade_logs": "[]",
                    "error": str(e)
                })

            # 2) 콤보 병렬 백테스트
            results = Parallel(n_jobs=-1, verbose=10)(
                delayed(_run_single_combo)(idx, combo, df_ohlcv, tf)
                for idx, combo in enumerate(combos, start=1)
            )

            # 3) CSV에 기록
            for row in results:
                writer.writerow(row)

    print(f"[run_is] 인-샘플 백테스트 종료. 결과: {out_csv}")


if __name__ == "__main__":
    run_in_sample_backtest()
