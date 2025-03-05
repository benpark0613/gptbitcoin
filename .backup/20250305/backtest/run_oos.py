# gptbitcoin/backtest/run_oos.py
# - 아웃오브샘플(OOS) 백테스트
# - IS 결과에서 Buy&Hold 수익률 이상인 콤보만 필터 -> OOS 백테스트
# - 결과 CSV 저장
# - joblib 병렬처리

import os
import csv
import json
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from joblib import Parallel, delayed

try:
    from config.config import (
        DB_PATH,
        IS_OOS_BOUNDARY_DATE,
        END_DATE,
        SYMBOL,
        TIMEFRAMES,
        RESULTS_DIR
    )
except ImportError:
    DB_PATH = "data/db/ohlcv.sqlite"
    IS_OOS_BOUNDARY_DATE = "2024-12-31 00:00:00"
    END_DATE = "2025-01-01 00:00:00"
    SYMBOL = "BTCUSDT"
    TIMEFRAMES = ["1d", "4h"]
    RESULTS_DIR = "results"

try:
    from utils.db_utils import connect_db
except ImportError:
    raise ImportError("db_utils.py 경로 문제")

try:
    from data.preprocess import clean_ohlcv, merge_old_recent
except ImportError:
    raise ImportError("data/preprocess.py 경로 문제")

try:
    from backtest.engine import run_backtest
except ImportError:
    raise ImportError("engine.py 경로 문제")

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

try:
    from backtest.combo_generator import generate_indicator_combos
except ImportError:
    raise ImportError("combo_generator.py 경로 문제")


def _select_ohlcv(conn, symbol, timeframe, start_ts, end_ts) -> pd.DataFrame:
    """
    DB에서 old_data, recent_data를 합쳐 [start_ts, end_ts] 구간의 OHLCV 조회
    """

    sql_old = """
        SELECT timestamp_utc AS open_time, open, high, low, close, volume
          FROM old_data
         WHERE symbol=? AND timeframe=?
           AND timestamp_utc>=? AND timestamp_utc<=?
    """
    sql_recent = """
        SELECT timestamp_utc AS open_time, open, high, low, close, volume
          FROM recent_data
         WHERE symbol=? AND timeframe=?
           AND timestamp_utc>=? AND timestamp_utc<=?
    """

    df_old = pd.read_sql_query(sql_old, conn, params=(symbol, timeframe, start_ts, end_ts))
    df_recent = pd.read_sql_query(sql_recent, conn, params=(symbol, timeframe, start_ts, end_ts))

    if len(df_old) == 0 and len(df_recent) == 0:
        return pd.DataFrame()

    merged = merge_old_recent(df_old, df_recent)
    return merged


def load_oos_data(symbol: str, timeframe: str) -> pd.DataFrame:
    """
    OOS 구간: IS_OOS_BOUNDARY_DATE ~ END_DATE
    DB에서 로드 후 전처리, 정렬
    """

    conn = connect_db(DB_PATH)
    boundary_dt = datetime.strptime(IS_OOS_BOUNDARY_DATE, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d %H:%M:%S")

    start_ts = int(boundary_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    df_ohlcv = _select_ohlcv(conn, symbol, timeframe, start_ts, end_ts)
    conn.close()

    if df_ohlcv.empty:
        return df_ohlcv

    df_ohlcv = clean_ohlcv(df_ohlcv)
    df_ohlcv.sort_values("open_time", inplace=True)
    df_ohlcv.reset_index(drop=True, inplace=True)
    return df_ohlcv


def apply_indicators_and_signals(df: pd.DataFrame, combo: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    IS에서 사용한 동일 combo를 OOS 데이터에 적용.
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
            df_work = support_resistance_signal(df_work, min_col, max_col, price_col="close", band_pct=b_pct, signal_col=sig_col)

        elif ind_type == "Channel_Breakout":
            w = indicator_params["window"]
            c_val = indicator_params.get("c_value", 0.0)
            min_col = f"chan_min_{w}"
            max_col = f"chan_max_{w}"
            df_work = compute_rolling_min(df_work, period=w, price_col="close", col_name=min_col)
            df_work = compute_rolling_max(df_work, period=w, price_col="close", col_name=max_col)
            df_work = channel_breakout_signal(df_work, min_col, max_col, price_col="close", breakout_pct=c_val, signal_col=sig_col)

        else:
            pass

    df_work = combine_signals(df_work, signal_cols, out_col="signal")
    return df_work


def _run_single_oos(
    row: pd.Series,
    df_ohlcv: pd.DataFrame
) -> Dict[str, Any]:
    """
    단일 콤보(row)에 대해 OOS 백테스트 수행.
    row["combo_index"], row["used_indicators"] (json str) 등 사용
    """
    try:
        combo_str = row["used_indicators"]
        combo = json.loads(combo_str)  # IS에서 저장했던 combo를 로드

        df_signals = apply_indicators_and_signals(df_ohlcv, combo)
        bt_result = run_backtest(df_signals, result_csv_path=None)

        return {
            "combo_index": row["combo_index"],
            "timeframe": row["timeframe"],
            "used_indicators": combo_str,
            "oos_start_cap": bt_result["start_cap"],
            "oos_final_cap": bt_result["final_cap"],
            "oos_pnl": bt_result["pnl"],
            "oos_equity_curve": json.dumps(bt_result["equity_curve"]),
            "oos_trade_logs": json.dumps(bt_result["trade_logs"]),
            "error": ""
        }
    except Exception as e:
        return {
            "combo_index": row["combo_index"],
            "timeframe": row["timeframe"],
            "used_indicators": row["used_indicators"],
            "oos_start_cap": None,
            "oos_final_cap": None,
            "oos_pnl": None,
            "oos_equity_curve": "[]",
            "oos_trade_logs": "[]",
            "error": str(e)
        }


def run_oos_backtest() -> None:
    """
    1) IS 결과 로딩 (is_results.csv)
    2) Buy&Hold(B/H) is_return 산출 -> b/h 이상인 콤보만 필터
    3) OOS 구간 백테스트 → CSV 저장
    """
    print("[run_oos] OOS 백테스트 시작")

    is_csv = os.path.join(RESULTS_DIR, "IS", "is_results.csv")
    out_folder = os.path.join(RESULTS_DIR, "OOS")
    os.makedirs(out_folder, exist_ok=True)
    out_csv = os.path.join(out_folder, "oos_results.csv")

    if not os.path.isfile(is_csv):
        print(f"[run_oos] IS 결과 파일 없음: {is_csv}")
        return

    df_is = pd.read_csv(is_csv)
    if df_is.empty:
        print("[run_oos] is_results.csv 비어 있음.")
        return

    # Buy&Hold 결과( combo_index=0 ) 찾기.
    # -> IS 단계에서 (final_cap - start_cap) / start_cap을 IS 수익률로 사용
    # timeframe별로 B/H 다른 값을 쓸 수도 있음 -> 이를 위해 df_is 중 combo_index=0 행을 TF별로 구분
    # 아래 예시는 TF별 b/h 수익률을 딕셔너리에 저장
    bh_return_map = {}

    # combo_index=0 인 행들(=Buy&Hold)만
    df_bh = df_is[df_is["combo_index"] == 0].copy()
    if df_bh.empty:
        print("[run_oos] Buy&Hold 기록이 없음. 필터링 불가 -> 전부 OOS 진행하겠다.")
        # bh_return_map 자체가 빈 dict -> 전부 허용
    else:
        # 각 timeframe별로 b/h is_return 계산
        for tf in df_bh["timeframe"].unique():
            row_bh = df_bh[df_bh["timeframe"] == tf].head(1)
            if len(row_bh) == 0:
                continue
            start_cap = row_bh.iloc[0]["start_cap"]
            final_cap = row_bh.iloc[0]["final_cap"]
            if pd.isnull(start_cap) or pd.isnull(final_cap):
                continue
            bh_ret = (final_cap - start_cap) / start_cap if start_cap != 0 else 0
            bh_return_map[tf] = bh_ret

    # 이제 combo_index != 0 인 일반 콤보만 필터
    df_combos = df_is[df_is["combo_index"] != 0].copy()
    if df_combos.empty:
        print("[run_oos] IS 콤보 결과가 전혀 없음.")
        return

    # is_return 계산
    # (final_cap - start_cap)/start_cap
    df_combos["is_return"] = (
        (df_combos["final_cap"] - df_combos["start_cap"]) / df_combos["start_cap"]
    ).fillna(0.0)

    # Buy&Hold 없이 전부 OOS 실행할 수도 있으나, 설계에 따라 B/H 이상만
    def pass_filter(row):
        tf = row["timeframe"]
        if tf in bh_return_map:
            if pd.isnull(row["is_return"]):
                return False
            return row["is_return"] >= bh_return_map[tf]
        # B/H 값이 없으면 일단 전부 허용
        return True

    df_combos["is_passed"] = df_combos.apply(pass_filter, axis=1)
    df_top = df_combos[df_combos["is_passed"] == True].copy()
    if df_top.empty:
        print("[run_oos] B/H 이상인 콤보가 없음. 종료.")
        return

    # OOS 결과 CSV:
    fieldnames = [
        "combo_index",
        "timeframe",
        "used_indicators",
        "oos_start_cap",
        "oos_final_cap",
        "oos_pnl",
        "oos_equity_curve",
        "oos_trade_logs",
        "error"
    ]
    with open(out_csv, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for tf in TIMEFRAMES:
            df_tf = df_top[df_top["timeframe"] == tf]
            if df_tf.empty:
                print(f"[run_oos] {tf}에 해당하는 통과 콤보가 없음.")
                continue

            # OOS 데이터
            df_oos = load_oos_data(SYMBOL, tf)
            if df_oos.empty:
                print(f"[run_oos] {tf} OOS 데이터가 비어 있음 -> 스킵")
                continue

            # 병렬 실행
            rows = df_tf.to_dict("records")
            results = Parallel(n_jobs=-1, verbose=10)(
                delayed(_run_single_oos)(pd.Series(r), df_oos)
                for r in rows
            )

            for r in results:
                writer.writerow(r)

    print(f"[run_oos] OOS 백테스트 완료. 결과 CSV: {out_csv}")


if __name__ == "__main__":
    run_oos_backtest()
