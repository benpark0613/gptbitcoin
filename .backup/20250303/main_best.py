# gptbitcoin/main_best.py
# 구글 스타일, 최소한의 한글 주석
#
# Filter, Support_Resistance, Channel_Breakout 컬럼을 포함하여
# DB에서 지표 데이터를 읽고, 새 데이터도 DB에 KST(UTC+9)로 시간 변환하여 저장한 뒤
# 단 하나의 인디케이터 조합(Combo)에 대해 전체 구간 백테스트를 수행한다.
# 결과로는 거래 기록만 줄바꿈 형식으로 출력한다.

import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

from pytz import timezone

from config.config import (
    BOUNDARY_DATE,
    START_CAPITAL
)
from data.fetch_data import fetch_ohlcv, klines_to_dataframe
from data.preprocess import preprocess_incremental_ohlcv_data
from backtest.run_best import run_best_combo

from utils.db_utils import (
    get_connection,
    load_indicators_from_db,
    insert_indicators
)

# KST 타임존 지정 (UTC+9)
KST = timezone("Asia/Seoul")


def today() -> str:
    """현재 날짜와 시간을 YYYY-MM-DD HH:MM:SS 형식으로 반환 (로컬 시스템 시각)."""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    """
    main_best.py:
      - IS/OOS 분할 없이, 전체 구간 백테스트
      - Filter / S&R / Channel 지표도 DB에 저장/조회
      - 단일 콤보로 백테스트 후 거래내역만 출력
      - 시간을 update_data.py와 동일하게 KST로 변환하여 DB 저장
    """

    # 사용자 지정
    SYMBOL = "BTCUSDT"
    START_DATE = "2024-01-01 00:00:00"  # KST 기준으로 가정
    END_DATE = today()  # 현재 시각 (로컬)

    # 사용자 전략 JSON (한 가지 예시)
    example_used_indicators = """
{
  "timeframe": "1d",
  "allow_short": true,
  "indicators": [
    {"indicator": "MA", "short_period": 5, "long_period": 100, "band_filter": 0.0},
    {"indicator": "OBV", "short_period": 5, "long_period": 100}
  ]
}
    """

    # JSON 파싱
    try:
        user_strat = json.loads(example_used_indicators)
    except json.JSONDecodeError:
        # 파싱 실패 시 종료
        sys.exit(1)

    # 파라미터 추출
    user_tf = user_strat.get("timeframe", "1d")
    allow_short = user_strat.get("allow_short", False)
    best_combo = user_strat.get("indicators", [])
    if not best_combo:
        # 인디케이터 설정이 비어있다면 종료
        return

    # 문자열 → datetime
    start_dt = pd.to_datetime(START_DATE)
    end_dt = pd.to_datetime(END_DATE)
    boundary_dt = pd.to_datetime(BOUNDARY_DATE)

    # 1) DB에서 old_df
    old_df = _load_old_data(
        symbol=SYMBOL,
        interval=user_tf,
        from_dt=start_dt,
        to_dt=boundary_dt - timedelta(days=1)
    )

    # 2) API에서 new_df
    new_df = _fetch_new_ohlcv(SYMBOL, user_tf, boundary_dt, end_dt)
    if new_df.empty:
        final_df = old_df
    else:
        # old_df tail + 새 구간 병합 → 지표 계산
        df_old_tail = old_df.tail(200) if len(old_df) >= 200 else old_df
        df_new_ind = preprocess_incremental_ohlcv_data(
            df_new=new_df,
            df_old_tail=df_old_tail,
            dropna_indicators=False
        )
        # DB에 저장
        _insert_new_indicators(SYMBOL, user_tf, df_new_ind, boundary_dt)
        # 최종 병합
        final_df = _merge_two_periods(old_df, df_new_ind, start_dt, end_dt)

    if final_df.empty or len(final_df) < 2:
        # 데이터가 거의 없으면 거래내역 없음
        return

    # 3) 백테스트
    out = run_best_combo(
        df=final_df,
        best_combo=best_combo,
        start_capital=START_CAPITAL,
        allow_short=allow_short
    )

    # 4) 거래내역 출력
    if "engine_out" not in out:
        return
    trades = out["engine_out"].get("trades", [])
    _print_trades_in_one_block(trades, final_df)


def _print_trades_in_one_block(trades: List[Dict[str, Any]], df: pd.DataFrame):
    """거래내역을 줄바꿈으로 출력한다."""
    if not trades:
        return

    for i, t in enumerate(trades, start=1):
        pos_type = t.get("position_type", "N/A").upper()
        e_idx = t.get("entry_index", None)
        x_idx = t.get("exit_index", None)

        if e_idx is not None and 0 <= e_idx < len(df):
            e_dt = str(df.iloc[e_idx]["datetime_utc"])
        else:
            e_dt = "N/A"

        if x_idx is not None and 0 <= x_idx < len(df):
            x_dt = str(df.iloc[x_idx]["datetime_utc"])
        elif isinstance(x_idx, int) and x_idx >= len(df):
            x_dt = "End"
        else:
            x_dt = "N/A"

        print(f"[{i}] {pos_type} Entry={e_dt}, Exit={x_dt}")


def _load_old_data(symbol: str, interval: str,
                   from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """
    DB에서 [from_dt ~ to_dt] 구간 데이터를 조회 (이미 KST 시각이 저장돼 있음).
    """
    if from_dt > to_dt:
        return pd.DataFrame()

    start_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_connection()
    try:
        df = load_indicators_from_db(conn, symbol, interval, start_str, end_str)
    finally:
        conn.close()

    return df


def _fetch_new_ohlcv(symbol: str, interval: str,
                     from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """
    바이낸스 API에서 [from_dt ~ to_dt] 구간 OHLCV를 가져와 DataFrame 생성.
    이후 update_data.py와 동일하게 UTC → KST 변환을 수행하여 'datetime_utc'에 저장.
    """
    if from_dt > to_dt:
        return pd.DataFrame()

    start_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    klines = fetch_ohlcv(symbol, interval, start_str, end_str)
    if not klines:
        return pd.DataFrame()

    df_raw = klines_to_dataframe(klines)  # 현재 UTC 문자열
    if df_raw.empty:
        return pd.DataFrame()

    # UTC → KST
    # 1) parse UTC string to Timestamp(UTC)
    df_raw["datetime_utc"] = pd.to_datetime(df_raw["datetime_utc"], format="%Y-%m-%d %H:%M:%S", utc=True)
    # 2) tz_convert -> KST
    df_raw["datetime_utc"] = df_raw["datetime_utc"].dt.tz_convert("Asia/Seoul")
    # 3) 다시 "%Y-%m-%d %H:%M:%S" 문자열
    df_raw["datetime_utc"] = df_raw["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 정렬
    df_raw.sort_values("datetime_utc", inplace=True)
    df_raw.reset_index(drop=True, inplace=True)
    return df_raw


def _insert_new_indicators(symbol: str, interval: str,
                           df: pd.DataFrame, boundary_dt: datetime):
    """
    boundary_dt 이후의 KST 시각을 DB에 저장.
    df["datetime_utc"]는 이미 "YYYY-MM-DD HH:MM:SS" (KST)
    """
    if df.empty:
        return

    # boundary_dt도 KST로 해석
    boundary_str = boundary_dt.strftime("%Y-%m-%d %H:%M:%S")
    df_use = df[df["datetime_utc"] >= boundary_str].copy()
    if df_use.empty:
        return

    # DB에 저장할 지표 컬럼
    all_indicator_cols = [
        "ma_5", "ma_10", "ma_20", "ma_50", "ma_100", "ma_200",
        "rsi_14", "rsi_21", "rsi_30",
        "obv", "obv_sma_5", "obv_sma_10", "obv_sma_30", "obv_sma_50", "obv_sma_100",
        "filter_min_10", "filter_max_10", "filter_min_20", "filter_max_20",
        "sr_min_10", "sr_max_10", "sr_min_20", "sr_max_20",
        "ch_min_14", "ch_max_14", "ch_min_20", "ch_max_20"
    ]

    conn = get_connection()
    try:
        insert_indicators(conn, symbol, interval, df_use, all_indicator_cols)
    finally:
        conn.close()


def _merge_two_periods(old_df: pd.DataFrame, new_df: pd.DataFrame,
                       from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """
    old_df + new_df 병합 후 [from_dt ~ to_dt] 구간만 슬라이스.
    모두 'datetime_utc'가 KST 문자열이므로, from_dt/to_dt도 KST로 가정.
    """
    df_all = pd.concat([old_df, new_df], ignore_index=True)
    df_all.sort_values("datetime_utc", inplace=True)
    df_all.reset_index(drop=True, inplace=True)

    start_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    mask = (df_all["datetime_utc"] >= start_str) & (df_all["datetime_utc"] <= end_str)
    final_df = df_all.loc[mask].copy()
    final_df.reset_index(drop=True, inplace=True)
    return final_df


if __name__ == "__main__":
    main()
