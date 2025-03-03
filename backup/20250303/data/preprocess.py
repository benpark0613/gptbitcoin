# gptbitcoin/data/preprocess.py
# 구글 스타일, 최소한의 한글 주석
# OBV + OBV_SMA(롤링) 등에 대해, offset 적용 후 최종 결과가 반영되도록 수정.

import pandas as pd
from indicators.indicators import calc_all_indicators

def preprocess_ohlcv_data(df: pd.DataFrame, dropna: bool = False) -> pd.DataFrame:
    """
    전체 구간의 OHLCV에 대해 숫자 변환, 결측 검사 후
    calc_all_indicators로 지표 계산.
    dropna=True면 지표 NaN 행을 제거한다.
    """
    if df.empty:
        print("[WARN] preprocess_ohlcv_data: df가 비어 있음.")
        return df

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"[ERROR] '{col}' 누락")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df[["open","high","low","close","volume"]].isna().any().any():
        raise ValueError("[ERROR] OHLC/volume 중 NaN 존재")

    df = calc_all_indicators(df)
    if dropna:
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df


def preprocess_incremental_ohlcv_data(
    df_new: pd.DataFrame,
    df_old_tail: pd.DataFrame,
    dropna_indicators: bool = False
) -> pd.DataFrame:
    """
    (1) old tail + new 구간 concat 후, calc_all_indicators로 전체 지표 1차 계산
    (2) OBV/obv_raw에 offset 적용 (과거 tail 마지막 obv와 이어붙임)
    (3) obv_sma_x 등은 offset 반영 후 새로 롤링 계산
    (4) dropna, slice -> 최종 반환
    """
    if df_new.empty:
        print("[WARN] preprocess_incremental_ohlcv_data: df_new 비어 있음.")
        return df_new

    # 신규 구간: 숫자 변환 & 결측 검사
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df_new.columns:
            raise ValueError(f"[ERROR] '{col}'가 df_new에 없음")
        df_new[col] = pd.to_numeric(df_new[col], errors="coerce")

    if df_new[["open","high","low","close","volume"]].isna().any().any():
        raise ValueError("[ERROR] df_new에 OHLC/volume NaN 존재")

    # 과거 tail 정리
    if df_old_tail is None:
        df_old_tail = pd.DataFrame()
    else:
        df_old_tail = df_old_tail.copy()
        df_old_tail.sort_values("datetime_utc", inplace=True)
        df_old_tail.reset_index(drop=True, inplace=True)

    # --------------------------
    # 1) 병합 후 1차 지표 계산
    # --------------------------
    df_merged = pd.concat([df_old_tail, df_new], ignore_index=True)
    df_merged.sort_values("datetime_utc", inplace=True)
    df_merged.reset_index(drop=True, inplace=True)

    df_merged = calc_all_indicators(df_merged)

    # --------------------------
    # 2) OBV offset 적용
    # --------------------------
    # df_old_tail 마지막 obv / obv_raw
    # vs. df_merged에서 같은 위치 obv / obv_raw
    if not df_old_tail.empty:
        old_len = len(df_old_tail)
        if "obv" in df_old_tail.columns and "obv" in df_merged.columns:
            old_obv_last = df_old_tail.iloc[-1].get("obv", 0.0)
            merged_obv_tail = df_merged.iloc[old_len - 1].get("obv", 0.0) if old_len > 0 else 0.0
            obv_offset = old_obv_last - merged_obv_tail

            if "obv_raw" in df_merged.columns and "obv_raw" in df_old_tail.columns:
                old_obv_raw_last = df_old_tail.iloc[-1].get("obv_raw", 0.0)
                merged_obv_raw_tail = df_merged.iloc[old_len - 1].get("obv_raw", 0.0)
                obv_raw_offset = old_obv_raw_last - merged_obv_raw_tail
            else:
                obv_raw_offset = obv_offset  # obv_raw 없으면 동일 offset 가정

            # df_merged 전 구간에 offset
            df_merged["obv"] = df_merged["obv"] + obv_offset
            if "obv_raw" in df_merged.columns:
                df_merged["obv_raw"] = df_merged["obv_raw"] + obv_raw_offset

            df_merged["obv"] = df_merged["obv"].round(2)
            if "obv_raw" in df_merged.columns:
                df_merged["obv_raw"] = df_merged["obv_raw"].round(2)

    # ----------------------------------
    # 3) obv_sma_x (등 obv 관련 롤링) 재계산
    # ----------------------------------
    # obv_sma_x는 indicators.py에서 obv_raw의 rolling.mean()으로 계산된다는 전제
    # 여기서만 다시 부분적으로 obv_sma_5, obv_sma_10, obv_sma_30 등 재계산
    # (calc_all_indicators 전체를 재호출하면 MA/RSI도 2번 계산되므로, 부분만 수행)
    if "obv_raw" in df_merged.columns:
        # 원하는 모든 기간이 있으면 루프
        # 아래는 예시로 obv_sma_5, obv_sma_10, obv_sma_30, obv_sma_50, obv_sma_100 등
        # 필요하다면 config에서 가져올 수도 있음
        periods = [5, 10, 30, 50, 100]
        for p in periods:
            col_name = f"obv_sma_{p}"
            # rolling min_periods=p
            roll_s = df_merged["obv_raw"].rolling(window=p, min_periods=p).mean()
            df_merged[col_name] = roll_s.round(2)

    # dropna
    if dropna_indicators:
        df_merged.dropna(inplace=True)
        df_merged.reset_index(drop=True, inplace=True)

    # ----------------------------------
    # 4) 신규 구간만 슬라이스 후 반환
    # ----------------------------------
    min_dt = df_new["datetime_utc"].min()
    max_dt = df_new["datetime_utc"].max()
    mask = (df_merged["datetime_utc"] >= min_dt) & (df_merged["datetime_utc"] <= max_dt)
    final_df = df_merged.loc[mask].copy()
    final_df.reset_index(drop=True, inplace=True)

    return final_df