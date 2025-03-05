# gptbitcoin/main.py
# 메인 스크립트, 구글 스타일, 최소한의 한글 주석

import sys
import sqlite3
import pandas as pd

from config.config import (
    SYMBOL,
    TIMEFRAMES,
    START_DATE,
    END_DATE,
    DB_BOUNDARY_DATE,
    DB_PATH,
)
from data.update_data import update_data_db
from data.preprocess import clean_ohlcv, merge_old_recent
from utils.db_utils import connect_db


def main():
    """
    백테스트 프로젝트 메인 진입점.
    1) config에서 설정값 로드
    2) update_data_db로 DB 갱신
    3) old_data, recent_data 각각 읽어서 병합
    4) clean_ohlcv로 최종 검증
    """
    # 1) update_data_db: 각 타임프레임별 DB 업데이트
    for tf in TIMEFRAMES:
        try:
            update_data_db(
                symbol=SYMBOL,
                timeframe=tf,
                start_str=START_DATE,
                end_str=END_DATE,
                db_path=DB_PATH,
                boundary_date=DB_BOUNDARY_DATE
            )
        except Exception as e:
            print(f"[main] {tf} 업데이트 오류: {e}")

    # 2) old_data, recent_data 읽어 병합(예시: 첫 번째 타임프레임만)
    if not TIMEFRAMES:
        print("[main] TIMEFRAMES가 비어 있음.")
        sys.exit(0)

    tf0 = TIMEFRAMES[0]
    conn = None
    try:
        conn = connect_db(DB_PATH)
        query_old = f"""
            SELECT *
              FROM old_data
             WHERE symbol='{SYMBOL}'
               AND timeframe='{tf0}'
             ORDER BY open_time
        """
        query_recent = f"""
            SELECT *
              FROM recent_data
             WHERE symbol='{SYMBOL}'
               AND timeframe='{tf0}'
             ORDER BY open_time
        """
        df_old = pd.read_sql(query_old, conn)
        df_recent = pd.read_sql(query_recent, conn)
    except sqlite3.Error as e:
        print(f"[main] DB 읽기 오류: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

    # 3) merge_old_recent로 시계열 정렬/병합
    df_merged = merge_old_recent(df_old, df_recent)

    # 4) clean_ohlcv로 최종 검증
    try:
        df_clean = clean_ohlcv(df_merged)
    except ValueError as ve:
        print(f"[main] clean_ohlcv 에러: {ve}")
        sys.exit(1)

    # 5) 병합+검증된 데이터를 CSV 등으로 저장 (예시)
    output_file = "cleaned_data.csv"
    df_clean.to_csv(output_file, index=False)
    print(f"[main] 완료. 결과 저장: {output_file}")


if __name__ == "__main__":
    main()
