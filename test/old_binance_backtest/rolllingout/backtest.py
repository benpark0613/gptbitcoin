# backtest.py

import os
import pandas as pd

# (1) OHLCV 수집 및 보조지표 추가 함수 불러오기
from get_binance_ohlcv import main as generate_ohlcv
from technical_indicators import add_all_indicators

# (2) 75,360개 기술적 트레이딩 룰 생성 함수 불러오기
from technical_rules import generate_technical_rules

def run_backtest():
    """
    1) OHLCV 데이터를 메모리로 가져옴(return_data=True)
    2) 보조지표 추가
    3) technical_rules.py 에서 75,360개 규칙(파라미터 조합) 생성
    4) 생성된 규칙을 CSV 저장 (또는 후속 로직 적용)
    """

    # 1) OHLCV 데이터 수집
    symbol = "BTCUSDT"
    intervals = ["1d", "1h", "30m", "15m"]
    start_date = "2024-01-01"
    end_date = "2024-02-01"

    print("[1] OHLCV 데이터를 메모리로 수집합니다...")
    ohlcv_dict = generate_ohlcv(
        symbol=symbol,
        intervals=intervals,
        start_date=start_date,
        end_date=end_date,
        return_data=True  # CSV가 아닌 메모리로 반환
    )
    print("OHLCV 수집 완료.")

    # 2) 보조지표 추가(예: 4개 interval 각각에 대하여)
    print("[2] 보조지표를 추가합니다...")
    ohlcv_with_indicators = {}
    for interval, df in ohlcv_dict.items():
        df = add_all_indicators(df)
        ohlcv_with_indicators[interval] = df
    print("보조지표 추가 완료.")

    # 3) technical_rules.py 에서 75,360개 규칙(파라미터 조합) 생성
    print("[3] 기술적 트레이딩 룰(총 75,360개)을 생성합니다...")
    all_rules = generate_technical_rules()
    print(f"생성된 규칙 개수: {len(all_rules)}")  # 보통 75,360개

    # 4) 생성된 규칙 저장 (예: CSV)
    # 규칙은 list[dict] 형태이므로 DataFrame으로 변환하여 저장
    rules_df = pd.DataFrame(all_rules)
    rules_output_path = "all_technical_rules.csv"
    rules_df.to_csv(rules_output_path, index=False, encoding="utf-8")
    print(f"[4] 모든 기술적 룰을 CSV로 저장 완료 → {rules_output_path}")

    # -- 여기서부터 원하는 대로 백테스트 로직, 시뮬레이션, 결과 분석 등을 추가 가능 --
    # 예) for rule in all_rules: ... (각 규칙별 시뮬레이션)

if __name__ == "__main__":
    run_backtest()
