# gptbitcoin/main_best.py
# 특정 combo_info를 이용해 하나의 시나리오만 백테스트 후 trades_log를 출력하는 메인 스크립트
# 주석은 필요한 최소한의 한글만, docstring은 구글 스타일

import sys
import os
import pandas as pd

# run_best.py의 run_best_single 함수를 사용
try:
    from backtest.run_best import run_best_single
except ImportError:
    print("[main_best.py] run_best.py를 찾을 수 없거나 import 에러가 발생했습니다.")
    sys.exit(1)

# 필요 시 지표 계산이 되어 있지 않다면, indicators 모듈을 사용해 계산 가능
# (옵션) from indicators.indicators import calc_all_indicators

def main():
    """
    특정 combo_info로만 백테스트 후 trades_log를 확인하는 예시 스크립트.
    1) combo_info 지정
    2) CSV나 DB에서 데이터로드
    3) run_best_single 실행
    """
    # 1) 테스트할 combo_info (used_indicators에서 복사한 예시)
    combo_info = {"timeframe": "1d", "combo_params": [{"type": "MA", "short_period": 5, "long_period": 100, "band_filter": 0.0}]}

    # 2) OHLCV(+보조지표) 데이터 로드
    # 아래는 예시 경로. 이미 main.py에서 만든 CSV 파일 경로 등으로 수정 가능
    data_csv = os.path.join("results", "1d", "ohlcv_with_indicators_BTCUSDT_1d.csv")
    if not os.path.exists(data_csv):
        print(f"[main_best.py] CSV 파일을 찾을 수 없습니다: {data_csv}")
        sys.exit(1)

    df = pd.read_csv(data_csv)
    if df.empty:
        print("[main_best.py] 로드된 DataFrame이 비어 있습니다.")
        sys.exit(1)

    # (옵션) 지표가 미리 계산돼 있지 않다면, calc_all_indicators 등으로 계산
    # from indicators.indicators import calc_all_indicators
    # df = calc_all_indicators(df)

    # 3) 백테스트 실행 및 trades_log 출력
    run_best_single(df, combo_info)

    print("[main_best.py] 완료.")

if __name__ == "__main__":
    main()
