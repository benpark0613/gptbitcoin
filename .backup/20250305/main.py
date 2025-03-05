# gptbitcoin/main.py
# 1) 인-샘플 백테스트
# 2) 아웃-오브-샘플 백테스트
# 3) 성과 스코어링(Sharpe, MDD 등) + 최종 CSV 내보내기
# 주석은 필요한 최소한만 한글로 작성, docstring은 구글 스타일

import os

# 인-샘플 백테스트
try:
    from backtest.run_is import run_in_sample_backtest
except ImportError:
    raise ImportError("backtest/run_is.py 임포트 실패")

# 아웃-오브-샘플 백테스트
try:
    from backtest.run_oos import run_oos_backtest
except ImportError:
    raise ImportError("backtest/run_oos.py 임포트 실패")

# 분석 및 최종 스코어링
try:
    from analysis.scoring import merge_and_score_is_oos
except ImportError:
    raise ImportError("analysis/scoring.py 임포트 실패")

# config에서 결과 폴더, 파일 경로 불러온다고 가정
try:
    from config.config import RESULTS_DIR
except ImportError:
    RESULTS_DIR = "results"

def main():
    """
    main.py를 실행하면:
      1) 인-샘플 백테스트 -> results/IS/is_results.csv
      2) 아웃-오브-샘플 백테스트 -> results/OOS/oos_results.csv
      3) scoring.py가 is_results.csv / oos_results.csv를 병합 + Sharpe, MDD 등 계산
         -> 최종 CSV(사용자가 원하는 16개 컬럼) 저장
    """
    print("=== 1) 인-샘플 백테스트 시작 ===")
    run_in_sample_backtest()
    print("=== 인-샘플 백테스트 완료 ===\n")

    print("=== 2) 아웃-오브-샘플 백테스트 시작 ===")
    run_oos_backtest()
    print("=== 아웃-오브-샘플 백테스트 완료 ===\n")

    # scoring.py에서 is_results.csv + oos_results.csv 병합, Sharpe, MDD, Trades 등 계산
    print("=== 3) 성과 스코어링 및 최종 CSV 생성 ===")
    is_csv = os.path.join(RESULTS_DIR, "IS", "is_results.csv")
    oos_csv = os.path.join(RESULTS_DIR, "OOS", "oos_results.csv")
    final_csv = os.path.join(RESULTS_DIR, "final_scores.csv")

    # merge_and_score_is_oos 내부에서:
    #  - is_csv / oos_csv를 로드
    #  - Sharpe, MDD, 승률 등 계산
    #  - timeframe, is_start_cap, is_end_cap, is_return, is_trades, is_sharpe, is_mdd,
    #    is_passed, oos_start_cap, oos_end_cap, oos_return, oos_trades, oos_sharpe,
    #    oos_mdd, used_indicators, oos_trades_log
    #    컬럼 포함하여 final_csv에 저장
    # (실제 계산은 scoring.py에 구현되어 있어야 함)
    merge_and_score_is_oos(
        is_results_path=is_csv,
        oos_results_path=oos_csv,
        out_csv_path=final_csv,
        buyhold_is_return=0.0  # 예: buy&hold IS 수익률을 scoring 내부에서 재산출하거나, 인자로 전달해도 됨
    )

    print(f"=== 최종 성과결과 CSV: {final_csv} ===")
    print("main.py 흐름 완료.")


if __name__ == "__main__":
    main()
