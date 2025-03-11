import os
import csv
import pandas as pd
from dateutil.relativedelta import relativedelta

def export_performance(
        df: pd.DataFrame,
        symbol: str,
        results_dir: str,
        base_filename: str = "final_performance"
) -> None:
    """
    성과 DataFrame을 CSV와 Excel로 저장한다.

    - CSV는 서식(색상)을 표현할 수 없으므로 일반 저장
    - Excel은 Styler를 이용해 다음 규칙 적용:
      1) is_sharpe >= 1 → 빨간색 셀
      2) oos_sharpe >= 1 → 빨간색 셀
      3) is_mdd <= 0.2 → 초록색 셀
      4) oos_mdd <= 0.2 → 초록색 셀
      5) 'Buy and Hold' 행 전체 → 노란색 (다른 색보다 우선)

    Args:
        df (pd.DataFrame): 성과 지표가 담긴 DataFrame
        symbol (str): 예) "BTCUSDT"
        results_dir (str): 결과 파일을 저장할 폴더 경로
        base_filename (str, optional): 파일명 접두어 (확장자 제외)
    """
    if df.empty:
        print("[data_export] 빈 DataFrame입니다. 저장 스킵.")
        return

    # 결과 폴더 생성
    os.makedirs(results_dir, exist_ok=True)

    # 1) CSV 저장 (색상 불가능)
    csv_path = os.path.join(results_dir, f"{base_filename}_{symbol}.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
    print(f"[data_export] CSV 저장 완료: {csv_path}")

    # 2) Excel 저장: Styler 사용
    xlsx_path = os.path.join(results_dir, f"{base_filename}_{symbol}.xlsx")

    # ---------- (A) 컬럼별(셀별) 색상 함수들 ----------
    def highlight_sharpe(val):
        """
        샤프지수가 1 이상이면 셀 배경을 빨간색으로.
        그렇지 않으면 빈 문자열(색상 없음).
        """
        try:
            numeric_val = float(val)
        except (ValueError, TypeError):
            return ""
        if numeric_val >= 1.0:
            return "background-color: red"
        return ""

    def highlight_mdd(val):
        """
        MDD가 0.2 이하(즉 20% 이하)면 셀 배경을 초록색으로.
        그렇지 않으면 빈 문자열.
        """
        try:
            numeric_val = float(val)
        except (ValueError, TypeError):
            return ""
        if numeric_val <= 0.2:
            return "background-color: green"
        return ""

    # ---------- (B) 특정 행(바이앤홀드) 전체 색상 함수 ----------
    def highlight_bh_row(row):
        """
        row 단위로 판단해서, 'Buy and Hold'이면 전체를 노란색으로 칠한다.
        """
        if "used_indicators" in row and row["used_indicators"] == "Buy and Hold":
            return ["background-color: yellow"] * len(row)
        return [""] * len(row)

    # ---------- (C) Styler 적용 순서 ----------
    # 1) 컬럼별로 map → 각 셀의 조건부 색상 (sharpe=red, mdd=green)
    # 2) 전체 행(바이앤홀드) → 노란색 (우선적으로 마지막에 적용되어 덮어씀)
    styler = df.style

    # is_sharpe 컬럼에 map
    if "is_sharpe" in df.columns:
        styler = styler.map(highlight_sharpe, subset=["is_sharpe"])

    # oos_sharpe 컬럼에 map
    if "oos_sharpe" in df.columns:
        styler = styler.map(highlight_sharpe, subset=["oos_sharpe"])

    # is_mdd 컬럼에 map
    if "is_mdd" in df.columns:
        styler = styler.map(highlight_mdd, subset=["is_mdd"])

    # oos_mdd 컬럼에 map
    if "oos_mdd" in df.columns:
        styler = styler.map(highlight_mdd, subset=["oos_mdd"])

    # 마지막에 바이앤홀드 행 전체 노란색
    styler = styler.apply(highlight_bh_row, axis=1)

    # ---------- (D) 엑셀로 출력 ----------
    styler.to_excel(xlsx_path, index=False)
    print(f"[data_export] Excel 저장 완료: {xlsx_path}")


def export_ohlcv_with_indicators(
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        results_dir: str
) -> None:
    """
    OHLCV + 보조지표가 포함된 DataFrame을 CSV로 저장한다.
    여기서는 색칠 규칙 없이 단순 CSV만 저장 예시.
    (원한다면 .style.map() 로직을 추가해 Excel로도 저장 가능)
    """
    if df.empty:
        print("[data_export] OHLCV+지표 DataFrame이 비어 있음.")
        return

    os.makedirs(results_dir, exist_ok=True)
    filename = f"ohlcv_with_indicators_{symbol}_{timeframe}.csv"
    save_path = os.path.join(results_dir, filename)
    df.to_csv(save_path, index=False, encoding="utf-8")
    print(f"[data_export] OHLCV+지표 CSV 저장 완료: {save_path}")
