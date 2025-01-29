import pandas as pd


def remove_first_row(input_file, output_file):
    """
    CSV 파일에서 첫 행을 제거하고 새로운 파일로 저장합니다.
    """
    # CSV 파일 읽기
    df = pd.read_csv(input_file)
    print("원본 데이터 미리보기:")
    print(df.head())  # 데이터 확인

    # 첫 행 제거
    df_cleaned = df.iloc[1:]  # 첫 번째 행 제외
    print("\n첫 행 제거 후 데이터 미리보기:")
    print(df_cleaned.head())

    # 새로운 CSV 파일로 저장
    df_cleaned.to_csv(output_file, index=False, encoding="utf-8")
    print(f"\n첫 행이 제거된 파일이 '{output_file}'로 저장되었습니다.")


def main():
    # 파일 경로 설정
    input_file = "binance_futures_20250125_204027.csv"  # 원본 파일 이름
    output_file = "binance_futures_20250125_204027_cleaned.csv"  # 결과 파일 이름

    # 함수 실행
    remove_first_row(input_file, output_file)


if __name__ == "__main__":
    main()
