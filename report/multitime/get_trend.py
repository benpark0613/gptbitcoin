# main.py

import datetime
import os
import warnings

from report.multitime.utils.indicators import add_indicators
from report.multitime.utils.utils import (
    init_binance_client,
    cleanup_report_folder,
    get_instructions_text,
    get_futures_ohlcv,
    create_dataframe,
    save_to_csv
)
from report.multitime.utils.config import optimal_timeframe_params

warnings.filterwarnings("ignore", category=FutureWarning)


def main():
    # 1) 바이낸스 클라이언트 초기화
    client = init_binance_client()
    symbol = "BTCUSDT"

    # 2) 사용자가 원하는 타임프레임들 (큰 시간프레임 -> 작은 시간프레임)
    # "1d", "4h", "1h", "15m", "5m", "1m",
    selected_timeframes = ["1d", "4h", "1h", "15m"]

    # 3) 각 타임프레임 당 가져올 OHLCV 개수 / 최근 몇 개를 텍스트에 저장할지
    total_data_count = 1500
    recent_count = 12

    # 4) 보고서 폴더 초기화
    folder_name = "report_multiple"
    cleanup_report_folder(folder_name)

    # 5) 현재 시각 (파일명에 사용)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")

    # 6) 안내 문구
    instructions = get_instructions_text()
    # instructions = ""

    # 7) 타임프레임별 CSV 경로 및 문자열 보관용
    csv_info_list = []

    for tf in selected_timeframes:
        print(f"[INFO] {tf} 데이터 가져오는 중...")

        # 7-1) OHLCV 1500개 가져오기
        klines = get_futures_ohlcv(
            client=client,
            symbol=symbol,
            interval=tf,
            limit=total_data_count
        )
        df_main = create_dataframe(klines)

        # 7-2) 보조지표 추가
        tf_params = optimal_timeframe_params.get(tf, optimal_timeframe_params["5m"])
        df_main = add_indicators(df_main, tf_params)

        # 7-3) 전체 CSV 저장
        csv_filename = os.path.join(folder_name, f"{timestamp}_{symbol}_{tf}_all.csv")
        save_to_csv(df_main, csv_filename)

        # 7-4) 최근 N개 슬라이스 후, CSV 문자열 변환
        #      line_terminator='\n'로 지정해 LF만 사용
        df_recent = df_main.tail(recent_count)
        recent_csv_str = df_recent.to_csv(
            index=False,
            float_format='%.2f',
            lineterminator='\n'  # ← 여기서 줄바꿈 통일
        )

        # 7-5) (타임프레임, CSV 문자열) 저장
        csv_info_list.append((tf, recent_csv_str))

    # 8) 최종 TXT 파일 생성
    final_txt_path = os.path.join(folder_name, f"{timestamp}_{symbol}_merged.txt")
    with open(final_txt_path, "w", encoding="utf-8-sig") as f_out:
        # 안내 문구
        f_out.write(instructions + "\n\n")

        # 가장 큰 타임프레임(4h)부터 작은 타임프레임(1m) 순으로 기록
        for (tf, recent_csv_str) in csv_info_list:
            f_out.write(f"====== {tf} / 최근 {recent_count}개 ======\n")
            f_out.write(recent_csv_str)

    print(f"[완료] {final_txt_path} 에 모든 TF 결과를 저장했습니다.")


if __name__ == "__main__":
    main()
