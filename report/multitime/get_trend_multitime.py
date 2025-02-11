# main.py

import datetime
import os
import warnings

# FutureWarning 무시
warnings.filterwarnings("ignore", category=FutureWarning)

from multitime.utils.indicators import add_trend_indicators
# (1) 모듈 임포트
from multitime.utils.utils import (
    init_binance_client,
    cleanup_report_folder,
    get_instructions_text,
    get_futures_ohlcv,
    create_dataframe,
    save_to_csv
)
from multitime.utils.config import (
    timeframe_set_1,
    timeframe_set_2,
    params_set_1,
    params_set_2
)

def process_timeframe_set(client, symbol, timeframe_list, param_dict, total_data_count, folder_name, timestamp):
    """
    여러 시간프레임(timeframe_list)을 순회하며:
      - 1500개 OHLCV 조회
      - 보조지표 추가 (파라미터는 param_dict[interval])
      - 전체 CSV 저장
      - 최근 slice_count개 CSV 저장
      - TXT를 합치기 위한 문자열(= CSV 내용) 생성 후 반환
    """
    combined_text = ""
    for interval, slice_count in timeframe_list:
        interval_str = interval  # 예: "4h", "1h", "15m", "5m"

        # 1) 1500개 OHLCV + DataFrame 변환
        klines = get_futures_ohlcv(client, symbol, interval_str, limit=total_data_count)
        df_main = create_dataframe(klines)

        # 2) 시간프레임별 파라미터
        tf_param = param_dict.get(interval_str, {})

        # 3) 보조지표 적용
        df_main = add_trend_indicators(df_main, tf_param)

        # 4) CSV 전체 저장(1500개)
        csv_filename_all = os.path.join(folder_name, f"{timestamp}_{symbol}_{interval_str}_all.csv")
        save_to_csv(df_main, csv_filename_all)

        # 5) CSV 최근 slice_count개 저장
        df_sliced = df_main.tail(slice_count)
        csv_filename_sliced = os.path.join(
            folder_name,
            f"{timestamp}_{symbol}_{interval_str}_recent{slice_count}.csv"
        )
        save_to_csv(df_sliced, csv_filename_sliced)

        # 6) combined_text에 합치기
        combined_text += f"====== {interval_str} / 최근 {slice_count}개 ======\n"
        with open(csv_filename_sliced, "r", encoding="utf-8-sig") as f:
            csv_data = f.read()
            combined_text += csv_data + "\n\n"

    return combined_text


def main():
    # 1) 바이낸스 클라이언트 초기화
    client = init_binance_client()
    symbol = "BTCUSDT"

    # 2) 사용자 선택
    # user_choice = input("어떤 세트를 사용할까요? (1: [15m,1h,4h], 2: [5m,15m,1h]) : ")
    user_choice = "2"

    if user_choice == "1":
        chosen_timeframes = timeframe_set_1
        chosen_params = params_set_1
    elif user_choice == "2":
        chosen_timeframes = timeframe_set_2
        chosen_params = params_set_2
    else:
        print("잘못된 입력, 기본값(세트1) 사용")
        chosen_timeframes = timeframe_set_1
        chosen_params = params_set_1

    # 3) 폴더 정리
    folder_name = "report"
    cleanup_report_folder(folder_name)

    # 4) 데이터 수 / 타임스탬프
    total_data_count = 1500
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")

    # 5) 안내 문구
    instructions = get_instructions_text()

    # 6) 세트 처리(가장 큰 타임프레임 -> 작은 타임프레임 순)
    combined_text_body = process_timeframe_set(
        client=client,
        symbol=symbol,
        timeframe_list=chosen_timeframes,
        param_dict=chosen_params,
        total_data_count=total_data_count,
        folder_name=folder_name,
        timestamp=timestamp
    )

    # 7) 최종 TXT 파일 저장
    combined_text = instructions + "\n\n" + combined_text_body
    final_txt_path = os.path.join(folder_name, f"{timestamp}_{symbol}_all_timeframes.txt")
    with open(final_txt_path, "w", encoding="utf-8-sig") as f:
        f.write(combined_text)

    print(f"[완료] {final_txt_path} 에 모든 결과를 저장했습니다.")


if __name__ == "__main__":
    main()
