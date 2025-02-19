# gptbitcoin/utils/file_io.py

import os
import shutil
import pandas as pd

def clean_data_folder(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        for entry in os.listdir(path):
            fp = os.path.join(path, entry)
            if os.path.isfile(fp):
                os.remove(fp)
            else:
                shutil.rmtree(fp)

def save_summary_to_csv(df_summary: pd.DataFrame, out_path: str) -> None:
    """
    백테스트 결과 요약 df_summary를 CSV로 저장하되,
    float_format='%.2f'로 지정해 소수점 둘째 자리까지만 표현.
    """
    folder = os.path.dirname(out_path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)

    # 소수점 2자리 표시
    df_summary.to_csv(out_path, index=False, float_format="%.2f")