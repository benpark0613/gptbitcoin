# utils/file_io.py

"""
file_io.py

데이터 폴더를 관리하는 유틸리티 모듈.
"""

import os
import shutil

def clean_data_folder(path: str) -> None:
    """
    주어진 폴더(path)가 없으면 생성하고,
    존재할 경우 내부의 모든 파일/폴더를 삭제한다.

    Parameters
    ----------
    path : str
        폴더 경로
    """
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        for entry in os.listdir(path):
            fp = os.path.join(path, entry)
            if os.path.isfile(fp):
                os.remove(fp)
            else:
                shutil.rmtree(fp)
