import os
import shutil
import logging

def clear_folder(folder_path):
    """폴더 내부의 모든 파일과 하위 폴더를 삭제"""
    if os.path.exists(folder_path):
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)  # 파일 삭제
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # 하위 폴더 삭제
                logging.info("Deleted: %s", file_path)
            except Exception as e:
                logging.error("Error deleting %s: %s", file_path, e)
