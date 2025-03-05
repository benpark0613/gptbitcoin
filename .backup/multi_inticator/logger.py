import logging
import os


def setup_logger(name, log_file, level=logging.INFO):
    """
    지정된 이름과 파일로 로거를 설정하여 반환합니다.
    이 함수는 파일과 콘솔 핸들러를 모두 추가합니다.
    """
    # 로거 생성
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 파일 핸들러 생성
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)

    # 콘솔 핸들러 생성
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # 포매터 생성 및 핸들러에 설정
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # 핸들러를 로거에 추가 (이미 핸들러가 존재하면 중복 추가 방지)
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
