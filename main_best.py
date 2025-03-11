# main_best.py
# 메인 백테스트 실행 스크립트 (항상 최신(today()) 시점으로 종료 시점을 설정)

import datetime
import platform
import time
import os
import logging
from typing import Optional, Dict, Any

import pytz
import schedule

# 날짜/시각 유틸
from utils.date_time import today

# Windows 알림
try:
    from win10toast import ToastNotifier

    _WINDOWS_TOAST_AVAILABLE = True
except ImportError:
    _WINDOWS_TOAST_AVAILABLE = False

# MacOS 알림
try:
    from pync import Notifier

    _MACOS_NOTIFIER_AVAILABLE = True
except ImportError:
    _MACOS_NOTIFIER_AVAILABLE = False

# 메일 전송 유틸
from utils.mail_utils import send_gmail

# === config.py에서 가져올 항목(심볼, DB 경계, 거래소 오픈일, DB 경로, 로그 경로 등) ===
from config.config import (
    SYMBOL,
    TIMEFRAMES,
    DB_BOUNDARY_DATE,
    EXCHANGE_OPEN_DATE,
    DB_PATH,
    IS_OOS_BOUNDARY_DATE,
    LOG_LEVEL,
    LOGS_DIR,
)

# 보조지표 설정(INDICATOR_CONFIG) - 워밍업 계산용
from config.indicator_config import INDICATOR_CONFIG

# DB 업데이트 (API 요청 → SQLite)
from data.update_data import update_data_db

# 전처리(NaN/이상치)
from data.preprocess import clean_ohlcv

# 지표 계산
from indicators.aggregator import calc_all_indicators

# 지표 파라미터(워밍업 봉 계산)
from utils.indicator_utils import get_required_warmup_bars

# DB 병합 로딩
from utils.db_utils import prepare_ohlcv_with_warmup

# 콤보 + B/H 백테스트 (단일 콤보)
from backtest.run_best import run_best_single

_previous_position: Optional[str] = None

# 로그 폴더 생성
os.makedirs(LOGS_DIR, exist_ok=True)

# 기존 로그 파일 정리 (최대 5개 유지)
existing_logs = [f for f in os.listdir(LOGS_DIR) if f.endswith(".log")]
if len(existing_logs) >= 5:
    existing_logs.sort(key=lambda x: os.path.getctime(os.path.join(LOGS_DIR, x)))
    while len(existing_logs) >= 5:
        oldest_file = existing_logs.pop(0)
        os.remove(os.path.join(LOGS_DIR, oldest_file))

# 새 로그 파일 설정
log_filename = datetime.datetime.now().strftime("main_best_%Y%m%d_%H%M%S.log")
log_filepath = os.path.join(LOGS_DIR, log_filename)

# 로그 설정
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filepath, encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def _show_system_notification(title: str, msg: str, duration_sec: int = 10):
    """Windows/MacOS 알림 혹은 콘솔 출력.

    Args:
        title (str): 알림 제목
        msg (str): 알림 내용
        duration_sec (int): 표시 지속 시간(초)
    """
    system_name = platform.system().lower()

    if system_name.startswith("win") and _WINDOWS_TOAST_AVAILABLE:
        toaster = ToastNotifier()
        toaster.show_toast(
            title=title,
            msg=msg,
            duration=duration_sec,
            threaded=True
        )
    elif system_name.startswith("darwin") and _MACOS_NOTIFIER_AVAILABLE:
        Notifier.notify(msg, title=title)
    else:
        logging.info(f"[TOAST] {title}: {msg}")


def _send_email_notification(subject: str, body: str) -> None:
    """이메일 발송.

    Args:
        subject (str): 메일 제목
        body (str): 메일 본문
    """
    try:
        send_gmail(subject=subject, body=body)
    except Exception as e:
        logging.error(f"[main_best] 메일 전송 실패: {e}")


def notify_user(message: str, send_email: bool = False, email_subject: str = ""):
    """로그 + (가능 시) 시스템 알림 + (옵션) 메일 발송.

    Args:
        message (str): 메시지
        send_email (bool): 메일 즉시 발송 여부
        email_subject (str): 메일 제목
    """
    now_kst = datetime.datetime.now(tz=pytz.timezone("Asia/Seoul"))
    now_str = now_kst.strftime("%Y-%m-%d %H:%M:%S KST")
    full_msg = f"{now_str} - {message}"

    logging.info(f"[ALERT] {full_msg}")
    _show_system_notification("백테스트 알림", message, duration_sec=15)

    if send_email and SEND_EMAIL:
        subject = email_subject if email_subject else "백테스트 알림"
        _send_email_notification(subject, full_msg)


def main_loop(
        timeframe: str,
        combo_info: Dict[str, Any],
        start_date_str: str,
        alert_on_same_position: bool
):
    """
    단일 콤보 백테스트 및 결과 처리.
    end_date는 항상 최신(today()) 값으로 설정한다.

    Args:
        timeframe (str): 타임프레임 (예: '1h')
        combo_info (Dict[str, Any]): 콤보 설정
        start_date_str (str): 백테스트 시작 UTC (문자열)
        alert_on_same_position (bool): 동일 포지션 시에도 메일 보내는지 여부
    """
    global _previous_position

    # 매 실행 시점마다 today()로 최신 종료 시각을 설정
    end_date_str = today()

    now_kst = datetime.datetime.now(tz=pytz.timezone("Asia/Seoul"))
    now_kst_str = now_kst.strftime("%Y-%m-%d %H:%M:%S KST")

    logging.info(f"[main_best] 시작: {now_kst_str}, TF={timeframe}, end_date={end_date_str}")

    # DB 업데이트
    try:
        update_data_db(
            symbol=SYMBOL,
            timeframe=timeframe,
            start_str=DB_BOUNDARY_DATE,
            end_str=end_date_str,
            update_mode="recent"
        )
        logging.info("[main_best] DB 업데이트 완료.")
    except Exception as e:
        notify_user(f"[main_best] DB 업데이트 실패: {e}")
        return

    try:
        # 필요한 워밍업 봉 수 계산
        warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)

        # DB에서 데이터 로딩 및 전처리
        df_merged = prepare_ohlcv_with_warmup(
            symbol=SYMBOL,
            timeframe=timeframe,
            start_utc_str=start_date_str,
            end_utc_str=end_date_str,
            warmup_bars=warmup_bars,
            exchange_open_date_utc_str=EXCHANGE_OPEN_DATE,
            boundary_date_utc_str=DB_BOUNDARY_DATE,
            db_path=DB_PATH
        )
        df_merged = clean_ohlcv(df_merged)
        if df_merged.empty:
            notify_user("[main_best] DF가 비어있음.")
            return

        df_ind = calc_all_indicators(df_merged, INDICATOR_CONFIG)

        dt_format = "%Y-%m-%d %H:%M:%S"
        utc = pytz.utc

        naive_start = datetime.datetime.strptime(start_date_str, dt_format)
        start_utc_dt = utc.localize(naive_start)
        start_ms = int(start_utc_dt.timestamp() * 1000)

        naive_end = datetime.datetime.strptime(end_date_str, dt_format)
        end_utc_dt = utc.localize(naive_end)
        end_ms = int(end_utc_dt.timestamp() * 1000)

        df_test = df_ind[
            (df_ind["open_time"] >= start_ms) & (df_ind["open_time"] <= end_ms)
            ].copy()
        df_test.reset_index(drop=True, inplace=True)

        if df_test.empty:
            notify_user("[main_best] 백테스트 구간 DF가 비어있음.")
            return

        # 콤보 + B/H 백테스트 실행
        result = run_best_single(df_test, combo_info)
        combo_score = result["combo_score"]
        combo_position = result["combo_position"]
        combo_trades_log = result["combo_trades_log"]
        bh_score = result["bh_score"]

        # 거래로그 - 최신 5개만 출력
        trades_lines = combo_trades_log.split("\n")
        if len(trades_lines) > 5:
            trades_lines = trades_lines[-5:]

        logging.info("[Combo TradesLog]")
        for ln in trades_lines:
            logging.info(ln)

        logging.info(
            f"[main_best] (Combo) StartC={combo_score['StartCapital']:.2f}, "
            f"EndC={combo_score['EndCapital']:.2f}, "
            f"Return={combo_score['Return']:.4f}, "
            f"Sharpe={combo_score['Sharpe']:.4f}, "
            f"Pos={combo_position}"
        )

        logging.info(
            f"[main_best] (Buy&Hold) StartC={bh_score['StartCapital']:.2f}, "
            f"EndC={bh_score['EndCapital']:.2f}, "
            f"Return={bh_score['Return']:.4f}, "
            f"Sharpe={bh_score['Sharpe']:.4f}"
        )

        # 메일 보낼 내용
        mail_subject = ""
        mail_body = (
                f"백테스트 결과:\n"
                f"- 콤보 StartC={combo_score['StartCapital']:.2f}, "
                f"EndC={combo_score['EndCapital']:.2f}, "
                f"Return={combo_score['Return']:.4f}, "
                f"Sharpe={combo_score['Sharpe']:.4f}, "
                f"Position={combo_position}\n"
                f"- B/H StartC={bh_score['StartCapital']:.2f}, "
                f"EndC={bh_score['EndCapital']:.2f}, "
                f"Return={bh_score['Return']:.4f}, "
                f"Sharpe={bh_score['Sharpe']:.4f}\n"
                f"- 최근 거래로그(5개):\n"
                + "\n".join(trades_lines)
        )

        send_email = False
        if _previous_position is None:
            # 첫 실행
            mail_subject = "[main_best] 첫 실행"
            send_email = True
            notify_user(
                f"첫 실행, 콤보 포지션={combo_position}",
                send_email=False,
                email_subject=mail_subject + " (콘솔)"
            )
        else:
            # 포지션 변경 시
            if combo_position != _previous_position:
                mail_subject = f"[main_best] 포지션 변경: {_previous_position}→{combo_position}"
                send_email = True
                notify_user(
                    f"포지션 변경! {_previous_position} → {combo_position}",
                    send_email=False,
                    email_subject=mail_subject + " (콘솔)"
                )
            else:
                # 동일 포지션일 때
                if alert_on_same_position:
                    mail_subject = "[main_best] 포지션 동일"
                    send_email = True
                    notify_user(
                        f"포지션 동일, 현재={combo_position}",
                        send_email=False,
                        email_subject=mail_subject + " (콘솔)"
                    )
                else:
                    logging.info(f"[main_best] 콤보 포지션 동일: {_previous_position}")

        _previous_position = combo_position

        if send_email and SEND_EMAIL:
            _send_email_notification(mail_subject, mail_body)

    except Exception as ex:
        notify_user(f"[main_best] 처리 오류: {ex}")


def run_main_best_repeated(
        combo_info: Dict[str, Any],
        start_date_str: str,
        interval_seconds: int,
        alert_on_same_position: bool
):
    """
    일정 간격으로 반복 실행.
    end_date는 main_loop 내부에서 항상 today()로 설정한다.

    Args:
        combo_info (Dict[str, Any]): 콤보 설정
        start_date_str (str): 백테스트 시작 시각 (UTC)
        interval_seconds (int): 반복 간격(초)
        alert_on_same_position (bool): 동일 포지션 시에도 알림(메일) 보낼지 여부
    """
    # 기본 타임프레임(예: "1h")이 combo_info에 정의되어 있다고 가정
    timeframe = combo_info.get("timeframe", TIMEFRAMES[0])

    # 최초 한 번 실행
    main_loop(
        timeframe=timeframe,
        combo_info=combo_info,
        start_date_str=start_date_str,
        alert_on_same_position=alert_on_same_position
    )

    # 지정된 주기로 반복
    schedule.every(interval_seconds).seconds.do(
        main_loop,
        timeframe=timeframe,
        combo_info=combo_info,
        start_date_str=start_date_str,
        alert_on_same_position=alert_on_same_position
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    """
    사용 예시:
      python main_best.py

    - 사용자 정의: MY_START_DATE
    - end_date는 제거됨(항상 today())로 처리
    - INTERVAL_SECONDS: 반복 주기(초)
    - ALERT_ON_SAME_POSITION: 동일 포지션 시에도 매번 메일을 보낼지 여부
    - SEND_EMAIL: 메일 발송 여부 (True/False)
    """

    SEND_EMAIL = True  # 메일 발송 여부
    MY_COMBO_INFO = {
        "timeframe": "1h",
        "combo_params": [
            {
                "type": "MACD",
                "fast_period": 8,
                "slow_period": 26,
                "signal_period": 10,
                "buy_time_delay": 0,
                "sell_time_delay": 0,
                "holding_period": float("inf")
            }
        ]
    }

    # 기본값: IS_OOS_BOUNDARY_DATE ~ 오늘
    MY_START_DATE = IS_OOS_BOUNDARY_DATE  # 예: '2023-01-01 00:00:00'
    INTERVAL_SECONDS = 60
    ALERT_ON_SAME_POSITION = False

    logging.info(
        f"[main_best] 스크립트 시작 - TF={MY_COMBO_INFO['timeframe']}, "
        f"start={MY_START_DATE}, interval={INTERVAL_SECONDS}초"
    )

    run_main_best_repeated(
        combo_info=MY_COMBO_INFO,
        start_date_str=MY_START_DATE,
        interval_seconds=INTERVAL_SECONDS,
        alert_on_same_position=ALERT_ON_SAME_POSITION
    )
