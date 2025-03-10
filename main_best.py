# gptbitcoin/main_best.py
# 동일한 백테스트 환경(심볼, DB 설정, 레버리지 등)은 유지하되,
# 사용자가 START_DATE, END_DATE를 직접 지정하도록 변경.
# 5분 간격(schedule) 반복 실행 가능.

import datetime
import platform
import time
from typing import Optional, Dict, Any

import pytz
import schedule

from utils.date_time import today

try:
    from win10toast import ToastNotifier
    _WINDOWS_TOAST_AVAILABLE = True
except ImportError:
    _WINDOWS_TOAST_AVAILABLE = False

# === config.py에서 가져올 항목(심볼, DB 경계, 거래소 오픈일, DB 경로 등) ===
from config.config import (
    SYMBOL,
    TIMEFRAMES,
    DB_BOUNDARY_DATE,
    EXCHANGE_OPEN_DATE,
    DB_PATH,
)
# 보조지표 설정(INDICATOR_CONFIG) - 워밍업 계산용
from config.indicator_config import INDICATOR_CONFIG

# DB 업데이트 (API 요청 → SQLite)
from data.update_data import update_data_db

# 전처리(NaN/이상치)
from data.preprocess import clean_ohlcv

# 지표 계산
from indicators.indicators import calc_all_indicators

# 지표 파라미터(워밍업 봉 계산)
from utils.indicator_utils import get_required_warmup_bars

# DB 병합 로딩
from utils.db_utils import prepare_ohlcv_with_warmup

# 콤보 + B/H 백테스트
from backtest.run_best import run_best_single


_previous_position: Optional[str] = None  # 직전 콤보 포지션 (롱/숏/FLAT) 추적


def _show_windows_toast(title: str, msg: str, duration_sec: int = 10):
    """
    Windows 10에서는 Toast 알림, 그 외에는 콘솔 출력으로 대체.
    """
    if platform.system().lower().startswith("win") and _WINDOWS_TOAST_AVAILABLE:
        toaster = ToastNotifier()
        toaster.show_toast(
            title=title,
            msg=msg,
            duration=duration_sec,
            threaded=True
        )
    else:
        print(f"[TOAST] {title}: {msg}")


def notify_user(message: str):
    """
    콘솔 + (가능 시) Windows Toast 알림
    """
    now_kst = datetime.datetime.now(tz=pytz.timezone("Asia/Seoul"))
    now_str = now_kst.strftime("%Y-%m-%d %H:%M:%S KST")
    full_msg = f"{now_str} - {message}"
    print(f"[ALERT] {full_msg}")
    _show_windows_toast("백테스트 알림", message, duration_sec=15)


def main_loop(
    timeframe: str,
    combo_info: Dict[str, Any],
    start_date_str: str,
    end_date_str: str,
    interval_minutes: int,
    alert_on_same_position: bool
):
    """
    주기적으로 실행되는 메인 로직:
      1) DB update_data_db: DB_BOUNDARY_DATE~end_date_str 구간 삭제 후 재수집
      2) prepare_ohlcv_with_warmup로 old+recent 병합 (워밍업 고려)
      3) clean_ohlcv, 지표 계산 후 백테스트 구간 필터링(start_date_str~end_date_str)
      4) 콤보 + B/H 백테스트
      5) 콘솔 출력 + 포지션 변경 시 알림

    Args:
        timeframe (str): 예) "1d", "4h"
        combo_info (Dict[str, Any]): {
            "timeframe": "...",
            "combo_params": [...]
        }
        start_date_str (str): 백테스트 시작 시각(UTC), "YYYY-MM-DD HH:MM:SS"
        end_date_str (str): 백테스트 종료 시각(UTC), "YYYY-MM-DD HH:MM:SS"
        interval_minutes (int): 반복 주기(분)
        alert_on_same_position (bool): 포지션이 동일해도 알림을 보낼지 여부
    """
    global _previous_position

    now_kst = datetime.datetime.now(tz=pytz.timezone("Asia/Seoul"))
    now_kst_str = now_kst.strftime("%Y-%m-%d %H:%M:%S KST")
    print(f"\n[main_best] 시작: {now_kst_str}, TF={timeframe}, interval={interval_minutes}분")

    # 1) DB 업데이트 (DB_BOUNDARY_DATE ~ end_date_str)
    try:
        update_data_db(
            symbol=SYMBOL,
            timeframe=timeframe,
            start_str=DB_BOUNDARY_DATE,  # UTC
            end_str=end_date_str,        # UTC
            update_mode="recent"         # old_data는 수정 안 함
        )
        print("[main_best] DB 업데이트 완료.")
    except Exception as e:
        notify_user(f"[main_best] DB 업데이트 실패: {e}")
        return

    # 2) prepare_ohlcv_with_warmup
    try:
        warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)
        df_merged = prepare_ohlcv_with_warmup(
            symbol=SYMBOL,
            timeframe=timeframe,
            start_utc_str=start_date_str,   # 사용자 정의
            end_utc_str=end_date_str,       # 사용자 정의
            warmup_bars=warmup_bars,
            exchange_open_date_utc_str=EXCHANGE_OPEN_DATE,
            boundary_date_utc_str=DB_BOUNDARY_DATE,
            db_path=DB_PATH
        )
        df_merged = clean_ohlcv(df_merged)
        if df_merged.empty:
            notify_user("[main_best] DF가 비어있음.")
            return

        # 지표 계산
        df_ind = calc_all_indicators(df_merged, INDICATOR_CONFIG)

        # 백테스트 구간 필터링
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

        # 3) 콤보 + B/H 백테스트
        result = run_best_single(df_test, combo_info)
        combo_score = result["combo_score"]
        combo_position = result["combo_position"]
        combo_trades_log = result["combo_trades_log"]
        bh_score = result["bh_score"]

        # 4) 콘솔 출력
        print("[Combo TradesLog]")
        print(combo_trades_log)
        print(f"[main_best] (Combo) StartC={combo_score['StartCapital']:.2f}, "
              f"EndC={combo_score['EndCapital']:.2f}, Return={combo_score['Return']:.4f}, "
              f"Sharpe={combo_score['Sharpe']:.4f}, Pos={combo_position}")

        print(f"[main_best] (Buy&Hold) StartC={bh_score['StartCapital']:.2f}, "
              f"EndC={bh_score['EndCapital']:.2f}, Return={bh_score['Return']:.4f}, "
              f"Sharpe={bh_score['Sharpe']:.4f}")

        # 5) 포지션 변경 여부 알림
        if _previous_position is None:
            notify_user(f"첫 실행, 콤보 포지션={combo_position}")
        else:
            if combo_position != _previous_position:
                notify_user(f"포지션 변경! {_previous_position} → {combo_position}")
            else:
                if alert_on_same_position:
                    notify_user(f"포지션 동일, 현재={combo_position}")
                else:
                    print(f"[main_best] 콤보 포지션 동일: {_previous_position}")

        _previous_position = combo_position

    except Exception as ex:
        notify_user(f"[main_best] 처리 오류: {ex}")


def run_main_best_repeated(
    combo_info: Dict[str, Any],
    start_date_str: str,
    end_date_str: str,
    interval_minutes: int,
    alert_on_same_position: bool
):
    """
    interval_minutes 간격으로 main_loop를 반복 실행.
    START_DATE, END_DATE를 사용자 입력으로 받아 운영할 수 있도록 함.

    Args:
        combo_info (Dict[str, Any]): 콤보 설정
            예) {
               "timeframe": "1d",
               "combo_params": [
                 {"type": "MA", "short_period": 20, "long_period": 50, "band_filter": 0.0},
                 ...
               ]
            }
        start_date_str (str): 백테스트 시작 시점(UTC) "YYYY-MM-DD HH:MM:SS"
        end_date_str (str): 백테스트 종료 시점(UTC)
        interval_minutes (int): 반복 주기(분)
        alert_on_same_position (bool): 포지션 동일시에도 알림 보낼지 여부
    """
    timeframe = combo_info.get("timeframe", TIMEFRAMES[0])

    # 먼저 1회 실행
    main_loop(
        timeframe=timeframe,
        combo_info=combo_info,
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        interval_minutes=interval_minutes,
        alert_on_same_position=alert_on_same_position
    )

    # 이후 interval_minutes 분마다 반복
    schedule.every(interval_minutes).minutes.do(
        main_loop,
        timeframe=timeframe,
        combo_info=combo_info,
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        interval_minutes=interval_minutes,
        alert_on_same_position=alert_on_same_position
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    """
    실행 예시:
    1) main.py 결과 CSV에서 used_indicators를 복사.
    2) 아래 MY_COMBO_INFO에 붙여넣고,
    3) START_DATE, END_DATE를 원하는 UTC 시각으로 지정.
    4) run_main_best_repeated(...) 호출.

    예) 
    MY_COMBO_INFO = {
      "timeframe": "1d",
      "combo_params": [
        {"type": "MA", "short_period": 5, "long_period": 100, "band_filter": 0.0}
      ]
    }
    MY_START_DATE = "2025-01-01 00:00:00"  # UTC
    MY_END_DATE   = "2025-12-31 23:59:59"  # UTC
    INTERVAL_MINUTES = 5
    ALERT_ON_SAME_POSITION = False
    """
    MY_COMBO_INFO = {"timeframe": "4h", "combo_params": [{"type": "MA", "short_period": 6, "long_period": 12, "band_filter": 0, "buy_time_delay": 0, "sell_time_delay": 0, "holding_period": 24}]}

    # 사용자가 직접 지정
    MY_START_DATE = "2025-03-01 00:00:00"  # UTC 기준
    # MY_END_DATE   = "2025-03-14 23:59:59"  # UTC 기준
    MY_END_DATE   = today()

    INTERVAL_MINUTES = 1
    ALERT_ON_SAME_POSITION = False

    print(f"[main_best] 스크립트 시작 - TF={MY_COMBO_INFO['timeframe']}, "
          f"start={MY_START_DATE}, end={MY_END_DATE}, interval={INTERVAL_MINUTES}분")

    run_main_best_repeated(
        combo_info=MY_COMBO_INFO,
        start_date_str=MY_START_DATE,
        end_date_str=MY_END_DATE,
        interval_minutes=INTERVAL_MINUTES,
        alert_on_same_position=ALERT_ON_SAME_POSITION
    )
