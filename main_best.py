# gptbitcoin/main_best.py
# 5분마다 특정 콤보 + 바이앤홀드(B/H) 백테스트를 반복 수행
# 콘솔에는 콤보 트레이드 로그, B/H는 요약 성과만 출력
# prepare_ohlcv_with_warmup 함수를 통해
# 워밍업 계산 + 조회 시점 조정 + DB에서 old_data+recent_data 병합 로딩을 한 번에 처리.

import time
import schedule
import platform
import datetime
import pytz
import pandas as pd
from typing import Optional, Dict, Any

try:
    from win10toast import ToastNotifier
    _WINDOWS_TOAST_AVAILABLE = True
except ImportError:
    # Windows 환경이 아니거나 win10toast 패키지가 없는 경우
    _WINDOWS_TOAST_AVAILABLE = False

# 프로젝트 설정값
from config.config import (
    SYMBOL,
    START_DATE,
    DB_BOUNDARY_DATE,
    EXCHANGE_OPEN_DATE,
    INDICATOR_CONFIG,
    DB_PATH
)

# DB 업데이트(바이낸스 선물 API 수집 후 저장)
from data.update_data import update_data_db

# 전처리(NaN/이상치 검사)
from data.preprocess import clean_ohlcv

# 지표 계산
from indicators.indicators import calc_all_indicators

# 지표 파라미터(워밍업 봉 계산)
from utils.indicator_utils import get_required_warmup_bars

# DB 유틸 - prepare_ohlcv_with_warmup
from utils.db_utils import prepare_ohlcv_with_warmup

# 콤보 + B/H 백테스트
from backtest.run_best import run_best_single

_previous_position: Optional[str] = None  # 직전 콤보 포지션 추적


def _show_windows_toast(title: str, msg: str, duration_sec: int = 10):
    """
    Windows 10 환경이면 토스트 알림, 그 외에는 콘솔에 대체 출력.
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
    콘솔 출력 + (Windows 10 + win10toast 지원 시) Toast 알림
    """
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"{now_str} - {message}"
    print(f"[ALERT] {full_msg}")
    _show_windows_toast("백테스트 알림", message, duration_sec=15)


def main_loop(
    timeframe: str,
    combo_info: Dict[str, Any],
    alert_on_same_position: bool
):
    """
    주기적으로 실행할 주요 함수:
      1) DB_BOUNDARY_DATE ~ 현재 UTC 구간만 recent_data 삭제 후 재수집
      2) prepare_ohlcv_with_warmup 함수로 old_data+recent_data 병합 로딩(워밍업 처리)
      3) clean_ohlcv + 보조지표 계산 + 백테스트 구간 필터링
      4) 콤보 + B/H 백테스트
      5) 콘솔 출력(콤보는 트레이드 로그, B/H는 요약)
      6) 직전 포지션 대비 변경 시 notify_user
    """
    global _previous_position

    # 현재 UTC 시각
    now_utc = datetime.datetime.utcnow()
    now_utc_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")

    # 콘솔용 KST 시각
    kst_zone = pytz.timezone("Asia/Seoul")
    now_kst = now_utc.replace(tzinfo=pytz.utc).astimezone(kst_zone)
    now_kst_str = now_kst.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n[main_best] 시작: {now_kst_str} (KST), TF={timeframe}")

    # (1) DB 업데이트 (DB_BOUNDARY_DATE~현재 UTC 구간)
    try:
        update_data_db(
            symbol=SYMBOL,
            timeframe=timeframe,
            start_str=DB_BOUNDARY_DATE,  # UTC
            end_str=now_utc_str,         # UTC
            update_mode="recent"
        )
        print("[main_best] DB 업데이트 완료.")
    except Exception as e:
        notify_user(f"[main_best] DB 업데이트 실패: {e}")
        return

    # (2) prepare_ohlcv_with_warmup로 DB에서 병합 로딩
    try:
        warmup_bars = get_required_warmup_bars(INDICATOR_CONFIG)

        df_merged = prepare_ohlcv_with_warmup(
            symbol=SYMBOL,
            timeframe=timeframe,
            start_utc_str=START_DATE,            # 백테스트 메인 시작(UTC)
            end_utc_str=now_utc_str,            # 현재 시각(UTC)
            warmup_bars=warmup_bars,
            exchange_open_date_utc_str=EXCHANGE_OPEN_DATE,
            boundary_date_utc_str=DB_BOUNDARY_DATE,
            db_path=DB_PATH
        )

        # 전처리(NaN/이상치 검사)
        df_merged = clean_ohlcv(df_merged)
        if df_merged.empty:
            notify_user("[main_best] DF가 비어 있습니다.")
            return

        # 지표 계산
        df_ind = calc_all_indicators(df_merged, INDICATOR_CONFIG)

        # 백테스트 구간 필터링(START_DATE ~ 현재 시각)
        dt_format = "%Y-%m-%d %H:%M:%S"
        utc = pytz.utc

        naive_start = datetime.datetime.strptime(START_DATE, dt_format)
        start_utc_dt = utc.localize(naive_start)
        start_ms = int(start_utc_dt.timestamp() * 1000)

        naive_end = datetime.datetime.strptime(now_utc_str, dt_format)
        end_utc_dt = utc.localize(naive_end)
        end_ms = int(end_utc_dt.timestamp() * 1000)

        df_test = df_ind[
            (df_ind["open_time"] >= start_ms) & (df_ind["open_time"] <= end_ms)
        ].copy()
        df_test.reset_index(drop=True, inplace=True)

        if df_test.empty:
            notify_user("[main_best] 백테스트 구간 DF가 비어 있음.")
            return

        # (3) 콤보 + B/H 백테스트
        result = run_best_single(df_test, combo_info)
        combo_score = result["combo_score"]
        combo_position = result["combo_position"]
        combo_trades_log = result["combo_trades_log"]
        bh_score = result["bh_score"]

        # (4) 콘솔 출력
        print("[Combo TradesLog]")
        print(combo_trades_log)
        print(f"[main_best] (Combo) StartC={combo_score['StartCapital']:.2f}, "
              f"EndC={combo_score['EndCapital']:.2f}, Return={combo_score['Return']:.4f}, "
              f"Sharpe={combo_score['Sharpe']:.4f}, Pos={combo_position}")

        print(f"[main_best] (Buy&Hold) StartC={bh_score['StartCapital']:.2f}, "
              f"EndC={bh_score['EndCapital']:.2f}, Return={bh_score['Return']:.4f}, "
              f"Sharpe={bh_score['Sharpe']:.4f}")

        # (5) 직전 포지션 대비 변경 체크 -> 알림
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
        notify_user(f"[main_best] 처리 중 오류: {ex}")


def run_main_best_repeated(
    timeframe: str,
    combo_info: Dict[str, Any],
    interval_minutes: int,
    alert_on_same_position: bool
):
    """
    interval_minutes 간격으로 main_loop를 반복 실행.
    """
    # 최초 1회 즉시 실행
    main_loop(timeframe, combo_info, alert_on_same_position)

    schedule.every(interval_minutes).minutes.do(
        main_loop,
        timeframe=timeframe,
        combo_info=combo_info,
        alert_on_same_position=alert_on_same_position
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    """
    사용자 설정:
      - TIMEFRAME
      - 콤보 파라미터(MY_COMBO_INFO)
      - INTERVAL_MINUTES
      - ALERT_ON_SAME_POSITION
    """
    TIMEFRAME = "15m"
    MY_COMBO_INFO = {
        "timeframe": TIMEFRAME,
        "combo_params": [
            {"type": "RSI", "length": 21, "overbought": 80, "oversold": 20},
            {"type": "Filter", "window": 20, "x_pct": 0.05, "y_pct": 0.1},
            {"type": "Support_Resistance", "window": 20, "band_pct": 0.02}
        ]
    }
    INTERVAL_MINUTES = 5
    ALERT_ON_SAME_POSITION = False

    print(f"[main_best] 스크립트 시작. TF={TIMEFRAME}, interval={INTERVAL_MINUTES}분")

    run_main_best_repeated(
        timeframe=TIMEFRAME,
        combo_info=MY_COMBO_INFO,
        interval_minutes=INTERVAL_MINUTES,
        alert_on_same_position=ALERT_ON_SAME_POSITION
    )
