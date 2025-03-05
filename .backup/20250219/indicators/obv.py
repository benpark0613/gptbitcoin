# gptbitcoin/indicators/obv.py

import pandas as pd
import numpy as np

def obv_signal(
    df,
    short_period=5,
    long_period=20,
    band_filter=0.0,
    delay_filter=0,
    holding_period=0
):
    """
    OBV(On-Balance Volume)를 계산하여, 단·장기 이동평균 교차로 매매 신호 발생.

    Parameters
    ----------
    df : pd.DataFrame
        'close'와 'volume' 열을 포함해야 하며,
        시계열 순서로 정렬된 상태여야 함.
    short_period : int
        OBV 단기 이동평균 기간
    long_period : int
        OBV 장기 이동평균 기간
    band_filter : float
        (obv_s - obv_l)가 long_period MA * band_filter 이상이어야 매수/매도 확정
    delay_filter : int
        새 신호가 연속 delay_filter개 캔들에서 동일해야 최종 확정
    holding_period : int or str
        - 정수: 진입 후 n캔들 동안 반대 신호 무시
        - 'inf': 시간 제한 없이, 반대 신호 발생 시 즉시 전환

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립) 시그널 시리즈(obv_signal).
    """

    # 1) close_diff
    close_diff = df["close"].diff()

    # 2) OBV 배열 생성
    #    - 직전 OBV 값에 +volume / -volume / 0
    #    - .iloc[i]로 위치 기반 접근
    obv = np.zeros(len(df), dtype=float)
    for i in range(1, len(df)):
        if close_diff.iloc[i] > 0:
            obv[i] = obv[i - 1] + df["volume"].iloc[i]
        elif close_diff.iloc[i] < 0:
            obv[i] = obv[i - 1] - df["volume"].iloc[i]
        else:
            obv[i] = obv[i - 1]

    # 3) OBV 단·장기 이동평균
    obv_s = pd.Series(obv).rolling(short_period).mean()
    obv_l = pd.Series(obv).rolling(long_period).mean()

    # 4) band_filter 적용
    threshold = band_filter * obv_l
    #   obv_s > obv_l + threshold -> 매수(+1)
    #   obv_s < obv_l - threshold -> 매도(-1)
    #   그 외 0
    raw_signal = np.where(
        (obv_s - obv_l) > threshold, 1,
        np.where((obv_l - obv_s) > threshold, -1, 0)
    )

    # 5) delay_filter, holding_period 반영
    final_signal = np.zeros_like(raw_signal, dtype=int)
    current_pos = 0
    hold_count = 0

    # holding_period가 'inf'인지 확인
    is_inf = (isinstance(holding_period, str) and holding_period.lower() == 'inf')

    for i in range(len(raw_signal)):
        if i == 0:
            final_signal[i] = 0
            continue

        # (1) holding_period(정수) & 남은 hold_count 있으면, 반대 신호 무시
        if (not is_inf) and (holding_period > 0) and (hold_count > 0):
            final_signal[i] = current_pos
            hold_count -= 1

        else:
            # (2) delay_filter 적용
            if delay_filter > 0 and i >= delay_filter:
                candidate = raw_signal[i]
                consistent = True
                for j in range(i - delay_filter + 1, i + 1):
                    if raw_signal[j] != candidate:
                        consistent = False
                        break
                if consistent:
                    final_signal[i] = candidate
                else:
                    final_signal[i] = final_signal[i - 1]
            else:
                final_signal[i] = raw_signal[i]

            # (3) 포지션 변경 시 hold_count 재설정
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]
                if is_inf:
                    hold_count = 0
                else:
                    hold_count = holding_period if current_pos != 0 else 0

    # 최종 시그널: pd.Series 형태로 반환
    return pd.Series(final_signal, index=df.index, name="obv_signal")
