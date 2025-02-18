# gptbitcoin/indicators/ma_cross.py

import pandas as pd
import numpy as np

def ma_cross(df, short_period, long_period, band_filter=0.0, delay_filter=0, holding_period=0):
    """
    이동평균 교차 전략.
    short_period, long_period를 사용하여 각각 이동평균(SMA)을 구한 뒤,
    (short_ma - long_ma)가 band_filter * long_ma를 넘으면 매수(+1),
    반대로 -(short_ma - long_ma)가 band_filter * long_ma를 넘으면 매도(-1) 신호를 만든다.

    Parameters
    ----------
    df : pd.DataFrame
        'close' 열을 포함해야 한다.
    short_period : int
        단기 이동평균 기간
    long_period : int
        장기 이동평균 기간
    band_filter : float, optional
        (short_ma - long_ma)가 band_filter * long_ma 이상이어야 유효 교차로 인정
    delay_filter : int, optional
        새 신호가 연속해서 n개 캔들에서 동일해야 최종 확정
    holding_period : int or str, optional
        - int: 포지션 진입 후 n캔들 동안 반대 신호 무시
        - 'inf': 시간 제한 없이, 반대 신호 발생 시 즉시 전환(논문 의도)

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립)로 구성된 신호('ma_cross_signal').
    """
    # 이동평균 계산
    short_ma = df["close"].rolling(short_period).mean()
    long_ma = df["close"].rolling(long_period).mean()

    # band_filter 기준치
    threshold = band_filter * long_ma

    # 기본 교차 신호:
    # (short_ma - long_ma) > threshold → 매수(+1)
    # (long_ma - short_ma) > threshold → 매도(-1)
    # 그 외 0
    raw_signal = np.where(
        (short_ma - long_ma) > threshold, 1,
        np.where((long_ma - short_ma) > threshold, -1, 0)
    )

    final_signal = np.zeros_like(raw_signal, dtype=int)
    current_pos = 0
    hold_count = 0

    # holding_period가 'inf'인지 확인
    is_inf = (isinstance(holding_period, str) and holding_period.lower() == 'inf')

    for i in range(len(raw_signal)):
        if i == 0:
            # 첫 캔들은 과거 데이터 부족 → 신호 없음
            final_signal[i] = 0
            continue

        # (1) holding_period가 정수이고, 아직 보유 기간(hold_count) 남아 있다면
        #     반대 신호가 와도 유지
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
                # delay_filter=0이면, raw_signal 그대로 사용
                final_signal[i] = raw_signal[i]

            # (3) 새 신호가 이전과 달라졌다면 포지션 변경
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]

                if is_inf:
                    # 'inf'인 경우, 시간 제한 없이 반대 신호 즉시 수용
                    hold_count = 0
                else:
                    # 정수 기간이라면 해당 값만큼 유지
                    hold_count = holding_period if current_pos != 0 else 0

    return pd.Series(final_signal, index=df.index, name="ma_cross_signal")
