# gptbitcoin/indicators/support_resistance.py

import pandas as pd
import numpy as np


def support_resistance(df, window, band_filter=0.0, delay_filter=0, holding_period=0):
    """
    일정 기간(window) 동안의 최고가/최저가를 기준으로 지지/저항선을 구하고,
    해당 구간을 돌파 시 매수(+1), 하향 돌파 시 매도(-1) 신호를 발생시킨다.

    Parameters
    ----------
    df : pd.DataFrame
        'close', 'high', 'low' 열을 포함해야 한다.
    window : int
        최근 n개 캔들 기준으로 최고가/최저가 계산.
    band_filter : float, optional
        가격이 rolling_high/rolling_low를 넘어설 때
        (band_filter * rolling_high/low)만큼 추가로 넘어야 유효 돌파로 간주.
    delay_filter : int, optional
        신호가 바뀌었을 때, 최근 delay_filter개 캔들에서
        동일 방향 신호가 연속해서 관찰되어야 최종 확정.
    holding_period : int or str, optional
        - int(6,12 등): 포지션 진입 후 최소한 그 캔들 수 동안 유지.
          예) holding_period=6 → 6캔들 동안 반대 신호 무시
        - 'inf': 시간 제한 없이, 반대 신호가 나오면 즉시 변경(논문 의도).
          즉, 최소 보유 기간이 없고 다음 신호가 나오면 바로 전환.

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립)로 구성된 시리즈('sr_signal').
    """

    rolling_high = df["high"].rolling(window).max().shift(1)
    rolling_low  = df["low"].rolling(window).min().shift(1)

    upper_thresh = rolling_high + (band_filter * rolling_high)
    lower_thresh = rolling_low  - (band_filter * rolling_low)

    # S&R 기본 신호: 현재 종가가 upper_thresh보다 크면 +1, lower_thresh보다 작으면 -1, 나머지는 0
    raw_signal = np.where(
        df["close"] > upper_thresh, 1,
        np.where(df["close"] < lower_thresh, -1, 0)
    )

    final_signal = np.zeros_like(raw_signal, dtype=int)
    current_pos = 0
    hold_count = 0

    # holding_period가 'inf'인지 여부를 체크
    is_inf = (isinstance(holding_period, str) and holding_period.lower() == 'inf')

    for i in range(len(raw_signal)):
        if i == 0:
            final_signal[i] = 0  # 첫 캔들은 과거 데이터 부족으로 신호 보류
            continue

        # 1) holding_period가 정수형이고, 아직 유지해야 할 hold_count가 남아 있으면,
        #    그대로 현재 포지션 유지 (새 신호 무시)
        if (not is_inf) and (holding_period > 0) and (hold_count > 0):
            final_signal[i] = current_pos
            hold_count -= 1

        else:
            # 2) delay_filter가 있으면, 최근 delay_filter개 캔들의 raw_signal이 모두 동일해야만 새 신호 확정
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
                # delay_filter가 0이면 raw_signal을 그대로
                final_signal[i] = raw_signal[i]

            # 3) 새 신호가 이전 신호와 달라졌다면, 포지션 변경
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]

                # holding_period가 'inf'이면 시간 제한 없이 반대 신호 시 즉시 바꿈(hold_count는 0)
                if is_inf:
                    hold_count = 0
                else:
                    # 일반 정수 holding_period라면, 포지션 잡을 때마다 hold_count 재설정
                    hold_count = holding_period if current_pos != 0 else 0

    return pd.Series(final_signal, index=df.index, name="sr_signal")
