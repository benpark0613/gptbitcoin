# gptbitcoin/indicators/channel_breakout.py

import pandas as pd
import numpy as np

def channel_breakout(
    df,
    window=20,
    c_value=0.05,
    band_filter=0.0,
    delay_filter=0,
    holding_period=0
):
    """
    Channel Breakout 지표:
      - 최근 window 구간의 최고가/최저가로 "채널"을 정의
      - (rolling_high - rolling_low)가 c_value 비율 이하일 때,
        즉 '좁은 채널'이라고 판단
      - 그 채널 상단/하단을 돌파하면 매매 신호 발생

    Parameters
    ----------
    df : pd.DataFrame
        'high', 'low', 'close' 열을 포함해야 한다.
    window : int, optional
        최근 n개 캔들로 채널(최고/최저)을 계산 (기본 20)
    c_value : float, optional
        채널 폭(rolling_high - rolling_low)이
        rolling_low * c_value 이하일 때만 "채널"로 인정.
        예) c_value=0.05 → (rolling_high - rolling_low) <= 0.05 * rolling_low
    band_filter : float, optional
        상단/하단 돌파 시 band_filter * rolling_high(or rolling_low)만큼
        추가로 넘어야 유효 신호로 인정 (노이즈 필터)
    delay_filter : int, optional
        최근 delay_filter개 캔들에서 같은 신호가 연속해야 최종 확정
    holding_period : int or str, optional
        - 정수(6,12 등): 포지션 진입 후 n캔들 동안 반대 신호 무시
        - 'inf': 시간 제한 없이 반대 신호가 오면 즉시 전환(논문 의도)

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립)로 구성된 시리즈 ('channel_signal').
    """

    # 1) rolling_high, rolling_low 계산 (shift(1)로 직전 구간 기준)
    rolling_high = df["high"].rolling(window).max().shift(1)
    rolling_low  = df["low"].rolling(window).min().shift(1)

    # 2) 채널 폭이 c_value 비율 이하인지 판별
    #    예: (rolling_high - rolling_low) <= c_value * rolling_low
    channel_range = rolling_high - rolling_low
    channel_condition = (channel_range <= c_value * rolling_low)

    # 3) band_filter 적용
    upper_thresh = rolling_high + (band_filter * rolling_high)
    lower_thresh = rolling_low  - (band_filter * rolling_low)

    # 4) 기본 신호(raw_signal):
    #    - 채널 조건이 True여야만 유효
    #    - 종가가 upper_thresh 돌파 → +1
    #    - 종가가 lower_thresh 하향 돌파 → -1
    #    - 그 외 0
    close_ = df["close"]
    raw_signal = np.where(
        (channel_condition & (close_ > upper_thresh)), 1,
        np.where((channel_condition & (close_ < lower_thresh)), -1, 0)
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

        # (1) holding_period가 정수 & 남은 hold_count 있으면
        #     반대 신호 무시하고 계속 유지
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
                # delay_filter 없으면 raw_signal 그대로
                final_signal[i] = raw_signal[i]

            # (3) 새 신호가 이전과 달라졌으면 포지션 업데이트
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]
                if is_inf:
                    # 무한 보유: 시간 제한 없이 반대 신호 즉시 수용
                    hold_count = 0
                else:
                    # 정수 holding_period
                    hold_count = holding_period if current_pos != 0 else 0

    return pd.Series(final_signal, index=df.index, name="channel_signal")
