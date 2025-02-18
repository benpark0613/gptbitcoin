# gptbitcoin/indicators/filter_rule.py

import pandas as pd
import numpy as np

def filter_rule(
    df,
    window=20,
    x=0.05,
    y=0.05,
    band_filter=0.0,
    delay_filter=0,
    holding_period=0
):
    """
    Filter Rule 지표:
      - 최근 window 구간의 최저가(rolling_low)에서 x% 이상 상승 시 매수(+1)
      - 최근 window 구간의 최고가(rolling_high)에서 y% 이상 하락 시 매도(-1)
      - band_filter를 곱해 노이즈를 걸러낼 수도 있음
      - delay_filter, holding_period 로직도 동일하게 적용

    Parameters
    ----------
    df : pd.DataFrame
        'close', 'high', 'low' 열을 포함해야 한다.
    window : int, optional
        최근 n개 캔들로 최저가/최고가를 계산 (기본 20)
    x : float, optional
        (1 + x)배 이상 상승 시 매수 조건 (0.05 => 5% 상승)
    y : float, optional
        (1 - y)배 이하로 하락 시 매도 조건 (0.05 => 5% 하락)
    band_filter : float, optional
        추가 필터(밴드). rolling_low/high에 대해 band_filter 비율만큼
        더 차이 나야 “진짜 돌파”로 인정
    delay_filter : int, optional
        신호가 나온 뒤, 최근 delay_filter개 캔들에서
        같은 신호가 연속 발생해야 최종 확정
    holding_period : int or str, optional
        - 정수(6,12 등): 진입 후 n캔들 동안 반대 신호 무시
        - 'inf': 시간 제한 없이 반대 신호 시 즉시 전환(논문 의도)

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립)로 구성된 시리즈 ('filter_signal').
    """

    # 1) rolling_high, rolling_low 계산 (shift(1)로 '직전' 구간 기준)
    rolling_high = df["high"].rolling(window).max().shift(1)
    rolling_low  = df["low"].rolling(window).min().shift(1)

    # 2) 밴드 필터를 rolling_high/rolling_low에 적용
    #    - 예: rolling_low * (1 + x + band_filter)
    #    - 예: rolling_high * (1 - y - band_filter)
    #    (실제로 밴드 필터를 어떻게 적용할지는 다양하지만,
    #     여기서는 '추가 퍼센트'를 합산/차감하는 방식을 사용)

    buy_thresh  = rolling_low  * (1 + x + band_filter)
    sell_thresh = rolling_high * (1 - y - band_filter)

    # 3) 기본 신호(raw_signal):
    #    - 종가가 buy_thresh 이상이면 매수(+1)
    #    - 종가가 sell_thresh 이하이면 매도(-1)
    #    - 그 외 0
    close_ = df["close"]
    raw_signal = np.where(
        close_ >= buy_thresh, 1,
        np.where(close_ <= sell_thresh, -1, 0)
    )

    # 4) delay_filter, holding_period 적용
    final_signal = np.zeros_like(raw_signal, dtype=int)
    current_pos = 0
    hold_count = 0

    # holding_period가 'inf'인지 여부
    is_inf = (isinstance(holding_period, str) and holding_period.lower() == 'inf')

    for i in range(len(raw_signal)):
        if i == 0:
            # 첫 캔들은 과거 데이터 부족 → 신호 없음
            final_signal[i] = 0
            continue

        # (1) holding_period가 정수 & 보유 중이면
        #     새 신호 무시, 기존 포지션 유지
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

            # (3) 새 신호가 이전 신호와 달라졌다면 포지션 갱신
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]
                if is_inf:
                    # 무한 보유: 반대 신호 나오면 즉시 전환
                    hold_count = 0
                else:
                    hold_count = holding_period if current_pos != 0 else 0

    return pd.Series(final_signal, index=df.index, name="filter_signal")
