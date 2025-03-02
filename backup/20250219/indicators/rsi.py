# gptbitcoin/indicators/rsi.py

import pandas as pd
import numpy as np

def rsi_signal(
    df,
    length=14,
    overbought=70,
    oversold=30,
    band_filter=0.0,
    delay_filter=0,
    holding_period=0
):
    """
    RSI(Relative Strength Index)를 기반으로
    과매수/과매도 구간을 돌파할 때 매매 신호를 생성한다.

    Parameters
    ----------
    df : pd.DataFrame
        'close' 열을 포함해야 하며, 시계열 순서로 정렬되어 있어야 함.
    length : int, optional
        RSI 계산에 사용할 기간. (기본 14)
    overbought : float, optional
        RSI가 이 값보다 올라가면 과매수 상태. (기본 70)
    oversold : float, optional
        RSI가 이 값보다 내려가면 과매도 상태. (기본 30)
    band_filter : float, optional
        (overbought ± band_filter) 또는 (oversold ± band_filter) 식으로
        일정 추가 범위를 넘어야 유효 신호로 볼 때 사용.
    delay_filter : int, optional
        새 신호가 나온 뒤, 최근 delay_filter개 캔들 연속으로 같은 신호가 있어야 최종 확정.
    holding_period : int or str, optional
        - 정수(6,12 등): 포지션 진입 후 n캔들 동안 반대 신호 무시.
        - 'inf': 시간 제한 없이 반대 신호가 오면 즉시 전환(논문 의도).

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립) 로 구성된 시리즈 (rsi_signal).
    """

    # 1) RSI 계산
    close_diff = df["close"].diff()
    gain = close_diff.where(close_diff > 0, 0.0)
    loss = -close_diff.where(close_diff < 0, 0.0)

    # SMA 기반 평균 상승폭/하락폭
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()

    rs = avg_gain / (avg_loss + 1e-12)  # 0 나눗셈 방지
    rsi = 100 - (100 / (1 + rs))

    # 2) band_filter 적용
    upper_band = overbought + (overbought * band_filter)
    lower_band = oversold   - (oversold   * band_filter)

    # 3) 신호 계산
    #    - RSI가 아래에서 위로 oversold → lower_band 구간 돌파하면 매수(+1)
    #    - RSI가 위에서 아래로 overbought → upper_band 구간 돌파하면 매도(-1)
    #    - 그 외 0
    raw_signal = np.zeros(len(rsi), dtype=int)

    prev_rsi = rsi.shift(1)

    for i in range(len(rsi)):
        if i == 0:
            raw_signal[i] = 0
            continue

        # (이전 RSI < lower_band) & (현재 RSI >= lower_band) → 매수
        if (prev_rsi.iloc[i] < lower_band) and (rsi.iloc[i] >= lower_band):
            raw_signal[i] = 1
        # (이전 RSI > upper_band) & (현재 RSI <= upper_band) → 매도
        elif (prev_rsi.iloc[i] > upper_band) and (rsi.iloc[i] <= upper_band):
            raw_signal[i] = -1
        else:
            raw_signal[i] = 0

    # 4) delay_filter, holding_period 반영
    final_signal = np.zeros_like(raw_signal, dtype=int)
    current_pos = 0
    hold_count = 0

    is_inf = (isinstance(holding_period, str) and holding_period.lower() == "inf")

    for i in range(len(raw_signal)):
        if i == 0:
            final_signal[i] = 0
            continue

        # (1) 정수 holding_period & 보유 중이면 계속 유지
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

            # (3) 새 신호가 이전과 달라지면 포지션 업데이트
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]
                if is_inf:
                    hold_count = 0
                else:
                    hold_count = holding_period if current_pos != 0 else 0

    return pd.Series(final_signal, index=df.index, name="rsi_signal")
