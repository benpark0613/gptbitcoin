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
    OBV를 계산한 뒤, OBV에 대해 단·장기 이동평균을 구하고
    교차 지점에서 매매 신호를 발생시키는 예시 구현.

    Parameters
    ----------
    df : pd.DataFrame
        'close'와 'volume' 열을 포함해야 한다.
        시계열 순서로 정렬되어 있어야 함.
    short_period : int, optional
        OBV에 대한 단기 이동평균 기간 (기본 5)
    long_period : int, optional
        OBV에 대한 장기 이동평균 기간 (기본 20)
    band_filter : float, optional
        (short_ma - long_ma)가 long_ma * band_filter 이상이어야 유효 교차로 인정.
    delay_filter : int, optional
        새 신호가 연속해서 delay_filter개 캔들에서 동일 방향이어야 최종 확정.
    holding_period : int or str, optional
        - 정수(6,12 등): 포지션 진입 후 n캔들 동안 반대 신호 무시
        - 'inf': 시간 제한 없이 반대 신호가 나오면 즉시 전환(논문 의도)

    Returns
    -------
    pd.Series
        +1(매수), -1(매도), 0(중립)로 구성된 시리즈 ('obv_signal').
    """

    # 1) OBV(On-Balance Volume) 계산
    #   - 이전 종가 대비 상승이면 OBV += 현재 거래량
    #   - 이전 종가 대비 하락이면 OBV -= 현재 거래량
    #   - 같으면 변화 없음
    close_diff = df["close"].diff()
    obv = np.zeros(len(df), dtype=float)

    for i in range(1, len(df)):
        if close_diff[i] > 0:
            obv[i] = obv[i - 1] + df["volume"].iloc[i]
        elif close_diff[i] < 0:
            obv[i] = obv[i - 1] - df["volume"].iloc[i]
        else:
            obv[i] = obv[i - 1]

    # 2) OBV에 대해 단기·장기 이동평균 계산
    obv_s = pd.Series(obv).rolling(short_period).mean()
    obv_l = pd.Series(obv).rolling(long_period).mean()

    # 3) band_filter 적용
    #    (obv_s - obv_l)가 (band_filter * obv_l)보다 커야 매수,
    #     반대 방향이면 매도, 그 외 0
    threshold = band_filter * obv_l
    raw_signal = np.where(
        (obv_s - obv_l) > threshold, 1,
        np.where((obv_l - obv_s) > threshold, -1, 0)
    )

    final_signal = np.zeros_like(raw_signal, dtype=int)
    current_pos = 0
    hold_count = 0

    # holding_period가 'inf'인지 확인
    is_inf = (isinstance(holding_period, str) and holding_period.lower() == 'inf')

    for i in range(len(raw_signal)):
        if i == 0:
            final_signal[i] = 0
            continue

        # (1) 정수 holding_period & hold_count가 남아 있으면
        #     반대 신호 무시 (유지)
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

            # (3) 새 신호가 이전 신호와 달라지면 포지션 변경
            if final_signal[i] != final_signal[i - 1]:
                current_pos = final_signal[i]
                if is_inf:
                    hold_count = 0
                else:
                    hold_count = holding_period if current_pos != 0 else 0

    return pd.Series(final_signal, index=df.index, name="obv_signal")
