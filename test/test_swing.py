import os
import pyupbit
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from typing import List

# ===================================================
# (1) 환경 변수 로드 & 업비트 인증 키 설정
# ===================================================
load_dotenv()
access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")

upbit = pyupbit.Upbit(access, secret)

# ===================================================
# (2) 차트 패턴 분석용 기본 클래스 & 구현체
# ===================================================
class ChartPattern:
    def __init__(self, name: str, window_size: int):
        """
        name: 패턴 이름
        window_size: calc_similarity()에서 분석할 최근 봉 개수
        """
        self.name = name
        self.window_size = window_size

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        """
        df: 전체 OHLCV 데이터
        return: (유사도 점수(0~100), 사용한 구간 시작 시각, 사용한 구간 종료 시각)
        """
        raise NotImplementedError("패턴별로 로직을 구현해주세요.")


# ---------------------------------------------------
# 1. Double Top
# ---------------------------------------------------
class DoubleTopPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Double Top", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        # 최근 window_size 봉만 사용
        subset = df.iloc[-self.window_size:]
        sub_start = subset.index[0]
        sub_end = subset.index[-1]

        highs = subset['high'].values
        closes = subset['close'].values
        length = len(highs)

        idx_peak1 = np.argmax(highs)
        peak1 = highs[idx_peak1]

        # 첫 번째 고점이 너무 뒤쪽(끝에서 5봉 이내)이면 패턴 미완성
        if idx_peak1 >= length - 5:
            return 0.0, sub_start, sub_end

        # 두 번째 고점
        second_part_highs = highs[idx_peak1+1:]
        idx_peak2_sub = np.argmax(second_part_highs)
        idx_peak2 = idx_peak1 + 1 + idx_peak2_sub
        peak2 = highs[idx_peak2]

        if idx_peak1 == idx_peak2:
            return 0.0, sub_start, sub_end

        # 고점 차이 비율
        avg_peak = (peak1 + peak2) / 2
        delta_tops = abs(peak1 - peak2)
        ratio_tops = delta_tops / avg_peak

        # 목선 찾기
        left_idx = min(idx_peak1, idx_peak2)
        right_idx = max(idx_peak1, idx_peak2)
        neck_min = np.min(subset['low'].values[left_idx:right_idx+1])

        ratio_neck = (avg_peak - neck_min) / avg_peak
        recent_close = closes[-1]
        broke_neck = (recent_close < neck_min)

        score = 0.0

        # (A) 두 고점 근접도
        if ratio_tops < 0.003:
            score += 25
        elif ratio_tops < 0.005:
            score += 15

        # (B) 목선~고점 괴리
        if ratio_neck >= 0.01:
            score += 30
        elif ratio_neck >= 0.005:
            score += 15

        # (C) 목선 하락 돌파 여부
        if broke_neck:
            score += 40

        score = min(score, 100)
        return score, sub_start, sub_end


# ---------------------------------------------------
# 2. Double Bottom
# ---------------------------------------------------
class DoubleBottomPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Double Bottom", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        # 간단 예시 (Double Bottom은 Double Top의 반대 논리)
        subset = df.iloc[-self.window_size:]
        sub_start = subset.index[0]
        sub_end = subset.index[-1]

        lows = subset['low'].values
        closes = subset['close'].values
        length = len(lows)

        idx_valley1 = np.argmin(lows)
        valley1 = lows[idx_valley1]

        if idx_valley1 >= length - 5:
            return 0.0, sub_start, sub_end

        second_part_lows = lows[idx_valley1+1:]
        idx_valley2_sub = np.argmin(second_part_lows)
        idx_valley2 = idx_valley1 + 1 + idx_valley2_sub
        valley2 = lows[idx_valley2]

        if idx_valley1 == idx_valley2:
            return 0.0, sub_start, sub_end

        # 저점 차이
        avg_valley = (valley1 + valley2) / 2
        delta_lows = abs(valley1 - valley2)
        ratio_lows = delta_lows / avg_valley

        # 목선(가장 높은 고점)
        left_idx = min(idx_valley1, idx_valley2)
        right_idx = max(idx_valley1, idx_valley2)
        neck_max = np.max(subset['high'].values[left_idx:right_idx+1])
        ratio_neck = (neck_max - avg_valley) / avg_valley

        recent_close = closes[-1]
        broke_neck = (recent_close > neck_max)

        score = 0.0

        # (A) 두 저점 근접도
        if ratio_lows < 0.003:
            score += 25
        elif ratio_lows < 0.005:
            score += 15

        # (B) 목선~저점 괴리
        if ratio_neck >= 0.01:
            score += 30
        elif ratio_neck >= 0.005:
            score += 15

        # (C) 목선 돌파 여부
        if broke_neck:
            score += 40

        score = min(score, 100)
        return score, sub_start, sub_end


# ---------------------------------------------------
# 3. Ascending Triangle
# ---------------------------------------------------
class AscendingTrianglePattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Ascending Triangle", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 여기는 간단 “가짜” 로직 (고점 수평 & 저점 상승 여부 정도만 점검)
        highs = subset['high'].values
        lows = subset['low'].values
        # 1) 고점의 분산이 적은지
        top_std = np.std(highs)
        # 2) 저점이 우상향하는지 (단순: 첫 lows vs 마지막 lows)
        bottom_trend = (lows[-1] - lows[0]) / lows[0]

        score = 0.0
        # 고점 표준편차가 적으면 -> 수평선 가능성
        if top_std < 0.3:  # 임의 수치
            score += 40
        # 저점 우상향 비율이 5% 이상이면
        if bottom_trend > 0.05:
            score += 40

        # 최종 가중
        score = min(score, 100)
        return score, sub_start, sub_end


# ---------------------------------------------------
# 4. Descending Triangle
# ---------------------------------------------------
class DescendingTrianglePattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Descending Triangle", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 반대로 저점 수평 & 고점 하락
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        highs = subset['high'].values
        lows = subset['low'].values

        low_std = np.std(lows)
        top_trend = (highs[-1] - highs[0]) / highs[0]  # 음수면 하락 중

        score = 0.0
        if low_std < 0.3:
            score += 40
        if top_trend < -0.05:
            score += 40

        score = min(score, 100)
        return score, sub_start, sub_end


# ---------------------------------------------------
# 5. Symmetrical Triangle
# ---------------------------------------------------
class SymmetricalTrianglePattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Symmetrical Triangle", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 고점 하락 & 저점 상승
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        highs = subset['high'].values
        lows = subset['low'].values

        top_trend = (highs[-1] - highs[0]) / highs[0]
        bot_trend = (lows[-1] - lows[0]) / lows[0]

        # 단순화: 고점 하락 & 저점 상승이면 대칭삼각형 경향
        score = 0.0
        if top_trend < -0.02 and bot_trend > 0.02:
            score += 80

        return score, sub_start, sub_end


# ---------------------------------------------------
# 6. Rising Wedge
# ---------------------------------------------------
class RisingWedgePattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Rising Wedge", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 상승 쐐기: 고점/저점 모두 상승하지만, 폭이 좁아짐
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values
        lows = subset['low'].values

        # 단순 예시: 첫~마지막 고점, 첫~마지막 저점의 기울기 비교
        wedge_top_slope = (highs[-1] - highs[0]) / (self.window_size)
        wedge_bot_slope = (lows[-1] - lows[0]) / (self.window_size)

        # 기울기가 둘 다 +지만, top_slope < bot_slope면 (사실 이상하긴)
        # 엄격 로직을 위해선 중간봉 개별 기울기 비교도 필요
        score = 0.0
        if wedge_top_slope > 0 and wedge_bot_slope > 0:
            # 폭이 점점 좁아진다는 조건은 생략(더미)
            score += 60

        return score, sub_start, sub_end


# ---------------------------------------------------
# 7. Falling Wedge
# ---------------------------------------------------
class FallingWedgePattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Falling Wedge", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 하락 쐐기: 고점/저점 모두 하락, 폭이 좁아짐
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values
        lows = subset['low'].values

        top_slope = (highs[-1] - highs[0]) / (self.window_size)
        bot_slope = (lows[-1] - lows[0]) / (self.window_size)

        score = 0.0
        if top_slope < 0 and bot_slope < 0:
            score += 60

        return score, sub_start, sub_end


# ---------------------------------------------------
# 8. Bullish Flag
# ---------------------------------------------------
class BullishFlagPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Bullish Flag", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 강한 상승 이후 짧은 횡보/조정이면 점수 부여
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        closes = subset['close'].values

        # "급등" 판단(처음 ~ 중간) & "횡보" 판단(중간 ~ 끝)
        mid = self.window_size // 2
        first_part = closes[:mid]
        second_part = closes[mid:]

        rise_rate = (first_part[-1] - first_part[0]) / first_part[0]
        second_range = np.max(second_part) - np.min(second_part)

        score = 0.0
        # 첫 파트 5%이상 상승
        if rise_rate > 0.05:
            score += 50
        # 두 번째 파트 변동폭이 상대적으로 작으면
        if second_range < (first_part[-1] * 0.03):
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 9. Bearish Flag
# ---------------------------------------------------
class BearishFlagPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Bearish Flag", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 강한 하락 뒤 짧은 횡보/조정
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        closes = subset['close'].values

        mid = self.window_size // 2
        first_part = closes[:mid]
        second_part = closes[mid:]

        fall_rate = (first_part[-1] - first_part[0]) / first_part[0]
        second_range = np.max(second_part) - np.min(second_part)

        score = 0.0
        if fall_rate < -0.05:
            score += 50
        if second_range < abs(first_part[-1] * 0.03):
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 10. Triple Top
# ---------------------------------------------------
class TripleTopPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Triple Top", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 3번의 고점이 유사해야 함(간단 스터빙)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        highs = subset['high'].values
        # 임의로 3등분
        part_len = self.window_size // 3
        segment1 = highs[:part_len]
        segment2 = highs[part_len:part_len*2]
        segment3 = highs[part_len*2:]

        max1 = np.max(segment1)
        max2 = np.max(segment2)
        max3 = np.max(segment3)

        avg_tops = (max1 + max2 + max3)/3
        diff1 = abs(max1 - avg_tops) / avg_tops
        diff2 = abs(max2 - avg_tops) / avg_tops
        diff3 = abs(max3 - avg_tops) / avg_tops

        # 세 고점이 avg_tops와 모두 1% 이내면 높은 점수
        cond = (diff1 < 0.01) and (diff2 < 0.01) and (diff3 < 0.01)
        score = 80.0 if cond else 10.0

        return score, sub_start, sub_end


# ---------------------------------------------------
# 11. Triple Bottom
# ---------------------------------------------------
class TripleBottomPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Triple Bottom", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 3번의 저점이 유사해야 함
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        lows = subset['low'].values
        part_len = self.window_size // 3
        seg1 = lows[:part_len]
        seg2 = lows[part_len:part_len*2]
        seg3 = lows[part_len*2:]

        min1 = np.min(seg1)
        min2 = np.min(seg2)
        min3 = np.min(seg3)

        avg_val = (min1 + min2 + min3)/3
        d1 = abs(min1 - avg_val)/avg_val
        d2 = abs(min2 - avg_val)/avg_val
        d3 = abs(min3 - avg_val)/avg_val

        cond = (d1 < 0.01) and (d2 < 0.01) and (d3 < 0.01)
        score = 80.0 if cond else 10.0

        return score, sub_start, sub_end


# ---------------------------------------------------
# 12. Head & Shoulders
# ---------------------------------------------------
class HeadAndShouldersPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Head & Shoulders", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        # 매우 단순화
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        closes = subset['close'].values
        head = np.max(closes)
        left_mean = closes[:self.window_size//2].mean()
        right_mean = closes[self.window_size//2:].mean()

        score = 0.0
        if head > left_mean * 1.03 and head > right_mean * 1.03:
            score = 70.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 13. Inverse Head & Shoulders
# ---------------------------------------------------
class InverseHeadAndShouldersPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Inverse H&S", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 간단 반대 로직
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        closes = subset['close'].values

        head = np.min(closes)
        left_mean = closes[:self.window_size//2].mean()
        right_mean = closes[self.window_size//2:].mean()

        score = 0.0
        if head < left_mean * 0.97 and head < right_mean * 0.97:
            score = 70.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 14. Bullish Pennant
# ---------------------------------------------------
class BullishPennantPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Bullish Pennant", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # Bullish Flag과 유사하게 간단
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 급등 + 짧은 수렴
        mid = self.window_size // 2
        first_part = closes[:mid]
        second_part = closes[mid:]
        rise_rate = (first_part[-1] - first_part[0]) / first_part[0]

        range2 = np.max(second_part) - np.min(second_part)
        score = 0.0
        if rise_rate > 0.05:
            score += 50
        if range2 < first_part[-1] * 0.03:
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 15. Bearish Pennant
# ---------------------------------------------------
class BearishPennantPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Bearish Pennant", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # Bearish Flag과 유사
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        mid = self.window_size // 2
        first_part = closes[:mid]
        second_part = closes[mid:]
        fall_rate = (first_part[-1] - first_part[0]) / first_part[0]

        range2 = np.max(second_part) - np.min(second_part)
        score = 0.0
        if fall_rate < -0.05:
            score += 50
        if range2 < abs(first_part[-1] * 0.03):
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 16. Bullish Rectangle
# ---------------------------------------------------
class BullishRectanglePattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Bullish Rectangle", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 단순: 횡보 구간 + 이전 상승 (더미)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        closes = subset['close'].values

        # 이전 상승 판단
        first_half = closes[:self.window_size//2]
        rise_rate = (first_half[-1] - first_half[0]) / first_half[0]

        # 후반 횡보
        second_half = closes[self.window_size//2:]
        range2 = np.ptp(second_half)  # peak-to-peak

        score = 0.0
        if rise_rate > 0.03:
            score += 40
        if range2 < first_half[-1] * 0.02:
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 17. Bearish Rectangle
# ---------------------------------------------------
class BearishRectanglePattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Bearish Rectangle", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 하락 + 횡보
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        first_half = closes[:self.window_size//2]
        drop_rate = (first_half[-1] - first_half[0]) / first_half[0]
        second_half = closes[self.window_size//2:]
        range2 = np.ptp(second_half)

        score = 0.0
        if drop_rate < -0.03:
            score += 40
        if range2 < abs(first_half[-1] * 0.02):
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 18. Rounding Top
# ---------------------------------------------------
class RoundingTopPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Rounding Top", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 천장 부근이 완만한 곡선
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values

        # 가운데 부분이 가장 높고 양끝이 낮은지 대충 확인(간단)
        mid_idx = self.window_size // 2
        left_half = highs[:mid_idx]
        right_half = highs[mid_idx:]
        condition = (np.argmax(left_half) < mid_idx-1) and (np.argmax(right_half) == 0)
        score = 70.0 if condition else 10.0

        return score, sub_start, sub_end


# ---------------------------------------------------
# 19. Rounding Bottom
# ---------------------------------------------------
class RoundingBottomPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Rounding Bottom", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 바닥 부근이 완만한 곡선
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        lows = subset['low'].values

        mid_idx = self.window_size // 2
        left_half = lows[:mid_idx]
        right_half = lows[mid_idx:]
        condition = (np.argmin(left_half) == mid_idx-1) and (np.argmin(right_half) == 0)
        score = 70.0 if condition else 10.0

        return score, sub_start, sub_end


# ---------------------------------------------------
# 20. Island Reversal
# ---------------------------------------------------
class IslandReversalPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Island Reversal", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 간단화: 갭다운 후 횡보, 다시 갭업
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        opens_ = subset['open'].values
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # first gap, second gap 임의 확인
        score = 0.0
        if (opens_[1] < closes[0]*0.98) and (opens_[-1] > closes[-2]*1.02):
            score = 80.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 21. Diamond Top
# ---------------------------------------------------
class DiamondTopPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Diamond Top", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 넓어졌다가 좁아지는 변동성
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        rng = np.ptp(subset['high'].values)  # high range
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 간단 예시
        # 변동성(앞부분) > 변동성(뒷부분) 이면 다이아 톱 형태라 가정
        half = self.window_size//2
        front_range = (subset['high'].values[:half].max() - subset['low'].values[:half].min())
        back_range = (subset['high'].values[half:].max() - subset['low'].values[half:].min())

        score = 0.0
        if front_range > back_range*1.5:
            score = 70.0

        return score, sub_start, sub_end


# ---------------------------------------------------
# 22. Diamond Bottom
# ---------------------------------------------------
class DiamondBottomPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Diamond Bottom", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 넓어졌다가 좁아지는 변동성 (바닥)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        half = self.window_size//2
        front_range = (subset['high'].values[:half].max() - subset['low'].values[:half].min())
        back_range = (subset['high'].values[half:].max() - subset['low'].values[half:].min())

        score = 0.0
        if front_range > back_range*1.5:
            score = 70.0

        sub_start, sub_end = subset.index[0], subset.index[-1]
        return score, sub_start, sub_end


# ---------------------------------------------------
# 23. Cup & Handle
# ---------------------------------------------------
class CupAndHandlePattern(ChartPattern):
    def __init__(self, window_size=60):
        super().__init__(name="Cup & Handle", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 장기간 U자 + 손잡이
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 여기서는 "왼쪽 하락, 바닥, 오른쪽 상승" + "짧은 조정" 정도만 단순 검사
        half = self.window_size//2
        left_trend = (closes[half] - closes[0]) / closes[0]
        right_trend = (closes[-1] - closes[half]) / closes[half]

        # 짧은 조정 (임의)
        handle_range = np.ptp(closes[-(self.window_size//5):])
        avg_price = np.mean(closes)
        ratio_handle = handle_range / avg_price

        score = 0.0
        if left_trend < 0 and right_trend > 0:
            score += 50
        if ratio_handle < 0.05:
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 24. Broadening Top
# ---------------------------------------------------
class BroadeningTopPattern(ChartPattern):
    def __init__(self, window_size=60):
        super().__init__(name="Broadening Top", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 변동성 점차 확대(고점 높아지고 저점 낮아짐)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values
        lows = subset['low'].values

        # 앞에서부터 고점이 점차 높아지고, 저점이 점차 낮아지면 스코어
        # 매우 단순
        peak_increase = (highs[-1] - highs[0]) / highs[0]
        valley_decrease = (lows[-1] - lows[0]) / lows[0]

        score = 0.0
        if peak_increase > 0.03 and valley_decrease < -0.03:
            score += 70

        return score, sub_start, sub_end


# ---------------------------------------------------
# 25. Broadening Bottom
# ---------------------------------------------------
class BroadeningBottomPattern(ChartPattern):
    def __init__(self, window_size=60):
        super().__init__(name="Broadening Bottom", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 유사하게 변동폭 확대
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values
        lows = subset['low'].values

        peak_increase = (highs[-1] - highs[0]) / highs[0]
        valley_decrease = (lows[-1] - lows[0]) / lows[0]

        score = 0.0
        # 간단히 동일
        if peak_increase > 0.03 and valley_decrease < -0.03:
            score += 70
        return score, sub_start, sub_end


# ---------------------------------------------------
# 26. Channel Pattern
# ---------------------------------------------------
class ChannelPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Channel Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 상하 평행 채널(단순: 고저점 regression)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        # 간단히 최대-최소, 중간 값이 일정 비율로 유지되면 채널이라고 가정
        highs = subset['high'].values
        lows = subset['low'].values
        gap = np.mean(highs - lows)
        # 변동성이 gap 주변에서 일정하면 +점수
        gap_std = np.std(highs - lows)

        score = 0.0
        if gap_std < gap*0.3:
            score += 80

        return score, sub_start, sub_end


# ---------------------------------------------------
# 27. Gaps Pattern
# ---------------------------------------------------
class GapsPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Gaps Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 최근 window 내 갭 발생 여부(단순)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        opens_ = subset['open'].values
        closes = subset['close'].values

        gap_count = 0
        for i in range(1, len(subset)):
            if opens_[i] > closes[i-1]*1.01 or opens_[i] < closes[i-1]*0.99:
                gap_count += 1

        # 갭이 많으면 점수
        score = min(gap_count * 10, 100)
        return score, sub_start, sub_end


# ---------------------------------------------------
# 28. Pipe Top
# ---------------------------------------------------
class PipeTopPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Pipe Top", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 꼭대기에 큰 봉 2개가 나란히 위치?
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values

        # 간단 예시: 뒤 2봉이 매우 높은가
        last2 = highs[-2:]
        rest = highs[:-2]
        if len(rest) == 0:
            return 0.0, sub_start, sub_end

        condition = (last2.min() > rest.mean() + rest.std()*1.0)
        score = 80.0 if condition else 10.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 29. Pipe Bottom
# ---------------------------------------------------
class PipeBottomPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="Pipe Bottom", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 바닥에 큰 봉 2개
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        lows = subset['low'].values
        last2 = lows[-2:]
        rest = lows[:-2]
        if len(rest) == 0:
            return 0.0, sub_start, sub_end

        condition = (last2.max() < rest.mean() - rest.std()*1.0)
        score = 80.0 if condition else 10.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 30. Spikes Pattern
# ---------------------------------------------------
class SpikesPattern(ChartPattern):
    def __init__(self, window_size=20):
        super().__init__(name="Spikes Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 급등/급락이 연이어 발생
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        closes = subset['close'].values

        big_moves = 0
        for i in range(1, len(closes)):
            change = abs(closes[i] - closes[i-1]) / closes[i-1]
            if change > 0.03:
                big_moves += 1

        score = min(big_moves * 15, 100)
        return score, sub_start, sub_end


# ---------------------------------------------------
# 31. Ascending Staircase
# ---------------------------------------------------
class AscendingStaircasePattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Ascending Staircase", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 일정 간격으로 고점과 저점이 단계적으로 높아짐
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 단순: 인덱스 0->10->20->... 비교
        steps = 5
        up_count = 0
        for i in range(0, self.window_size - steps, steps):
            if closes[i+steps] > closes[i]:
                up_count += 1

        rate = up_count / ((self.window_size - steps) / steps)
        score = rate * 100
        return score, sub_start, sub_end


# ---------------------------------------------------
# 32. Descending Staircase
# ---------------------------------------------------
class DescendingStaircasePattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Descending Staircase", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 고점, 저점이 단계적으로 낮아짐
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        steps = 5
        down_count = 0
        for i in range(0, self.window_size - steps, steps):
            if closes[i+steps] < closes[i]:
                down_count += 1

        rate = down_count / ((self.window_size - steps) / steps)
        score = rate * 100
        return score, sub_start, sub_end


# ---------------------------------------------------
# 33. Megaphone Pattern
# ---------------------------------------------------
class MegaphonePattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Megaphone", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 고점이 더 높아지고 저점이 더 낮아지는 (확산형)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        highs = subset['high'].values
        lows = subset['low'].values

        # 첫 절반 대비 두 번째 절반
        half = self.window_size // 2
        first_high_range = highs[:half].max() - highs[:half].min()
        second_high_range = highs[half:].max() - highs[half:].min()

        first_low_range = lows[:half].max() - lows[:half].min()
        second_low_range = lows[half:].max() - lows[half:].min()

        score = 0.0
        if second_high_range > first_high_range*1.2 and second_low_range > first_low_range*1.2:
            score += 70

        return score, sub_start, sub_end


# ---------------------------------------------------
# 34. V Pattern
# ---------------------------------------------------
class VPattern(ChartPattern):
    def __init__(self, window_size=30):
        super().__init__(name="V Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 급락 후 급등 (간단)
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        mid = self.window_size//2
        drop_rate = (closes[mid] - closes[0]) / closes[0]
        rise_rate = (closes[-1] - closes[mid]) / closes[mid]

        score = 0.0
        if drop_rate < -0.05 and rise_rate > 0.05:
            score += 80

        return score, sub_start, sub_end


# ---------------------------------------------------
# 35. Harmonic Pattern (더미)
# ---------------------------------------------------
class HarmonicPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Harmonic Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 실제론 피보나치 비율로 AB=CD 등 검사해야 함
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 가짜 점수
        score = 10.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 36. Elliott Wave Pattern (더미)
# ---------------------------------------------------
class ElliottWavePattern(ChartPattern):
    def __init__(self, window_size=80):
        super().__init__(name="Elliott Wave", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 실제로는 5파동 + 3파동 등 복잡
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        # 가짜
        score = 10.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 37. Candlestick Pattern (더미)
# ---------------------------------------------------
class CandlestickPattern(ChartPattern):
    def __init__(self, window_size=5):
        super().__init__(name="Candlestick Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 실제론 여러 캔들 패턴 (도지, 해머 등)을 검사해야 함
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]
        # 더미
        score = 15.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 38. Three Drives Pattern (더미)
# ---------------------------------------------------
class ThreeDrivesPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Three Drives", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 3번 상승(또는 하락) & 되돌림
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 가짜
        score = 20.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 39. Bump and Run Pattern (더미)
# ---------------------------------------------------
class BumpAndRunPattern(ChartPattern):
    def __init__(self, window_size=50):
        super().__init__(name="Bump and Run", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 급등 후 횡보 뒤 급락(또는 반대) 등
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT
        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        score = 20.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 40. Quasimodo Pattern (더미)
# ---------------------------------------------------
class QuasimodoPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Quasimodo", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 좌-머리-우-어깨비슷? price action 기반
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        sub_start, sub_end = subset.index[0], subset.index[-1]

        score = 20.0
        return score, sub_start, sub_end


# ---------------------------------------------------
# 41. Dead Cat Bounce
# ---------------------------------------------------
class DeadCatBouncePattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Dead Cat Bounce", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # 큰 하락 -> 약한 반등 -> 재하락
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        mid = self.window_size//2
        drop_rate = (closes[mid] - closes[0]) / closes[0]
        final_rate = (closes[-1] - closes[mid]) / closes[mid]

        score = 0.0
        # 큰 하락
        if drop_rate < -0.1:
            score += 50
        # 약간 반등 후 다시 하락
        if final_rate < 0:
            score += 30

        return score, sub_start, sub_end


# ---------------------------------------------------
# 42. Scallop Pattern
# ---------------------------------------------------
class ScallopPattern(ChartPattern):
    def __init__(self, window_size=40):
        super().__init__(name="Scallop Pattern", window_size=window_size)

    def calc_similarity(self, df: pd.DataFrame) -> (float, pd.Timestamp, pd.Timestamp):
        # J자형 또는 역J자형 곡선
        if len(df) < self.window_size:
            return 0.0, pd.NaT, pd.NaT

        subset = df.iloc[-self.window_size:]
        closes = subset['close'].values
        sub_start, sub_end = subset.index[0], subset.index[-1]

        # 간단: 지수곡선과 오차가 작은지 등등...
        # 여기선 더미
        score = 20.0
        return score, sub_start, sub_end


# ===================================================
# (3) ChartPatternScanner
# ===================================================
class ChartPatternScanner:
    def __init__(self, patterns: List[ChartPattern]):
        self.patterns = patterns

    def detect_all_patterns(self, df: pd.DataFrame):
        """
        모든 패턴의 유사도 계산 → (패턴명, 점수, 부분구간 시작, 종료) 리스트 반환
        """
        results = []
        for pattern in self.patterns:
            score, sub_start, sub_end = pattern.calc_similarity(df)
            results.append((pattern.name, score, sub_start, sub_end))
        return results

    def detect_best_pattern(self, df: pd.DataFrame):
        """
        최고 점수 패턴 찾기
        """
        all_results = self.detect_all_patterns(df)
        if not all_results:
            return None, 0.0, pd.NaT, pd.NaT

        best = max(all_results, key=lambda x: x[1])  # 점수가 가장 높은 튜플
        return best  # (pattern_name, score, sub_start, sub_end)


# ===================================================
# (4) 메인 실행 부분
# ===================================================
if __name__ == "__main__":
    # (4-1) 기본 파라미터
    ticker = "KRW-BTC"
    interval = "minute5"    # 5분봉
    count = 576             # 대략 48시간분

    # (4-2) OHLCV 데이터 가져오기
    df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
    if df is None or len(df) == 0:
        print("업비트에서 데이터를 가져오지 못했습니다.")
        exit()

    chart_start_time = df.index[0]
    chart_end_time = df.index[-1]

    print("=== 전체 차트 데이터 범위 ===")
    print(f"시작 시점: {chart_start_time}, 끝 시점: {chart_end_time}\n")

    # (4-3) 42개 패턴 객체 준비
    patterns_to_check = [
        DoubleTopPattern(),
        DoubleBottomPattern(),
        AscendingTrianglePattern(),
        DescendingTrianglePattern(),
        SymmetricalTrianglePattern(),
        RisingWedgePattern(),
        FallingWedgePattern(),
        BullishFlagPattern(),
        BearishFlagPattern(),
        TripleTopPattern(),
        TripleBottomPattern(),
        HeadAndShouldersPattern(),
        InverseHeadAndShouldersPattern(),
        BullishPennantPattern(),
        BearishPennantPattern(),
        BullishRectanglePattern(),
        BearishRectanglePattern(),
        RoundingTopPattern(),
        RoundingBottomPattern(),
        IslandReversalPattern(),
        DiamondTopPattern(),
        DiamondBottomPattern(),
        CupAndHandlePattern(),
        BroadeningTopPattern(),
        BroadeningBottomPattern(),
        ChannelPattern(),
        GapsPattern(),
        PipeTopPattern(),
        PipeBottomPattern(),
        SpikesPattern(),
        AscendingStaircasePattern(),
        DescendingStaircasePattern(),
        MegaphonePattern(),
        VPattern(),
        HarmonicPattern(),
        ElliottWavePattern(),
        CandlestickPattern(),
        ThreeDrivesPattern(),
        BumpAndRunPattern(),
        QuasimodoPattern(),
        DeadCatBouncePattern(),
        ScallopPattern()
    ]

    # (4-4) 스캐너
    scanner = ChartPatternScanner(patterns=patterns_to_check)

    # 1) 전체 결과
    all_results = scanner.detect_all_patterns(df)

    # 2) 최고 점수 패턴
    best_pattern_name, best_score, best_sub_start, best_sub_end = scanner.detect_best_pattern(df)

    # (4-5) 콘솔 출력
    print("=== 전체 패턴 유사도 ===")
    for (pname, pscore, p_sub_start, p_sub_end) in all_results:
        print(f"{pname}: {pscore:.2f} | (sub_start={p_sub_start}, sub_end={p_sub_end})")

    if best_pattern_name is not None:
        print("\n=== 최고 점수 패턴 ===")
        print(f"패턴: {best_pattern_name}, 점수: {best_score:.2f}")
        print(f"구간: {best_sub_start} ~ {best_sub_end}")
    else:
        print("패턴 스캔 결과가 없습니다.")

    # (4-6) CSV 저장
    df_save = pd.DataFrame(
        [
            (
                chart_start_time, chart_end_time, pname, pscore,
                p_sub_start, p_sub_end
            )
            for (pname, pscore, p_sub_start, p_sub_end) in all_results
        ],
        columns=[
            "chart_start_time", "chart_end_time", "pattern_name", "similarity",
            "pattern_sub_start_time", "pattern_sub_end_time"
        ]
    )

    out_filename = "all_pattern_results.csv"
    df_save.to_csv(out_filename, index=False, encoding="utf-8-sig")

    print(f"\n전체 42개 패턴에 대한 유사도 결과가 '{out_filename}' 에 저장되었습니다.")