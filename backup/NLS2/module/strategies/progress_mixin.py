# module/strategies/progress_mixin.py

import backtrader as bt
from tqdm import tqdm

class ProgressMixin(bt.Strategy):
    """
    Backtrader가 데이터를 미리 로딩한 뒤 '전략 시작(start)' → 각 Bar 처리(next) → '전략 종료(stop)' 흐름으로 동작하는데,
    이 때 Bar(캔들) 진행 상황을 0~100% 형태로 표시해주는 믹스인(Mixin)입니다.

    문제점 수정 요약:
      - start() 시점에 전체 Bar 수(self.total_bars)를 잡되,
      - tqdm의 total=100으로 고정하여 '퍼센트' 단위로만 막대를 운용
      - next()가 수만 ~ 수백만 번 호출되더라도, 실제 tqdm 내부 n 값은 최대 100만 찍히도록 조정
      - 즉, 매 next()마다 현재 진행 퍼센트(new_percent)를 계산하고,
        그 이전 퍼센트(old_percent)와의 차이만큼만 update() 해서
        'x/100' 형태의 정상적인 막대가 나오도록 함
    """

    def __init__(self):
        # __init__ 단계에서는 len(self.data)가 아직 0일 수 있으므로
        # 여기서는 미리 초기화만 하고, 실제 total_bars는 start()에서 계산합니다.
        self.current_bar = 0
        self.old_percent = 0
        self.pbar = None
        self.total_bars = 0

    def start(self):
        """
        Backtrader가 모든 데이터를 로딩한 직후, '전략 시작'할 때 자동 호출됩니다.
        이 시점에는 len(self.data)가 실제 Bar 개수를 올바르게 반환합니다.
        """
        self.total_bars = len(self.data)
        # 혹시나 total_bars가 0일 경우의 방어 코드
        if self.total_bars < 1:
            self.total_bars = 1

        # tqdm를 '총 100단계' = 0~100%로 세팅
        self.pbar = tqdm(
            total=100,
            desc="Backtest Progress",
            unit="%",  # 단위 표시
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} {postfix}'
        )

        self.current_bar = 0
        self.old_percent = 0

    def next(self):
        """
        매 Bar(캔들)마다 한 번씩 호출됩니다.
        여기서 현재 Bar 번호를 1씩 증가시키고, 이를 기반으로 진행 퍼센트를 계산하여 tqdm를 업데이트합니다.
        """
        self.current_bar += 1

        # 전체 대비 어느 정도 비율인지(0.0 ~ 1.0)
        fraction = self.current_bar / self.total_bars
        # 혹시 1을 넘어갈 수 있으므로 min() 처리
        fraction = min(fraction, 1.0)

        # 0~100 퍼센트로 환산
        new_percent = int(fraction * 100)

        # 이전 퍼센트(old_percent)와 차이만큼만 update
        delta = new_percent - self.old_percent
        if delta > 0:
            self.pbar.update(delta)
            self.old_percent = new_percent

    def stop(self):
        """
        백테스트가 끝날 때 호출됩니다.
        남아 있는 퍼센트를 마저 올려서 100%가 되도록 하고 마무리합니다.
        """
        remain = 100 - self.old_percent
        if remain > 0:
            self.pbar.update(remain)

        if self.pbar:
            self.pbar.close()
