# module/strategies/nls2_combined_progress.py

from module.strategies.nls2_combined import NLS2Combined
from module.strategies.progress_mixin import ProgressMixin

class NLS2CombinedProgress(ProgressMixin, NLS2Combined):
    def __init__(self):
        ProgressMixin.__init__(self)
        NLS2Combined.__init__(self)

    def next(self):
        super().next()         # ProgressMixin.next() → 진행률 업데이트
        NLS2Combined.next(self)# 논문 전략 로직

    def stop(self):
        super().stop()
        # NLS2Combined.stop(self) 필요 시 호출
