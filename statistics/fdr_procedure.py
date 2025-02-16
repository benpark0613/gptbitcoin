# statistics/fdr_procedure.py

import numpy as np
import pandas as pd

class FDRTester:
    """
    Barras et al. (2010) 논문 방식의 FDR 검정을 구현한 클래스 예시.
    각 규칙(혹은 펀드)의 알파와 p-value를 입력으로 받아,
    진짜 알파가 있는 규칙과 단순히 운 좋았던 규칙을 구분한다.
    """

    def __init__(self, alpha=0.05, bootstrap=False):
        """
        :param alpha: 유의수준 (기본 0.05)
        :param bootstrap: (옵션) 부트스트랩 방식을 쓸지 여부
        """
        self.alpha = alpha
        self.bootstrap = bootstrap
        # 논문에서는 p-value를 이용해 pi0, fdr+ 등 추정

    def estimate_p0_storey(self, pvals, lambda_val=0.5):
        """
        Storey (2002) 방식으로 pi0 추정
        pvals 중 lambda_val보다 큰 비율 / (1 - lambda_val)
        """
        n = len(pvals)
        count = np.sum(pvals > lambda_val)
        pi0_hat = count / (n * (1 - lambda_val))
        return pi0_hat

    def run_fdr_procedure(self, pvals, side="two-sided"):
        """
        가장 간단한 Benjamini–Hochberg/Barras 접근:
        1) p-value 오름차순 정렬
        2) storey pi0 추정
        3) 임계값 검정 -> fdr+ 대상
        여기서는 논문의 모든 절차를 간소화한 예시 (실무에 맞춰 확장 가능)
        """
        # pvals는 1차원 array라고 가정
        n = len(pvals)
        if n == 0:
            return np.array([False]*n)

        pi0_hat = self.estimate_p0_storey(pvals, lambda_val=0.5)
        pvals_sorted = np.sort(pvals)
        idx_sorted = np.argsort(pvals)

        # BH-ish cutoff: p_k <= (k / n) * (alpha / pi0_hat)
        # Barras et al.의 세부 알고리즘(π^+ 등)은 별도 구현 가능
        cutoff_thresholds = (np.arange(1, n+1) / n) * (self.alpha / pi0_hat)

        is_rejected_sorted = pvals_sorted <= cutoff_thresholds
        # 최대 k 위치 찾기
        if not np.any(is_rejected_sorted):
            # 아무 것도 reject 안됨
            is_rejected_final = np.array([False]*n)
        else:
            max_k = np.where(is_rejected_sorted)[0].max()
            p_cutoff = pvals_sorted[max_k]
            is_rejected_final = pvals <= p_cutoff

        return is_rejected_final

