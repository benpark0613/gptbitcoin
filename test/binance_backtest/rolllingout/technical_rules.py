"""
기술적 지표 파라미터 조합을 생성하는 모듈.
연구(SSRN 논문) 자료에 근거하여, 6가지 보조지표 × 파라미터 조합 × 4개 시간간격을
모두 만들어낸다.
"""

from typing import List, Dict

def generate_ma_rules() -> List[Dict]:
    """
    이동평균(MA) 규칙 조합 생성.
    p < q 조건을 반영하여, 파라미터를 모두 조합한 뒤 리스트로 반환.
    """
    p_list = [1, 2, 6, 12, 18, 24, 30, 48, 96, 144, 168]
    q_list = [2, 6, 12, 18, 24, 30, 48, 96, 144, 168, 192]
    x_list = [0, 0.05, 0.1, 0.5, 1, 5]
    d_list = [0, 2, 3, 4, 5]
    k_list = [6, 12, 24, '∞']

    rules = []
    for p in p_list:
        for q in q_list:
            if p < q:  # p가 q보다 작은 경우만
                for x in x_list:
                    for d in d_list:
                        for k in k_list:
                            rule = {
                                'rule_type': 'MA',
                                'p': p,
                                'q': q,
                                'x': x,
                                'd': d,
                                'k': k
                            }
                            rules.append(rule)
    return rules

def generate_rsi_rules() -> List[Dict]:
    """
    RSI 규칙 조합 생성.
    """
    h_list = [2, 6, 12, 14, 18, 24, 30, 48, 96, 144, 168, 192]
    v_list = [10, 15, 20, 25]  # 50 ± v
    d_list = [1, 2, 5]
    k_list = [1, 6, 12, 24, '∞']

    rules = []
    for h in h_list:
        for v in v_list:
            for d in d_list:
                for k in k_list:
                    rule = {
                        'rule_type': 'RSI',
                        'h': h,
                        'v': v,
                        'd': d,
                        'k': k
                    }
                    rules.append(rule)
    return rules

def generate_sr_rules() -> List[Dict]:
    """
    지지·저항(Support & Resistance) 규칙 조합 생성.
    """
    j_list = [2, 6, 12, 18, 24, 30, 48, 96, 168]
    x_list = [0.05, 0.1, 0.5, 1, 2.5, 5, 10]
    d_list = [0, 1, 2, 3, 4, 5]
    k_list = [1, 6, 12, 24, '∞']

    rules = []
    for j in j_list:
        for x in x_list:
            for d in d_list:
                for k in k_list:
                    rule = {
                        'rule_type': 'S&R',
                        'j': j,
                        'x': x,
                        'd': d,
                        'k': k
                    }
                    rules.append(rule)
    return rules

def generate_filter_rules() -> List[Dict]:
    """
    필터 룰(Filter Rules) 규칙 조합 생성.
    연구에서 필터룰은 (1) 단순 x% 돌파와 (2) x,y% 돌파 + d(x), d(y) 보유 같은 형태가 섞여 있지만,
    여기서는 논문(Table B.6)에 나오는 파라미터 집합을 통합해 생성.
    """
    # j(lookback) = [1,2,6,12,24]
    # x, y, d(x), d(y) 등 두 종류로 나뉨
    # 여기서는 논문 표를 단순화해 "필터룰(1,575 + 1,260 = 2,835)" 형태를 모두 커버.
    j_list = [1, 2, 6, 12, 24]

    # 첫 번째 유형(간단):
    # x ∈ [0.05, 0.1, 0.5, 1, 5, 10, 20]
    # d(x) ∈ [0,1,2,3,4,5], k ∈ [6,12,18,20,24,∞]
    # (매도 신호도 유사)
    # 두 번째 유형 등 여러 변형이 있지만, 논문에 맞춰 최대한 그대로...
    x_candidates = [0.05, 0.1, 0.5, 1, 5, 10, 20]
    d_candidates = [0, 1, 2, 3, 4, 5]
    k_candidates = [6, 12, 18, 20, 24, '∞']

    # TODO: 필터룰 세부 정의는 논문 Appendix B.7에서 2가지 종류를 합쳐 총 (1,575 + 1,260).
    # 여기서는 대표 예시만 전부 조합 (상세는 본인이 맞춤 조정 필요)
    # x, y 둘 다 돌파하는 경우도 있고, d(x), d(y) 등. 본 예시에선 모든 조합을 단순화로 처리.

    # 추가로 'x'와 'y'를 동시에 설정하는 경우
    y_candidates = [0.05, 0.1, 0.5, 1, 5, 10, 20]

    rules = []
    # 첫 번째: x만 사용하는 경우
    for j in j_list:
        for x in x_candidates:
            for d in d_candidates:
                for k in k_candidates:
                    rule = {
                        'rule_type': 'Filter',
                        'j': j,
                        'x': x,
                        'd': d,
                        'k': k,
                        'y': None,  # y 사용 안 함
                        'd_y': None
                    }
                    rules.append(rule)

    # 두 번째: x, y 둘 다 쓰는 경우
    # x, y, d(x), d(y)
    d_y_candidates = [0, 1, 2, 3, 4]  # 예시로 d_y < d_x로 설정 가능하지만, 여기서는 전부 조합
    for j in j_list:
        for x in x_candidates:
            for y in y_candidates:
                for dx in d_candidates:
                    for dy in d_y_candidates:
                        for k in k_candidates:
                            # 일부 조건(x <= y 등)을 줄 수도 있으나, 논문에서 세부 규정은 다양.
                            rule = {
                                'rule_type': 'Filter',
                                'j': j,
                                'x': x,
                                'd': dx,
                                'k': k,
                                'y': y,
                                'd_y': dy
                            }
                            rules.append(rule)

    return rules

def generate_cb_rules() -> List[Dict]:
    """
    채널 돌파(Channel Breakout, CB) 규칙 조합.
    """
    j_list = [6, 12, 18, 24, 36, 72, 120, 168]
    c_list = [0.5, 1, 5, 10, 15]
    x_list = [0.05, 0.1, 0.5, 1, 5]
    d_list = [0, 1, 2]
    k_list = [1, 6, 12, 24, '∞']

    rules = []
    for j in j_list:
        for c in c_list:
            for x in x_list:
                for d in d_list:
                    for k in k_list:
                        rule = {
                            'rule_type': 'CB',
                            'j': j,
                            'c': c,
                            'x': x,
                            'd': d,
                            'k': k
                        }
                        rules.append(rule)
    return rules

def generate_obv_rules() -> List[Dict]:
    """
    OBV(On-Balance Volume) 이동평균 규칙 조합.
    """
    p_list = [2, 6, 12, 18, 24, 30, 48, 96, 144, 168]
    q_list = [2, 6, 12, 18, 24, 30, 48, 96, 144, 168, 192]
    x_list = [0, 0.01, 0.05]
    d_list = [0, 2, 3, 4, 5]
    k_list = [6, 12, '∞']

    rules = []
    for p in p_list:
        for q in q_list:
            if p < q:  # p가 q보다 작아야 단기 vs 장기 개념이 성립
                for x in x_list:
                    for d in d_list:
                        for k in k_list:
                            rule = {
                                'rule_type': 'OBV',
                                'p': p,
                                'q': q,
                                'x': x,
                                'd': d,
                                'k': k
                            }
                            rules.append(rule)
    return rules


def generate_technical_rules() -> List[Dict]:
    """
    6가지 지표(MA, RSI, S&R, Filter, CB, OBV) × 각 파라미터 조합을 모두 생성.
    이어서 4가지 시간간격(Daily, 60m, 30m, 10m)을 결합해,
    최종적으로 (규칙, 시간간격)을 나타내는 딕셔너리 리스트를 반환한다.

    Returns:
        [
          { 'rule_type': 'MA', 'freq': '1D', 'p': ..., 'q':..., 'x':..., ... },
          { 'rule_type': 'RSI', 'freq': '60m', 'h': ..., 'v':..., ... },
          ...
        ]
    """
    ma_rules   = generate_ma_rules()
    rsi_rules  = generate_rsi_rules()
    sr_rules   = generate_sr_rules()
    ft_rules   = generate_filter_rules()
    cb_rules   = generate_cb_rules()
    obv_rules  = generate_obv_rules()

    all_rules  = []
    all_rules.extend(ma_rules)
    all_rules.extend(rsi_rules)
    all_rules.extend(sr_rules)
    all_rules.extend(ft_rules)
    all_rules.extend(cb_rules)
    all_rules.extend(obv_rules)

    # 연구에서는 일봉(Daily), 60분봉, 30분봉, 10분봉 총 4가지 적용
    freqs = ['1D', '60m', '30m', '10m']

    final_list = []
    for rule in all_rules:
        for freq in freqs:
            # 규칙 사전에 'freq' 키 추가
            new_rule = dict(rule)
            new_rule['freq'] = freq
            final_list.append(new_rule)

    return final_list


if __name__ == "__main__":
    # 모듈 단독 실행 시, 테스트 출력
    ttr_list = generate_technical_rules()
    print(f"총 규칙 개수: {len(ttr_list)}")
    # 예: 상위 5개만 확인
    for r in ttr_list[:5]:
        print(r)
