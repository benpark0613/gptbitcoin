# gptbitcoin/backtest/combo_generator.py
# 다양한 지표 파라미터 조합을 생성하는 모듈
# 주석은 필요한 최소한으로 한글 작성, docstring은 구글 스타일

import itertools
from typing import List, Dict


from config.config import INDICATOR_COMBO_SIZES, INDICATOR_CONFIG


def get_ma_param_dicts(ma_config: dict) -> List[dict]:
    """
    MA 지표에 대한 모든 파라미터 조합을 생성한다.

    Args:
        ma_config (dict): 예) {
            "short_periods": [...],
            "long_periods": [...],
            "band_filters": [...]
        }

    Returns:
        List[dict]: 각각 { "type": "MA", "short_period": ..., "long_period": ..., "band_filter": ... } 형태의 파라미터
    """
    results = []
    short_periods = ma_config.get("short_periods", [])
    long_periods = ma_config.get("long_periods", [])
    band_filters = ma_config.get("band_filters", [])

    # 모든 조합
    for sp in short_periods:
        for lp in long_periods:
            # 단기 < 장기가 합리적이라면 조건식 추가 가능. (원하는 경우 주석 제거)
            if sp >= lp:
                continue
            for bf in band_filters:
                param = {
                    "type": "MA",
                    "short_period": sp,
                    "long_period": lp,
                    "band_filter": bf
                }
                results.append(param)
    return results


def get_rsi_param_dicts(rsi_config: dict) -> List[dict]:
    """
    RSI 지표에 대한 모든 파라미터 조합을 생성한다.

    Args:
        rsi_config (dict): 예) {
            "lengths": [...],
            "overbought_values": [...],
            "oversold_values": [...]
        }

    Returns:
        List[dict]: 각각 { "type": "RSI", "length": ..., "overbought": ..., "oversold": ... } 형태의 파라미터
    """
    results = []
    lengths = rsi_config.get("lengths", [])
    overbought_vals = rsi_config.get("overbought_values", [])
    oversold_vals = rsi_config.get("oversold_values", [])

    for length in lengths:
        for ob in overbought_vals:
            for os_ in oversold_vals:
                param = {
                    "type": "RSI",
                    "length": length,
                    "overbought": ob,
                    "oversold": os_
                }
                results.append(param)
    return results


def get_obv_param_dicts(obv_config: dict) -> List[dict]:
    """
    OBV 지표에 대한 모든 파라미터 조합을 생성한다.

    Args:
        obv_config (dict): 예) {
            "short_periods": [...],
            "long_periods": [...]
        }

    Returns:
        List[dict]: { "type": "OBV", "short_period": ..., "long_period": ... } 등
                    단, OBV는 추가 파라미터가 많지 않다면 간단히 구성
    """
    results = []
    short_periods = obv_config.get("short_periods", [])
    long_periods = obv_config.get("long_periods", [])

    for sp in short_periods:
        for lp in long_periods:
            param = {
                "type": "OBV",
                "short_period": sp,
                "long_period": lp
            }
            results.append(param)
    return results


def get_filter_param_dicts(filter_config: dict) -> List[dict]:
    """
    Filter(필터룰) 지표에 대한 모든 파라미터 조합 생성.

    Args:
        filter_config (dict): 예) {
            "windows": [...],
            "x_values": [...],
            "y_values": [...]
        }

    Returns:
        List[dict]: { "type": "Filter", "window": ..., "x_pct": ..., "y_pct": ... }
    """
    results = []
    windows = filter_config.get("windows", [])
    x_vals = filter_config.get("x_values", [])
    y_vals = filter_config.get("y_values", [])

    for w in windows:
        for x_ in x_vals:
            for y_ in y_vals:
                param = {
                    "type": "Filter",
                    "window": w,
                    "x_pct": x_,
                    "y_pct": y_
                }
                results.append(param)
    return results


def get_sr_param_dicts(sr_config: dict) -> List[dict]:
    """
    Support/Resistance 관련 파라미터 조합을 생성.

    Args:
        sr_config (dict): 예) {
            "windows": [...],
            "band_pcts": [...]
        }

    Returns:
        List[dict]: { "type": "Support_Resistance", "window": ..., "band_pct": ... }
    """
    results = []
    windows = sr_config.get("windows", [])
    band_pcts = sr_config.get("band_pcts", [])

    for w in windows:
        for bp in band_pcts:
            param = {
                "type": "Support_Resistance",
                "window": w,
                "band_pct": bp
            }
            results.append(param)
    return results


def get_channel_param_dicts(cb_config: dict) -> List[dict]:
    """
    Channel Breakout 관련 파라미터 조합을 생성.

    Args:
        cb_config (dict): 예) {
            "windows": [...],
            "c_values": [...]
        }

    Returns:
        List[dict]: { "type": "Channel_Breakout", "window": ..., "c_value": ... }
    """
    results = []
    windows = cb_config.get("windows", [])
    c_vals = cb_config.get("c_values", [])

    for w in windows:
        for c_ in c_vals:
            param = {
                "type": "Channel_Breakout",
                "window": w,
                "c_value": c_
            }
            results.append(param)
    return results


def get_indicator_param_dicts() -> Dict[str, List[dict]]:
    """
    INDICATOR_CONFIG에 따라 지표별 파라미터 조합을 전부 생성해 사전 형태로 반환.
    예: {
      "MA": [ {...}, {...}, ... ],
      "RSI": [ {...}, {...}, ... ],
      ...
    }

    Returns:
        Dict[str, List[dict]]: 각 지표 유형별로 모든 파라미터 조합을 리스트로 묶어 반환
    """
    result = {}

    for indicator_name, cfg in INDICATOR_CONFIG.items():
        if indicator_name == "MA":
            result[indicator_name] = get_ma_param_dicts(cfg)
        elif indicator_name == "RSI":
            result[indicator_name] = get_rsi_param_dicts(cfg)
        elif indicator_name == "OBV":
            result[indicator_name] = get_obv_param_dicts(cfg)
        elif indicator_name == "Filter":
            result[indicator_name] = get_filter_param_dicts(cfg)
        elif indicator_name == "Support_Resistance":
            result[indicator_name] = get_sr_param_dicts(cfg)
        elif indicator_name == "Channel_Breakout":
            result[indicator_name] = get_channel_param_dicts(cfg)
        else:
            # 알 수 없는 지표 타입
            result[indicator_name] = []
    return result


def generate_indicator_combos() -> List[List[dict]]:
    """
    config.py의 INDICATOR_COMBO_SIZES와 INDICATOR_CONFIG를 참조해,
    여러 지표(Indicator)들의 파라미터 조합을 모두 생성한다.
    예: COMBO_SIZES = [1, 2, 3]이면
        1개 지표만 사용하는 경우 + 2개 지표 조합 + 3개 지표 조합 모두 계산.

    Returns:
        List[List[dict]]: 각 원소는 [ {type=..., params...}, {type=..., params...}, ... ] 형태
    """
    # 지표별 파라미터 조합 딕셔너리
    indicator_param_dicts = get_indicator_param_dicts()
    # 사용 가능한 지표 이름들
    indicator_names = list(indicator_param_dicts.keys())

    all_combos = []  # 최종 결과: 지표 세트를 담은 리스트(각 세트는 여러 지표 파라미터 dict)

    # 예: COMBO_SIZES = [1, 2, 3]
    for combo_size in INDICATOR_COMBO_SIZES:
        # 지표 이름들 중에서 combo_size개를 뽑는 조합
        for indicator_name_subset in itertools.combinations(indicator_names, combo_size):
            # 각 지표별로 가능한 파라미터들의 카르테시안 곱
            # ex) MA(3가지 param) * RSI(2가지 param) => 6가지
            param_lists = [indicator_param_dicts[name] for name in indicator_name_subset]
            for merged_tuple in itertools.product(*param_lists):
                # merged_tuple은 ( {MA 파라미터}, {RSI 파라미터} ) 같은 구조
                combo_list = list(merged_tuple)
                all_combos.append(combo_list)

    return all_combos


if __name__ == "__main__":
    """
    간단 테스트: combo_generator 실행 예시.
    """
    combos = generate_indicator_combos()
    print(f"총 조합 개수: {len(combos)}")
    # 예: 첫 3개만 출력
    for i in range(min(3, len(combos))):
        print(f"{i+1}번째 조합: {combos[i]}")
