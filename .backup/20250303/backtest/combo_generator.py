# gptbitcoin/backtest/combo_generator.py
# 구글 스타일, 최소한의 한글 주석
#
# 동일한 인디케이터 타입(예: 둘 다 MA, 둘 다 RSI) 조합은 제외하고,
# 서로 다른 인디케이터끼리만 조합을 생성한다.

import itertools
from typing import Dict, List, Any
from config.config import INDICATOR_CONFIG, INDICATOR_COMBO_SIZES

def _generate_ma_combos(ma_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for short_p in ma_cfg["short_periods"]:
        for long_p in ma_cfg["long_periods"]:
            if short_p >= long_p:
                continue
            for bf in ma_cfg["band_filters"]:
                combo = {
                    "indicator": "MA",
                    "short_period": short_p,
                    "long_period": long_p,
                    "band_filter": bf,
                }
                results.append(combo)
    return results

def _generate_rsi_combos(rsi_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for length in rsi_cfg["lengths"]:
        for ob in rsi_cfg["overbought_values"]:
            for os_ in rsi_cfg["oversold_values"]:
                combo = {
                    "indicator": "RSI",
                    "length": length,
                    "overbought": ob,
                    "oversold": os_,
                }
                results.append(combo)
    return results

def _generate_filter_combos(f_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for w in f_cfg["windows"]:
        for x_val in f_cfg["x_values"]:
            for y_val in f_cfg["y_values"]:
                combo = {
                    "indicator": "Filter",
                    "window": w,
                    "x": x_val,
                    "y": y_val,
                }
                results.append(combo)
    return results

def _generate_snr_combos(snr_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for w in snr_cfg["windows"]:
        for band in snr_cfg["band_pcts"]:
            combo = {
                "indicator": "Support_Resistance",
                "window": w,
                "band_pct": band,
            }
            results.append(combo)
    return results

def _generate_channel_combos(cb_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for w in cb_cfg["windows"]:
        for c_val in cb_cfg["c_values"]:
            combo = {
                "indicator": "Channel_Breakout",
                "window": w,
                "c_value": c_val,
            }
            results.append(combo)
    return results

def _generate_obv_combos(obv_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for sp in obv_cfg["short_periods"]:
        for lp in obv_cfg["long_periods"]:
            if sp >= lp:
                continue
            combo = {
                "indicator": "OBV",
                "short_period": sp,
                "long_period": lp,
            }
            results.append(combo)
    return results

def generate_all_indicator_combos() -> List[Dict[str, Any]]:
    """
    INDICATOR_CONFIG를 바탕으로 6가지 규칙의 단일 파라미터 조합들을 전부 반환한다.
    (인디케이터 타입마다 중복 없는 단일 조합)
    """
    all_combos = []
    if "MA" in INDICATOR_CONFIG:
        all_combos.extend(_generate_ma_combos(INDICATOR_CONFIG["MA"]))
    if "RSI" in INDICATOR_CONFIG:
        all_combos.extend(_generate_rsi_combos(INDICATOR_CONFIG["RSI"]))
    if "Filter" in INDICATOR_CONFIG:
        all_combos.extend(_generate_filter_combos(INDICATOR_CONFIG["Filter"]))
    if "Support_Resistance" in INDICATOR_CONFIG:
        all_combos.extend(_generate_snr_combos(INDICATOR_CONFIG["Support_Resistance"]))
    if "Channel_Breakout" in INDICATOR_CONFIG:
        all_combos.extend(_generate_channel_combos(INDICATOR_CONFIG["Channel_Breakout"]))
    if "OBV" in INDICATOR_CONFIG:
        all_combos.extend(_generate_obv_combos(INDICATOR_CONFIG["OBV"]))

    return all_combos

def generate_multi_indicator_combos() -> List[List[Dict[str, Any]]]:
    """
    INDICATOR_COMBO_SIZES에 따라 2~N개 인디케이터를 '서로 다른 타입'으로만 묶은 조합을 생성한다.
    예) [ {MA}, {RSI} ] 또는 [ {MA}, {Filter}, {OBV} ] 등
    같은 인디케이터 타입(MA,MA)은 제외한다.
    """
    single_configs = generate_all_indicator_combos()
    all_combos = []

    for size in INDICATOR_COMBO_SIZES:
        # 단일은 size=1일 때 해당, 여기선 2나 3 등 사용자 설정에 따라 달라짐
        # itertools.combinations로 size개씩 뽑는다.
        # 뽑은 뒤, indicator 중복이 있는지 필터링
        for combo_tuple in itertools.combinations(single_configs, size):
            indicator_types = [cfg["indicator"] for cfg in combo_tuple]
            # indicator가 중복되면 제외
            if len(set(indicator_types)) == size:
                all_combos.append(list(combo_tuple))

    return all_combos

def _test_combo_counts() -> None:
    """
    테스트용 함수:
    1) 단일 인디케이터 조합 수
    2) combo_sizes에 따른 '서로 다른 인디케이터' 조합 수
    """
    single_list = generate_all_indicator_combos()
    multi_list = generate_multi_indicator_combos()

    print(f"[TEST] 단일 인디케이터 조합 수: {len(single_list)}")
    print(f"[TEST] 다중(서로 다른 인디케이터) 조합 수: {len(multi_list)}")

    if len(single_list) > 0:
        print("예시 단일 조합 3개:")
        for c in single_list[:3]:
            print(" ", c)

    if len(multi_list) > 0:
        print("예시 다중 조합 3개:")
        for cm in multi_list[:3]:
            print(" ", cm)

if __name__ == "__main__":
    _test_combo_counts()
