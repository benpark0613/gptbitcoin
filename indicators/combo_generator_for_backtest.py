# gptbitcoin/indicators/combo_generator_for_backtest.py
# 보조지표 파라미터 조합을 생성하는 모듈.
# main.py에서 combos를 생성할 때 사용.

import itertools
from typing import List, Dict

from config.indicator_config import (
    INDICATOR_CONFIG,
    INDICATOR_COMBO_SIZES
)


def get_ma_param_dicts(cfg: dict) -> List[dict]:
    """
    이동평균(MA) 파라미터 조합을 생성한다.
    - short_ma_periods, long_ma_periods를 분리하여
      sp < lp 조건을 만족하는 쌍만 combo로 만든다.
    """
    short_list = cfg["short_ma_periods"]
    long_list = cfg["long_ma_periods"]
    results = []
    for sp in short_list:
        for lp in long_list:
            if sp < lp:
                results.append({
                    "type": "MA",
                    "short_period": sp,
                    "long_period": lp
                })
    return results


def get_rsi_param_dicts(cfg: dict) -> List[dict]:
    """
    RSI 파라미터 조합 생성.
    - lookback_periods, thresholds를 조합.
    - 여기서는 threshold까지 조합해 일괄 생성하되,
      실제 aggregator에서는 threshold는 지표 계산에 쓰이지 않고,
      시그널 단계에서 참조될 수 있음.
    """
    lb_list = cfg["lookback_periods"]
    thr_sets = cfg["thresholds"]
    results = []
    for lb in lb_list:
        for thr in thr_sets:
            low_th, high_th = thr
            results.append({
                "type": "RSI",
                "lookback": lb,
                "oversold": low_th,
                "overbought": high_th
            })
    return results


def get_obv_param_dicts(cfg: dict) -> List[dict]:
    """
    OBV 파라미터 조합을 생성한다.
    (단기/장기 이동평균 교차를 위한 short/long 분리)
    """
    short_list = cfg["short_ma_periods"]
    long_list = cfg["long_ma_periods"]
    results = []
    for sp in short_list:
        for lp in long_list:
            if sp < lp:
                results.append({
                    "type": "OBV",
                    "short_period": sp,
                    "long_period": lp
                })
    return results


def get_macd_param_dicts(cfg: dict) -> List[dict]:
    """
    MACD 파라미터 조합(fast, slow, signal) 생성
    """
    fasts = cfg["fast_periods"]
    slows = cfg["slow_periods"]
    signals = cfg["signal_periods"]
    results = []
    for f_ in fasts:
        for s_ in slows:
            if f_ < s_:
                for sig in signals:
                    results.append({
                        "type": "MACD",
                        "fast_period": f_,
                        "slow_period": s_,
                        "signal_period": sig
                    })
    return results


def get_dmi_adx_param_dicts(cfg: dict) -> List[dict]:
    """
    DMI_ADX (lookback_periods, adx_thresholds)
    - adx_thresholds는 실제 시그널 판단용이지만, 여기서는 일단 combos로 생성
    """
    lookbacks = cfg["lookback_periods"]
    adx_thr_list = cfg["adx_thresholds"]
    results = []
    for lb in lookbacks:
        for adx_thr in adx_thr_list:
            results.append({
                "type": "DMI_ADX",
                "lookback": lb,
                "adx_threshold": adx_thr
            })
    return results


def get_boll_param_dicts(cfg: dict) -> List[dict]:
    """
    볼린저 밴드(BOLL) (lookback_periods, stddev_multipliers)
    """
    lb_list = cfg["lookback_periods"]
    stddevs = cfg["stddev_multipliers"]
    results = []
    for lb in lb_list:
        for sd in stddevs:
            results.append({
                "type": "BOLL",
                "lookback": lb,
                "stddev_mult": sd
            })
    return results


def get_ichimoku_param_dicts(cfg: dict) -> List[dict]:
    """
    일목균형표(ICHIMOKU) (tenkan, kijun, senkou_span_b)
    """
    t_list = cfg["tenkan_period"]
    k_list = cfg["kijun_period"]
    s_list = cfg["senkou_span_b_period"]
    results = []
    for t_ in t_list:
        for k_ in k_list:
            # t>=k를 제외하거나 말거나는 선택적
            # 여기서는 굳이 제외하지 않고 모두 생성
            for s_ in s_list:
                results.append({
                    "type": "ICHIMOKU",
                    "tenkan_period": t_,
                    "kijun_period": k_,
                    "senkou_span_b_period": s_
                })
    return results


def get_psar_param_dicts(cfg: dict) -> List[dict]:
    """
    PSAR(acceleration_step, acceleration_max)
    """
    steps = cfg["acceleration_step"]
    maxes = cfg["acceleration_max"]
    results = []
    for st in steps:
        for mx in maxes:
            # st <= mx인지 여부는 필요 시 필터링
            if st <= mx:
                results.append({
                    "type": "PSAR",
                    "acceleration_step": st,
                    "acceleration_max": mx
                })
    return results


def get_supertrend_param_dicts(cfg: dict) -> List[dict]:
    """
    SUPERTREND(atr_period, multiplier)
    """
    atr_list = cfg["atr_period"]
    mult_list = cfg["multiplier"]
    results = []
    for a_ in atr_list:
        for m_ in mult_list:
            results.append({
                "type": "SUPERTREND",
                "atr_period": a_,
                "multiplier": m_
            })
    return results


def get_donchian_param_dicts(cfg: dict) -> List[dict]:
    """
    DONCHIAN_CHANNEL(lookback_periods)
    """
    lb_list = cfg["lookback_periods"]
    results = []
    for lb in lb_list:
        results.append({
            "type": "DONCHIAN_CHANNEL",
            "lookback": lb
        })
    return results


def get_stoch_param_dicts(cfg: dict) -> List[dict]:
    """
    STOCH(K, D, thresholds)
    일반적으로 K >= D 조합만 사용하는 버전.

    Args:
        cfg (dict): STOCH 설정 딕셔너리
          - cfg["k_period"], cfg["d_period"], cfg["thresholds"] 등이 포함

    Returns:
        List[dict]: 스토캐스틱 지표 파라미터 조합 목록
    """
    k_list = cfg["k_period"]
    d_list = cfg["d_period"]
    thr_sets = cfg["thresholds"]
    results = []

    for k_ in k_list:
        for d_ in d_list:
            # 일반적으로는 K >= D인 경우를 사용하므로 그 외는 건너뜀
            if k_ < d_:
                continue
            for thr in thr_sets:
                low, high = thr
                results.append({
                    "type": "STOCH",
                    "k_period": k_,
                    "d_period": d_,
                    "oversold": low,
                    "overbought": high
                })

    return results

def get_stoch_rsi_param_dicts(cfg: dict) -> List[dict]:
    """
    STOCH_RSI(rsi_periods, stoch_periods, k_period, d_period, thresholds)
    """
    rsi_list = cfg["rsi_periods"]
    stoch_list = cfg["stoch_periods"]
    k_list = cfg["k_period"]
    d_list = cfg["d_period"]
    thr_sets = cfg["thresholds"]
    results = []
    for r_ in rsi_list:
        for st_ in stoch_list:
            for k_ in k_list:
                for d_ in d_list:
                    for thr in thr_sets:
                        low, high = thr
                        results.append({
                            "type": "STOCH_RSI",
                            "rsi_length": r_,
                            "stoch_length": st_,
                            "k_period": k_,
                            "d_period": d_,
                            "oversold": low,
                            "overbought": high
                        })
    return results


def get_vwap_param_dicts(cfg: dict) -> List[dict]:
    """
    VWAP(별도 파라미터 없음)
    """
    return [{"type": "VWAP"}]


def get_indicator_param_dicts() -> Dict[str, List[dict]]:
    """
    indicator_config.py에 정의된 모든 지표에 대해
    파라미터 dict의 목록을 만든다.

    Returns:
      Dict[str, List[dict]]: 각 지표명(key)에 대한 파라미터 dict 리스트(value).
    """
    result = {}
    cfg = INDICATOR_CONFIG

    if "MA" in cfg:
        ma_cfg = cfg["MA"]
        result["MA"] = get_ma_param_dicts(ma_cfg)

    if "RSI" in cfg:
        result["RSI"] = get_rsi_param_dicts(cfg["RSI"])

    if "OBV" in cfg:
        obv_cfg = cfg["OBV"]
        result["OBV"] = get_obv_param_dicts(obv_cfg)

    if "MACD" in cfg:
        result["MACD"] = get_macd_param_dicts(cfg["MACD"])

    if "DMI_ADX" in cfg:
        result["DMI_ADX"] = get_dmi_adx_param_dicts(cfg["DMI_ADX"])

    if "BOLL" in cfg:
        result["BOLL"] = get_boll_param_dicts(cfg["BOLL"])

    if "ICHIMOKU" in cfg:
        result["ICHIMOKU"] = get_ichimoku_param_dicts(cfg["ICHIMOKU"])

    if "PSAR" in cfg:
        result["PSAR"] = get_psar_param_dicts(cfg["PSAR"])

    if "SUPERTREND" in cfg:
        result["SUPERTREND"] = get_supertrend_param_dicts(cfg["SUPERTREND"])

    if "DONCHIAN_CHANNEL" in cfg:
        result["DONCHIAN_CHANNEL"] = get_donchian_param_dicts(cfg["DONCHIAN_CHANNEL"])

    if "STOCH" in cfg:
        result["STOCH"] = get_stoch_param_dicts(cfg["STOCH"])

    if "STOCH_RSI" in cfg:
        result["STOCH_RSI"] = get_stoch_rsi_param_dicts(cfg["STOCH_RSI"])

    if "VWAP" in cfg:
        result["VWAP"] = get_vwap_param_dicts(cfg["VWAP"])

    return result


def generate_indicator_combos() -> List[List[dict]]:
    """
    INDICATOR_COMBO_SIZES에 기초해 여러 지표를 조합한 파라미터 세트들을 생성.
    - 예: combo_size=1 이면 단일 지표.
    - combo_size=2 이면 2개 지표 조합(예: MA + RSI).
    - 현재 config에서 INDICATOR_COMBO_SIZES = [1] 로 두어,
      각 지표를 개별적으로만 쓰는 콤보들을 만든다.

    Returns:
      List[List[dict]]: 각 콤보별 파라미터 dict의 리스트.
        예) [ [ {type:"MA", short_period:5, long_period:20} ],
              [ {type:"RSI", lookback:14, oversold:30, overbought:70} ],
              ...
            ]
    """
    indicator_param_dicts = get_indicator_param_dicts()
    indicator_names = list(indicator_param_dicts.keys())
    all_combos = []

    for combo_size in INDICATOR_COMBO_SIZES:
        for indicator_subset in itertools.combinations(indicator_names, combo_size):
            param_lists = [indicator_param_dicts[name] for name in indicator_subset]
            for merged_tuple in itertools.product(*param_lists):
                all_combos.append(list(merged_tuple))

    return all_combos


def _test_count() -> None:
    """combo 수 간단 출력 테스트"""
    combos = generate_indicator_combos()
    print(f"[combo_generator] 전체 combo 개수: {len(combos)}")
    # 각 지표별 파라미터 수도 출력
    dicts_by_indicator = get_indicator_param_dicts()
    for ind_name, plist in dicts_by_indicator.items():
        print(f"{ind_name}: {len(plist)} combos")


if __name__ == "__main__":
    _test_count()
