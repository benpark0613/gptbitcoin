# gptbitcoin/backtest/combo_generator.py
# 보조지표 파라미터 조합 및 여러 지표를 합성하여 콤보 세트를 생성하는 모듈

# 아래 소스는 기존 combo_generator.py를
# 새롭게 정의된 indicator_config.py(특히 OBV 항목)에 맞춰 수정한 완성본입니다.
# 불필요한 주석은 최소화하였으며, 필수적인 한글 주석만 첨부하였습니다.

import itertools
from typing import List, Dict

from config.indicator_config import (
    INDICATOR_CONFIG,
    INDICATOR_COMBO_SIZES
)


def get_ma_param_dicts(cfg: dict) -> List[dict]:
    """이동평균(MA) 파라미터 조합을 생성한다."""
    shorts = cfg["short_ma_periods"]
    longs = cfg["long_ma_periods"]
    results = []
    for s in shorts:
        for l in longs:
            if s >= l:
                continue
            results.append({
                "type": "MA",
                "short_period": s,
                "long_period": l
            })
    return results


def get_rsi_param_dicts(cfg: dict) -> List[dict]:
    """RSI 파라미터 조합을 생성한다."""
    lbs = cfg["lookback_periods"]
    thr_sets = cfg["thresholds"]
    results = []
    for lb in lbs:
        for thr in thr_sets:
            oversold, overbought = thr
            results.append({
                "type": "RSI",
                "lookback": lb,
                "oversold": oversold,
                "overbought": overbought
            })
    return results


def get_obv_param_dicts(cfg: dict) -> List[dict]:
    """
    OBV 파라미터 조합을 생성한다.
    기존의 short/long_period 기반이 아닌,
    absolute_threshold_periods, absolute_threshold_percentiles 사용.
    """
    periods = cfg["absolute_threshold_periods"]
    percentiles = cfg["absolute_threshold_percentiles"]
    results = []
    for p in periods:
        for pc in percentiles:
            results.append({
                "type": "OBV",
                "lookback_period": p,
                "threshold_percentile": pc
            })
    return results


def get_macd_param_dicts(cfg: dict) -> List[dict]:
    """MACD 파라미터 조합을 생성한다."""
    fasts = cfg["fast_periods"]
    slows = cfg["slow_periods"]
    signals = cfg["signal_periods"]
    results = []
    for f in fasts:
        for s in slows:
            if f >= s:
                continue
            for sig in signals:
                results.append({
                    "type": "MACD",
                    "fast_period": f,
                    "slow_period": s,
                    "signal_period": sig
                })
    return results


def get_dmi_adx_param_dicts(cfg: dict) -> List[dict]:
    """DMI_ADX 파라미터 조합을 생성한다."""
    lbs = cfg["lookback_periods"]
    adxs = cfg["adx_thresholds"]
    results = []
    for lb in lbs:
        for adx_th in adxs:
            results.append({
                "type": "DMI_ADX",
                "lookback": lb,
                "adx_threshold": adx_th
            })
    return results


def get_boll_param_dicts(cfg: dict) -> List[dict]:
    """볼린저 밴드(BOLL) 파라미터 조합을 생성한다."""
    lbs = cfg["lookback_periods"]
    stds = cfg["stddev_multipliers"]
    results = []
    for lb in lbs:
        for sd in stds:
            results.append({
                "type": "BOLL",
                "lookback": lb,
                "stddev_mult": sd
            })
    return results


def get_ichimoku_param_dicts(cfg: dict) -> List[dict]:
    """일목균형표(ICHIMOKU) 파라미터 조합을 생성한다."""
    tenkans = cfg["tenkan_period"]
    kijuns = cfg["kijun_period"]
    spans = cfg["senkou_span_b_period"]
    results = []
    for t in tenkans:
        for k in kijuns:
            for s in spans:
                results.append({
                    "type": "ICHIMOKU",
                    "tenkan_period": t,
                    "kijun_period": k,
                    "senkou_span_b_period": s
                })
    return results


def get_psar_param_dicts(cfg: dict) -> List[dict]:
    """PSAR 파라미터 조합을 생성한다."""
    steps = cfg["acceleration_step"]
    maxes = cfg["acceleration_max"]
    results = []
    for st in steps:
        for mx in maxes:
            results.append({
                "type": "PSAR",
                "acc_step": st,
                "acc_max": mx
            })
    return results


def get_supertrend_param_dicts(cfg: dict) -> List[dict]:
    """SUPERTREND 파라미터 조합을 생성한다."""
    atrs = cfg["atr_period"]
    mults = cfg["multiplier"]
    results = []
    for a in atrs:
        for m in mults:
            results.append({
                "type": "SUPERTREND",
                "atr_period": a,
                "multiplier": m
            })
    return results


def get_donchian_param_dicts(cfg: dict) -> List[dict]:
    """돈채인 채널(DONCHIAN_CHANNEL) 파라미터 조합을 생성한다."""
    lbs = cfg["lookback_periods"]
    results = []
    for lb in lbs:
        results.append({
            "type": "DONCHIAN_CHANNEL",
            "lookback": lb
        })
    return results


def get_stoch_param_dicts(cfg: dict) -> List[dict]:
    """Stochastic (STOCH) 파라미터 조합을 생성한다."""
    k_list = cfg["k_period"]
    d_list = cfg["d_period"]
    thr_sets = cfg.get("thresholds", [])
    results = []
    for k in k_list:
        for d in d_list:
            if len(thr_sets) == 0:
                results.append({
                    "type": "STOCH",
                    "k_period": k,
                    "d_period": d
                })
            else:
                for thr in thr_sets:
                    low, high = thr
                    results.append({
                        "type": "STOCH",
                        "k_period": k,
                        "d_period": d,
                        "oversold": low,
                        "overbought": high
                    })
    return results


def get_stoch_rsi_param_dicts(cfg: dict) -> List[dict]:
    """Stochastic RSI (STOCH_RSI) 파라미터 조합을 생성한다."""
    lb_list = cfg["lookback_periods"]
    k_list = cfg["k_period"]
    d_list = cfg["d_period"]
    thr_sets = cfg.get("thresholds", [])
    results = []
    for lb in lb_list:
        for k in k_list:
            for d in d_list:
                if len(thr_sets) == 0:
                    results.append({
                        "type": "STOCH_RSI",
                        "lookback": lb,
                        "k_period": k,
                        "d_period": d
                    })
                else:
                    for thr in thr_sets:
                        low, high = thr
                        results.append({
                            "type": "STOCH_RSI",
                            "lookback": lb,
                            "k_period": k,
                            "d_period": d,
                            "oversold": low,
                            "overbought": high
                        })
    return results


def get_mfi_param_dicts(cfg: dict) -> List[dict]:
    """MFI (Money Flow Index) 파라미터 조합을 생성한다."""
    lb_list = cfg["lookback_periods"]
    thr_sets = cfg.get("thresholds", [])
    results = []
    for lb in lb_list:
        if not thr_sets:
            results.append({
                "type": "MFI",
                "lookback": lb
            })
        else:
            for thr in thr_sets:
                low, high = thr
                results.append({
                    "type": "MFI",
                    "lookback": lb,
                    "oversold": low,
                    "overbought": high
                })
    return results


def get_vwap_param_dicts(cfg: dict) -> List[dict]:
    """VWAP 파라미터 조합을 생성한다. (특별한 파라미터 없음)"""
    return [{
        "type": "VWAP"
    }]

def get_indicator_param_dicts() -> Dict[str, List[dict]]:
    """
    indicator_config.py의 모든 지표에 대한 파라미터 조합을 만든다.
    """
    result = {}
    for indicator, cfg in INDICATOR_CONFIG.items():
        if indicator == "MA":
            combos = get_ma_param_dicts(cfg)
        elif indicator == "RSI":
            combos = get_rsi_param_dicts(cfg)
        elif indicator == "OBV":
            combos = get_obv_param_dicts(cfg)  # 수정됨
        elif indicator == "MACD":
            combos = get_macd_param_dicts(cfg)
        elif indicator == "DMI_ADX":
            combos = get_dmi_adx_param_dicts(cfg)
        elif indicator == "BOLL":
            combos = get_boll_param_dicts(cfg)
        elif indicator == "ICHIMOKU":
            combos = get_ichimoku_param_dicts(cfg)
        elif indicator == "PSAR":
            combos = get_psar_param_dicts(cfg)
        elif indicator == "SUPERTREND":
            combos = get_supertrend_param_dicts(cfg)
        elif indicator == "DONCHIAN_CHANNEL":
            combos = get_donchian_param_dicts(cfg)
        elif indicator == "STOCH":
            combos = get_stoch_param_dicts(cfg)
        elif indicator == "STOCH_RSI":
            combos = get_stoch_rsi_param_dicts(cfg)
        elif indicator == "MFI":
            combos = get_mfi_param_dicts(cfg)
        elif indicator == "VWAP":
            combos = get_vwap_param_dicts(cfg)
        else:
            raise ValueError(f"알 수 없는 지표: {indicator}")

        result[indicator] = combos

    return result


def generate_indicator_combos() -> List[List[dict]]:
    """
    INDICATOR_COMBO_SIZES에 기초해 여러 지표를 조합한 파라미터 세트들을 생성한다.
    """
    indicator_param_dicts = get_indicator_param_dicts()
    indicator_names = list(indicator_param_dicts.keys())
    all_combos = []

    for combo_size in INDICATOR_COMBO_SIZES:
        # 서로 다른 indicator를 combo_size개 선택한 뒤, 각 파라미터들의 product를 생성
        for indicator_subset in itertools.combinations(indicator_names, combo_size):
            param_lists = [indicator_param_dicts[name] for name in indicator_subset]
            for merged_tuple in itertools.product(*param_lists):
                all_combos.append(list(merged_tuple))

    return all_combos


def _test_count() -> None:
    """전체 콤보 개수를 간단히 테스트 출력해 본다."""
    combos = generate_indicator_combos()
    print(f"전체 콤보 개수: {len(combos)}")

    dicts_by_indicator = get_indicator_param_dicts()
    for ind_name, plist in dicts_by_indicator.items():
        print(f"{ind_name}: {len(plist)} combos")


if __name__ == "__main__":
    _test_count()
