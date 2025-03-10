# gptbitcoin/backtest/combo_generator.py
# 최소한의 한글 주석, 구글 스타일 docstring
# 새로 추가된 MACD, DMI_ADX, BOLL, ICHIMOKU, PSAR, SUPERTREND, FIBO 지표도 처리

import itertools
from typing import List, Dict, Any

from config.indicator_config import INDICATOR_CONFIG, INDICATOR_COMBO_SIZES


def get_ma_param_dicts(ma_config: dict) -> List[dict]:
    """
    MA 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    shorts = ma_config.get("short_ma_periods", [])
    longs = ma_config.get("long_ma_periods", [])
    band_filters = ma_config.get("band_filters", [])
    time_delays = ma_config.get("time_delays", [])
    holding_periods = ma_config.get("holding_periods", [])

    for sp in shorts:
        for lp in longs:
            if sp >= lp:
                continue
            for bf in band_filters:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "MA",
                            "short_period": sp,
                            "long_period": lp,
                            "band_filter": bf,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_rsi_param_dicts(rsi_config: dict) -> List[dict]:
    """
    RSI 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    lookbacks = rsi_config.get("lookback_periods", [])
    thresholds = rsi_config.get("thresholds", [])
    time_delays = rsi_config.get("time_delays", [])
    holding_periods = rsi_config.get("holding_periods", [])

    for lb in lookbacks:
        for thr in thresholds:
            overbought = 50 + thr
            oversold = 50 - thr
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "RSI",
                        "lookback": lb,
                        "overbought": overbought,
                        "oversold": oversold,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_sr_param_dicts(sr_config: dict) -> List[dict]:
    """
    SR(지지/저항) 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    lookbacks = sr_config.get("lookback_periods", [])
    band_filters = sr_config.get("band_filters", [])
    time_delays = sr_config.get("time_delays", [])
    holding_periods = sr_config.get("holding_periods", [])

    for lb in lookbacks:
        for bf in band_filters:
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "SR",
                        "lookback": lb,
                        "band_filter": bf,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_filter_param_dicts(_filter_cfg: dict) -> Dict[str, Any]:
    """
    Filter 룰 파라미터 조합을 생성한다.
    여기서는 uniform_filters만 사용 (아래위 동일 퍼센트).
    """
    uniform_combos = []

    lb_list = _filter_cfg.get("lookback_periods", [])
    filters = _filter_cfg.get("uniform_filters", [])
    time_delays = _filter_cfg.get("uniform_time_delays", [])
    holding_periods = _filter_cfg.get("holding_periods", [])

    for lb in lb_list:
        for ft in filters:
            for td in time_delays:
                for hp in holding_periods:
                    uniform_combos.append({
                        "type": "Filter",
                        "variant": "uniform_delay",
                        "lookback": lb,
                        "buy_filter": ft,
                        "sell_filter": ft,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })

    return {
        "combos": uniform_combos,
        "sep_count": 0,
        "uni_count": len(uniform_combos)
    }


def get_channel_param_dicts(cb_config: dict) -> List[dict]:
    """
    CB(Channel Breakout) 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    lookbacks = cb_config.get("lookback_periods", [])
    c_channels = cb_config.get("c_percent_channels", [])
    band_filters = cb_config.get("band_filters", [])
    time_delays = cb_config.get("time_delays", [])
    holding_periods = cb_config.get("holding_periods", [])

    for lb in lookbacks:
        for c in c_channels:
            for bf in band_filters:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "CB",
                            "lookback": lb,
                            "c_channel": c,
                            "band_filter": bf,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_obv_param_dicts(obv_config: dict) -> List[dict]:
    """
    OBV 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    shorts = obv_config.get("short_ma_periods", [])
    longs = obv_config.get("long_ma_periods", [])
    band_filters = obv_config.get("band_filters", [])
    time_delays = obv_config.get("time_delays", [])
    holding_periods = obv_config.get("holding_periods", [])

    for sp in shorts:
        for lp in longs:
            if sp >= lp:
                continue
            for bf in band_filters:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "OBV",
                            "short_period": sp,
                            "long_period": lp,
                            "band_filter": bf,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_macd_param_dicts(macd_config: dict) -> List[dict]:
    """
    MACD 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    fasts = macd_config.get("fast_periods", [])
    slows = macd_config.get("slow_periods", [])
    signals = macd_config.get("signal_periods", [])
    time_delays = macd_config.get("time_delays", [])
    holding_periods = macd_config.get("holding_periods", [])

    for f in fasts:
        for s in slows:
            if f >= s:
                continue
            for sig in signals:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "MACD",
                            "fast_period": f,
                            "slow_period": s,
                            "signal_period": sig,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_dmi_adx_param_dicts(dmi_config: dict) -> List[dict]:
    """
    DMI & ADX 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    dmi_periods = dmi_config.get("dmi_periods", [])
    adx_ths = dmi_config.get("adx_thresholds", [])
    time_delays = dmi_config.get("time_delays", [])
    holding_periods = dmi_config.get("holding_periods", [])

    for dp in dmi_periods:
        for ath in adx_ths:
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "DMI_ADX",
                        "dmi_period": dp,
                        "adx_threshold": ath,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_boll_param_dicts(boll_config: dict) -> List[dict]:
    """
    볼린저 밴드 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    lookbacks = boll_config.get("lookback_periods", [])
    stddevs = boll_config.get("stddev_multipliers", [])
    time_delays = boll_config.get("time_delays", [])
    holding_periods = boll_config.get("holding_periods", [])

    for lb in lookbacks:
        for sd in stddevs:
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "BOLL",
                        "lookback": lb,
                        "stddev_mult": sd,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_ichimoku_param_dicts(ich_config: dict) -> List[dict]:
    """
    일목균형표(Ichimoku) 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    tenkans = ich_config.get("tenkan_period", [])
    kijuns = ich_config.get("kijun_period", [])
    spans = ich_config.get("senkou_span_b_period", [])
    time_delays = ich_config.get("time_delays", [])
    holding_periods = ich_config.get("holding_periods", [])

    for t in tenkans:
        for k in kijuns:
            for s in spans:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "ICHIMOKU",
                            "tenkan_period": t,
                            "kijun_period": k,
                            "senkou_span_b_period": s,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_psar_param_dicts(psar_config: dict) -> List[dict]:
    """
    Parabolic SAR 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    acc_steps = psar_config.get("acceleration_step", [])
    acc_maxes = psar_config.get("acceleration_max", [])
    time_delays = psar_config.get("time_delays", [])
    holding_periods = psar_config.get("holding_periods", [])

    for st in acc_steps:
        for mx in acc_maxes:
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "PSAR",
                        "acc_step": st,
                        "acc_max": mx,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_supertrend_param_dicts(st_config: dict) -> List[dict]:
    """
    SuperTrend 파라미터 조합을 생성한다.
    time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
    """
    results = []
    atr_ps = st_config.get("atr_period", [])
    mults = st_config.get("multiplier", [])
    time_delays = st_config.get("time_delays", [])
    holding_periods = st_config.get("holding_periods", [])

    for ap in atr_ps:
        for mt in mults:
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "SUPERTREND",
                        "atr_period": ap,
                        "multiplier": mt,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_fibo_param_dicts(fibo_config: dict) -> List[dict]:
    results = []
    levels_list = fibo_config.get("levels", [])
    time_delays = fibo_config.get("time_delays", [])
    holding_periods = fibo_config.get("holding_periods", [])

    for lv_set in levels_list:           # 여러 level set을 순회
        for td in time_delays:
            for hp in holding_periods:
                results.append({
                    "type": "FIBO",
                    "levels": lv_set,   # 세트 하나씩
                    "buy_time_delay": td,
                    "sell_time_delay": td,
                    "holding_period": hp
                })
    return results

def get_indicator_param_dicts() -> Dict[str, List[dict]]:
    """
    각 지표별 파라미터 조합을 얻는다.
    """
    result = {}

    # Filter 통계 변수
    global _filter_sep_count, _filter_uni_count
    _filter_sep_count = 0
    _filter_uni_count = 0

    for indicator, cfg in INDICATOR_CONFIG.items():
        if indicator == "MA":
            combos = get_ma_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "RSI":
            combos = get_rsi_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "SR":
            combos = get_sr_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "Filter":
            fdict = get_filter_param_dicts(cfg)
            combos = fdict["combos"]
            _filter_sep_count = fdict["sep_count"]
            _filter_uni_count = fdict["uni_count"]
            result[indicator] = combos
        elif indicator == "CB":
            combos = get_channel_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "OBV":
            combos = get_obv_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "MACD":
            combos = get_macd_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "DMI_ADX":
            combos = get_dmi_adx_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "BOLL":
            combos = get_boll_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "ICHIMOKU":
            combos = get_ichimoku_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "PSAR":
            combos = get_psar_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "SUPERTREND":
            combos = get_supertrend_param_dicts(cfg)
            result[indicator] = combos
        elif indicator == "FIBO":
            combos = get_fibo_param_dicts(cfg)
            result[indicator] = combos
        else:
            # 알 수 없는 지표
            result[indicator] = []
    return result


def generate_indicator_combos() -> List[List[dict]]:
    """
    indicator_config.py의 INDICATOR_COMBO_SIZES만큼
    서로 다른 indicator들을 조합해 파라미터 리스트를 생성한다.

    Returns:
        List[List[dict]]: 예) [[param1, param2], ...] 식의 콤보 리스트
    """
    indicator_param_dicts = get_indicator_param_dicts()
    indicator_names = list(indicator_param_dicts.keys())
    all_combos = []

    for combo_size in INDICATOR_COMBO_SIZES:
        # 서로 다른 indicator들의 부분집합
        for indicator_subset in itertools.combinations(indicator_names, combo_size):
            # 각 indicator별 파라미터 리스트의 카테시안 곱
            param_lists = [indicator_param_dicts[name] for name in indicator_subset]
            for merged_tuple in itertools.product(*param_lists):
                combo_list = list(merged_tuple)
                all_combos.append(combo_list)
    return all_combos


_filter_sep_count = 0
_filter_uni_count = 0


def _test_count() -> None:
    """
    전체 파라미터 조합이 몇 개인지와,
    Filter 룰에서 separate + uniform 개수를 출력한다.
    """
    combos = generate_indicator_combos()
    print(f"[전체 지표 콤보] 총 개수: {len(combos)}")

    indicator_param_dicts = get_indicator_param_dicts()
    global _filter_sep_count, _filter_uni_count

    for indicator, plist in indicator_param_dicts.items():
        print(f"[{indicator}] combos={len(plist)}")
        if indicator == "Filter":
            sc = _filter_sep_count
            uc = _filter_uni_count
            print(f"    → separate={sc}, uniform={uc}, total={sc + uc}")

if __name__ == "__main__":
    _test_count()
