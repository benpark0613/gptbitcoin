# gptbitcoin/backtest/combo_generator.py
# 주석은 한글로 작성 (필요 최소한만), 구글 스타일 Docstring
# config에 정의된 모든 파라미터를 콤보 생성 시 사용하도록 수정
# inf인 경우 백테스트에서 “다른 매매 시그널이 나오기 전까지 계속 보유”로 처리
# 매수/매도 지연은 동일(delay)하게 적용
# 일반적인 전제: 단기가 장기 이상이면 스킵

import itertools
from typing import List, Dict, Any

# 본 예제에서는 config 패키지의 indicator_config 모듈을 import한다고 가정
from config.indicator_config import INDICATOR_CONFIG, INDICATOR_COMBO_SIZES

def get_ma_param_dicts(ma_config: dict) -> List[dict]:
    """
    이동평균(MA) 파라미터 조합을 생성한다.

    Args:
        ma_config (dict): 이동평균 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
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
                # 단기가 장기 이상이면 스킵
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

    Args:
        rsi_config (dict): RSI 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
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

    Args:
        sr_config (dict): SR 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
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


def get_filter_param_dicts(_filter_cfg: dict) -> List[dict]:
    """
    Filter 룰 파라미터 조합을 생성한다. (uniform_filters만 사용)

    Args:
        _filter_cfg (dict): 필터룰 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
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

    return uniform_combos


def get_channel_param_dicts(cb_config: dict) -> List[dict]:
    """
    채널 돌파(Channel Breakout) 파라미터 조합을 생성한다.

    Args:
        cb_config (dict): 채널 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
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
    OBV(On-Balance Volume) 파라미터 조합을 생성한다.

    Args:
        obv_config (dict): OBV 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
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

    Args:
        macd_config (dict): MACD 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
    """
    results = []
    fasts = macd_config.get("fast_periods", [])
    slows = macd_config.get("slow_periods", [])
    signals = macd_config.get("signal_periods", [])
    band_filters = macd_config.get("band_filters", [])
    time_delays = macd_config.get("time_delays", [])
    holding_periods = macd_config.get("holding_periods", [])

    for f in fasts:
        for s in slows:
            if f >= s:
                continue
            for sig in signals:
                for bf in band_filters:
                    for td in time_delays:
                        for hp in holding_periods:
                            results.append({
                                "type": "MACD",
                                "fast_period": f,
                                "slow_period": s,
                                "signal_period": sig,
                                "band_filter": bf,
                                "buy_time_delay": td,
                                "sell_time_delay": td,
                                "holding_period": hp
                            })
    return results


def get_dmi_adx_param_dicts(dmi_config: dict) -> List[dict]:
    """
    DMI와 ADX 파라미터 조합을 생성한다.

    Args:
        dmi_config (dict): DMI/ADX 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
    """
    results = []
    dmi_periods = dmi_config.get("dmi_periods", [])
    adx_ths = dmi_config.get("adx_thresholds", [])
    band_filters = dmi_config.get("band_filters", [])
    time_delays = dmi_config.get("time_delays", [])
    holding_periods = dmi_config.get("holding_periods", [])

    for dp in dmi_periods:
        for ath in adx_ths:
            for bf in band_filters:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "DMI_ADX",
                            "dmi_period": dp,
                            "adx_threshold": ath,
                            "band_filter": bf,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_boll_param_dicts(boll_config: dict) -> List[dict]:
    """
    볼린저 밴드(BOLL) 파라미터 조합을 생성한다.

    Args:
        boll_config (dict): 볼린저 밴드 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
    """
    results = []
    lookbacks = boll_config.get("lookback_periods", [])
    stddevs = boll_config.get("stddev_multipliers", [])
    band_filters = boll_config.get("band_filters", [])
    time_delays = boll_config.get("time_delays", [])
    holding_periods = boll_config.get("holding_periods", [])

    for lb in lookbacks:
        for sd in stddevs:
            for bf in band_filters:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "BOLL",
                            "lookback": lb,
                            "stddev_mult": sd,
                            "band_filter": bf,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_ichimoku_param_dicts(ich_config: dict) -> List[dict]:
    """
    일목균형표(Ichimoku) 파라미터 조합을 생성한다.

    Args:
        ich_config (dict): 일목균형표 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
    """
    results = []
    tenkans = ich_config.get("tenkan_period", [])
    kijuns = ich_config.get("kijun_period", [])
    spans = ich_config.get("senkou_span_b_period", [])
    band_filters = ich_config.get("band_filters", [])
    time_delays = ich_config.get("time_delays", [])
    holding_periods = ich_config.get("holding_periods", [])

    for t in tenkans:
        for k in kijuns:
            for s in spans:
                for bf in band_filters:
                    for td in time_delays:
                        for hp in holding_periods:
                            results.append({
                                "type": "ICHIMOKU",
                                "tenkan_period": t,
                                "kijun_period": k,
                                "senkou_span_b_period": s,
                                "band_filter": bf,
                                "buy_time_delay": td,
                                "sell_time_delay": td,
                                "holding_period": hp
                            })
    return results


def get_psar_param_dicts(psar_config: dict) -> List[dict]:
    """
    Parabolic SAR(PSAR) 파라미터 조합을 생성한다.

    Args:
        psar_config (dict): PSAR 관련 설정 딕셔너리
          - 예: {
              "lookback_periods": [5, 7],
              "acceleration_step": [0.01, 0.02],
              "acceleration_max": [0.2],
              "band_filters": [0, 0.03],
              "time_delays": [0, 2],
              "holding_periods": [float('inf')]
            }

    Returns:
        List[dict]: PSAR 파라미터 조합 리스트
    """
    results = []
    lookbacks = psar_config.get("lookback_periods", [])
    acc_steps = psar_config.get("acceleration_step", [])
    acc_maxes = psar_config.get("acceleration_max", [])
    band_filters = psar_config.get("band_filters", [])
    time_delays = psar_config.get("time_delays", [])
    holding_periods = psar_config.get("holding_periods", [])

    for lb in lookbacks:
        for st in acc_steps:
            for mx in acc_maxes:
                for bf in band_filters:
                    for td in time_delays:
                        for hp in holding_periods:
                            results.append({
                                "type": "PSAR",
                                "init_lookback": lb,  # or "lookback" 등 원하는 키 이름
                                "acc_step": st,
                                "acc_max": mx,
                                "band_filter": bf,
                                "buy_time_delay": td,
                                "sell_time_delay": td,
                                "holding_period": hp
                            })
    return results


def get_supertrend_param_dicts(st_config: dict) -> List[dict]:
    """
    슈퍼트렌드(SuperTrend) 파라미터 조합을 생성한다.

    Args:
        st_config (dict): SuperTrend 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
    """
    results = []
    atr_ps = st_config.get("atr_period", [])
    mults = st_config.get("multiplier", [])
    band_filters = st_config.get("band_filters", [])
    time_delays = st_config.get("time_delays", [])
    holding_periods = st_config.get("holding_periods", [])

    for ap in atr_ps:
        for mt in mults:
            for bf in band_filters:
                for td in time_delays:
                    for hp in holding_periods:
                        results.append({
                            "type": "SUPERTREND",
                            "atr_period": ap,
                            "multiplier": mt,
                            "band_filter": bf,
                            "buy_time_delay": td,
                            "sell_time_delay": td,
                            "holding_period": hp
                        })
    return results


def get_fibo_param_dicts(fibo_config: dict) -> List[dict]:
    """
    피보나치(Fibonacci) 파라미터 조합을 생성한다.

    Args:
        fibo_config (dict): 피보나치 관련 설정 딕셔너리

    Returns:
        List[dict]: 파라미터 조합 리스트
    """
    results = []
    levels_list = fibo_config.get("levels", [])
    band_filters = fibo_config.get("band_filters", [])
    time_delays = fibo_config.get("time_delays", [])
    holding_periods = fibo_config.get("holding_periods", [])

    for lv_set in levels_list:
        for bf in band_filters:
            for td in time_delays:
                for hp in holding_periods:
                    results.append({
                        "type": "FIBO",
                        "levels": lv_set,
                        "band_filter": bf,
                        "buy_time_delay": td,
                        "sell_time_delay": td,
                        "holding_period": hp
                    })
    return results


def get_indicator_param_dicts() -> Dict[str, List[dict]]:
    """
    지표별 파라미터 조합을 전부 생성해서 반환한다.

    Returns:
        Dict[str, List[dict]]: 지표 이름을 키로, 파라미터 dict 리스트를 값으로 가지는 딕셔너리
    """
    result = {}

    # 각 지표별로 개별 함수 호출
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
            combos = get_filter_param_dicts(cfg)
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
    INDICATOR_COMBO_SIZES에 명시된 개수만큼 서로 다른 지표를 조합한다.

    Returns:
        List[List[dict]]: 예) [[param1, param2], ...] 식의 콤보 리스트
    """
    indicator_param_dicts = get_indicator_param_dicts()
    indicator_names = list(indicator_param_dicts.keys())
    all_combos = []

    for combo_size in INDICATOR_COMBO_SIZES:
        # 서로 다른 indicator들의 부분집합
        for indicator_subset in itertools.combinations(indicator_names, combo_size):
            # 카테시안 곱
            param_lists = [indicator_param_dicts[name] for name in indicator_subset]
            for merged_tuple in itertools.product(*param_lists):
                combo_list = list(merged_tuple)
                all_combos.append(combo_list)

    return all_combos


def _test_count() -> None:
    """
    단순 테스트용: 전체 파라미터 콤보 개수를 출력한다.
    """
    combos = generate_indicator_combos()
    print(f"[전체 지표 콤보] 총 개수: {len(combos)}")

    indicator_param_dicts = get_indicator_param_dicts()
    for indicator, plist in indicator_param_dicts.items():
        print(f"[{indicator}] combos={len(plist)}")


if __name__ == "__main__":
    _test_count()
