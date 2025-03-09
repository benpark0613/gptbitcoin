# gptbitcoin/backtest/combo_generator.py
# 최소한의 한글 주석, 구글 스타일 docstring
# Filter 룰에서 separate=1,575 / uniform=1,260을 정확히 생성하도록 구현.
# 다른 지표(MA, RSI, SR, CB, OBV)는 기존 time_delay를
# buy_time_delay, sell_time_delay 형태로 동일한 값으로 넣도록 수정.

import itertools
from typing import List, Dict

from config.indicator_config import INDICATOR_COMBO_SIZES, INDICATOR_CONFIG


def get_ma_param_dicts(ma_config: dict) -> List[dict]:
    """
    MA(이동평균) 파라미터 조합을 생성한다.
    수정: time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
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
                        param = {
                            "type": "MA",
                            "short_period": sp,
                            "long_period": lp,
                            "band_filter": bf,
                            "buy_time_delay": td,    # 수정
                            "sell_time_delay": td,   # 수정
                            "holding_period": hp
                        }
                        results.append(param)
    return results


def get_rsi_param_dicts(rsi_config: dict) -> List[dict]:
    """
    RSI 파라미터 조합을 생성한다.
    수정: time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
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
                    param = {
                        "type": "RSI",
                        "lookback": lb,
                        "overbought": overbought,
                        "oversold": oversold,
                        "buy_time_delay": td,     # 수정
                        "sell_time_delay": td,    # 수정
                        "holding_period": hp
                    }
                    results.append(param)
    return results


def get_sr_param_dicts(sr_config: dict) -> List[dict]:
    """
    SR(지지/저항) 파라미터 조합을 생성한다.
    수정: time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
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
                    param = {
                        "type": "SR",
                        "lookback": lb,
                        "band_filter": bf,
                        "buy_time_delay": td,     # 수정
                        "sell_time_delay": td,    # 수정
                        "holding_period": hp
                    }
                    results.append(param)
    return results


def get_channel_param_dicts(cb_config: dict) -> List[dict]:
    """
    CB(Channel Breakout) 파라미터 조합을 생성한다.
    수정: time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
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
                        param = {
                            "type": "CB",
                            "lookback": lb,
                            "c_channel": c,
                            "band_filter": bf,
                            "buy_time_delay": td,   # 수정
                            "sell_time_delay": td,  # 수정
                            "holding_period": hp
                        }
                        results.append(param)
    return results


def get_obv_param_dicts(obv_config: dict) -> List[dict]:
    """
    OBV 파라미터 조합을 생성한다.
    수정: time_delays를 buy_time_delay, sell_time_delay로 동일하게 세팅.
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
                        param = {
                            "type": "OBV",
                            "short_period": sp,
                            "long_period": lp,
                            "band_filter": bf,
                            "buy_time_delay": td,   # 수정
                            "sell_time_delay": td,  # 수정
                            "holding_period": hp
                        }
                        results.append(param)
    return results


def get_filter_param_dicts() -> Dict[str, any]:
    """
    Filter 룰 파라미터 조합을 '논문 그대로' 고정 생성:
      separate_delay => 1,575개
      uniform_delay  => 1,260개
      총 2,835개

    (5 × 21 × 5 × 3 = 1,575)
    (5 × 7 × 6 × 6 = 1,260)

    Returns:
        {
          "combos": [...],
          "sep_count": 1575,
          "uni_count": 1260
        }
    """
    separate_combos = []
    uniform_combos = []

    # 1) 별도 딜레이(separate_delay)
    #   - lookback_periods= [1, 2, 6, 12, 24] => 5개
    #   - (x, y) 쌍 => y<x => 총 21쌍
    #   - (buy_time_delay, sell_time_delay) => 5쌍 ([ (1,0),(1,1),(2,0),(2,1),(2,2) ])
    #   - holding_period= [6, 12, 18] => 3개
    lb_list = [1, 2, 6, 12, 24]
    bf_candidates = [0.05, 0.1, 0.5, 1, 5, 10, 20]  # buy_filter
    # sell_filter도 동일, y < x => 21쌍
    xy_pairs = []
    for i in range(len(bf_candidates)):
        for j in range(len(bf_candidates)):
            if bf_candidates[j] < bf_candidates[i]:
                xy_pairs.append((bf_candidates[i], bf_candidates[j]))
    # => xy_pairs는 21개

    # buy_time_delay=[1,2], sell_time_delay=[0,1,2] => 총 5쌍
    delay_pairs = [(1,0), (1,1), (2,0), (2,1), (2,2)]
    hps = [6, 12, 18]

    for lb in lb_list:
        for (x, y) in xy_pairs:
            for (btd, std) in delay_pairs:
                for hp in hps:
                    param = {
                        "type": "Filter",
                        "variant": "separate_delay",
                        "lookback": lb,
                        "buy_filter": x,   # x%
                        "sell_filter": y,  # y%
                        "buy_time_delay": btd,  # <- separate
                        "sell_time_delay": std,  # <- separate
                        "holding_period": hp
                    }
                    separate_combos.append(param)

    # 2) 동일 딜레이(uniform_delay)
    #   - lookback_periods= [1, 2, 6, 12, 24] => 5개
    #   - (x, y) => x==y => 7개 => 0.05, 0.1, 0.5, 1, 5, 10, 20
    #   - uniform_time_delays= [0,1,2,3,4,5] => 6개
    #   - holding_period= [6,12,18,20,24,float('inf')] => 총 6개
    #   => 5×7×6×6=1,260
    hps_uniform = [6,12,18,20,24,float('inf')]
    uniform_tds = [0,1,2,3,4,5]
    bf_candidates_same = [0.05, 0.1, 0.5, 1, 5, 10, 20]
    xy_same = [(v, v) for v in bf_candidates_same]  # x==y

    for lb in lb_list:
        for (x, y) in xy_same:  # x==y
            for utd in uniform_tds:
                for hp in hps_uniform:
                    param = {
                        "type": "Filter",
                        "variant": "uniform_delay",
                        "lookback": lb,
                        "buy_filter": x,
                        "sell_filter": y,
                        "buy_time_delay": utd,   # <- uniform
                        "sell_time_delay": utd,  # <- uniform
                        "holding_period": hp
                    }
                    uniform_combos.append(param)

    combos = separate_combos + uniform_combos
    return {
        "combos": combos,
        "sep_count": len(separate_combos),
        "uni_count": len(uniform_combos)
    }


def get_filter_param_dicts_from_config(_filter_cfg: dict) -> Dict[str, any]:
    """
    기존 config를 무시하고, 논문 로직 그대로 생성하려면 get_filter_param_dicts() 직접 사용.
    """
    # 여기서는 그냥 get_filter_param_dicts() 직접 호출
    return get_filter_param_dicts()


def get_indicator_param_dicts() -> Dict[str, List[dict]]:
    """
    각 지표별 파라미터 조합을 얻는다. Filter는 별도의 고정 로직 사용.
    """
    result = {}
    global _filter_sep_count, _filter_uni_count

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
            fdict = get_filter_param_dicts()  # 논문 그대로 생성
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
        else:
            result[indicator] = []
    return result


_filter_sep_count = 0
_filter_uni_count = 0


def generate_indicator_combos() -> List[List[dict]]:
    """
    indicator_config.py의 INDICATOR_COMBO_SIZES만큼
    서로 다른 indicator들을 조합하여 파라미터 리스트를 생성한다.

    Returns:
        List[List[dict]]: 예) [[param1, param2], ...] 식의 콤보 리스트
    """
    indicator_param_dicts = get_indicator_param_dicts()
    indicator_names = list(indicator_param_dicts.keys())
    all_combos = []

    # combo_size에 따라 서로 다른 indicator들을 고른 뒤,
    # 그 각 indicator별 파라미터 리스트의 카테시안 곱(product)을 형성
    for combo_size in INDICATOR_COMBO_SIZES:
        for indicator_subset in itertools.combinations(indicator_names, combo_size):
            param_lists = [indicator_param_dicts[name] for name in indicator_subset]
            for merged_tuple in itertools.product(*param_lists):
                combo_list = list(merged_tuple)
                all_combos.append(combo_list)
    return all_combos


def _test_count() -> None:
    """
    전체 파라미터 조합이 몇 개인지와,
    Filter 룰에서 separate + uniform 개수를 합산해 출력한다.
    """
    combos = generate_indicator_combos()
    print(f"\n[전체 지표 콤보] 총 개수: {len(combos)}")

    # 지표별 파라미터 목록
    indicator_param_dicts = get_indicator_param_dicts()
    global _filter_sep_count, _filter_uni_count

    for indicator, plist in indicator_param_dicts.items():
        if indicator == "Filter":
            sc = _filter_sep_count
            uc = _filter_uni_count
            print(f"[Filter] separate={sc} + uniform={uc} => {sc + uc} total (지표별 파라미터)")
        else:
            print(f"[{indicator}] combos={len(plist)}")

    # 샘플 3개 출력
    for i in range(min(3, len(combos))):
        print(f"샘플[{i+1}]: {combos[i]}")


if __name__ == "__main__":
    _test_count()
