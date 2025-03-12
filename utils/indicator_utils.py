# gptbitcoin/utils/indicator_utils.py
# 보조지표를 계산하기 위해 필요한 최대 윈도우(봉 개수)를 구한다.
# 각 지표별 파라미터(기간 등)를 참고하여, 가장 긴 윈도우를 추출한다.

from typing import Dict, Any, List

from config.indicator_config import INDICATOR_CONFIG


def get_required_warmup_bars(cfg: Dict[str, Any]) -> int:
    """
    보조지표 계산 시 필요한 최대 윈도우(봉 개수)를 찾는다.
    cfg 인자로 MA, RSI, OBV, Filter, SR, CB, MACD, DMI_ADX, BOLL, ICHIMOKU,
    PSAR, SUPERTREND, FIBO 등에 대한 파라미터를 전달받는다.

    Returns:
        int: 필요한 최대 봉 개수 (가장 긴 지표 윈도우)
    """
    candidates: List[int] = []

    # 1) MA: short_ma_periods + long_ma_periods 중 최댓값
    if "MA" in cfg:
        short_list = cfg["MA"].get("short_ma_periods", [])
        long_list = cfg["MA"].get("long_ma_periods", [])
        if short_list or long_list:
            candidates.append(max(short_list + long_list))

    # 2) RSI: lookback_periods 중 최댓값
    if "RSI" in cfg:
        lookback_list = cfg["RSI"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # 3) OBV: short_ma_periods + long_ma_periods 중 최댓값
    if "OBV" in cfg:
        sp_list = cfg["OBV"].get("short_ma_periods", [])
        lp_list = cfg["OBV"].get("long_ma_periods", [])
        if sp_list or lp_list:
            candidates.append(max(sp_list + lp_list))

    # 4) Filter: lookback_periods 중 최댓값
    if "Filter" in cfg:
        lookback_list = cfg["Filter"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # 5) SR: lookback_periods 중 최댓값
    if "SR" in cfg:
        lookback_list = cfg["SR"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # 6) CB (Channel Breakout): lookback_periods 중 최댓값
    if "CB" in cfg:
        lookback_list = cfg["CB"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # -------------------
    # 여기서부터 새로 추가된 지표
    # -------------------

    # 7) MACD: slow_periods + signal_periods가 가장 큰 조합을 고려 (fast_period는 일반적으로 slow보다 작음)
    #    여기서는 단순히 max(slow) + max(signal)로 대략 추정
    if "MACD" in cfg:
        slow_list = cfg["MACD"].get("slow_periods", [])
        sig_list = cfg["MACD"].get("signal_periods", [])
        if slow_list and sig_list:
            max_slow = max(slow_list)
            max_sig = max(sig_list)
            # MACD 특성상 slow_period + signal_period 정도를 워밍업으로 추정
            candidates.append(max_slow + max_sig)

    # 8) DMI_ADX: dmi_periods 중 최댓값
    if "DMI_ADX" in cfg:
        dmi_list = cfg["DMI_ADX"].get("dmi_periods", [])
        if dmi_list:
            candidates.append(max(dmi_list))

    # 9) BOLL: lookback_periods 중 최댓값
    if "BOLL" in cfg:
        lb_list = cfg["BOLL"].get("lookback_periods", [])
        if lb_list:
            candidates.append(max(lb_list))

    # 10) ICHIMOKU: tenkan_period, kijun_period, senkou_span_b_period 중 최댓값
    if "ICHIMOKU" in cfg:
        t_list = cfg["ICHIMOKU"].get("tenkan_period", [])
        k_list = cfg["ICHIMOKU"].get("kijun_period", [])
        s_list = cfg["ICHIMOKU"].get("senkou_span_b_period", [])
        # 각 리스트가 비어 있을 수 있으므로 max 시 에러 방지
        t_max = max(t_list) if t_list else 0
        k_max = max(k_list) if k_list else 0
        s_max = max(s_list) if s_list else 0
        # 세 값 중 가장 큰 값
        candidates.append(max(t_max, k_max, s_max))

    # 11) PSAR: acceleration_step / acceleration_max는 윈도우 개념이 아님 → 0 처리
    #    pandas_ta.psar도 내부 계산에 특정 길이가 필요할 수 있으나, config 파라미터로 window는 없으므로 0
    if "PSAR" in cfg:
        # 원한다면 일정 기본값(예:20)로 가정할 수도 있으나 여기서는 0
        candidates.append(0)

    # 12) SUPERTREND: atr_period 중 최댓값
    if "SUPERTREND" in cfg:
        atr_list = cfg["SUPERTREND"].get("atr_period", [])
        if atr_list:
            candidates.append(max(atr_list))

    # 13) FIBO: rolling_window (기본 20).  여러 levels가 있어도 rolling_window는 하나만 쓰임
    if "FIBO" in cfg:
        fibo_cfg = cfg["FIBO"]
        roll_win = fibo_cfg.get("rolling_window", 20)
        # rolling_window라는 키가 없으면 20으로 가정
        candidates.append(roll_win)

    if not candidates:
        return 0
    return max(candidates)


def main():
    """
    테스트용 main 함수:
    indicator_config.py의 INDICATOR_CONFIG를 불러와
    필요한 워밍업 봉 수를 계산 후 출력한다.
    """
    needed = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[indicator_utils] 워밍업에 필요한 최대 봉 수: {needed}")


if __name__ == "__main__":
    main()
