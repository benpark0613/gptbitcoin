# gptbitcoin/utils/indicator_utils.py
# 보조지표 계산 시 필요한 최대 윈도우(봉 개수)를 계산한다. (구글 스타일 Docstring, 최소한의 한글 주석)

from typing import Dict, Any, List

def get_required_warmup_bars(cfg: Dict[str, Any]) -> int:
    """
    주어진 지표 설정(cfg)에 따라 필요한 최대 워밍업(봉 개수)을 계산한다.
    지표별로 사용되는 기간(lookback 등) 중 가장 큰 값을 찾아 반환한다.

    Args:
        cfg (Dict[str, Any]): indicator_config.py 등에서 불러온 지표 설정 정보

    Returns:
        int: 필요한 최대 봉(캔들) 개수
    """
    candidates: List[int] = []

    # 1) MA
    # short_ma_periods, long_ma_periods 중 최댓값
    if "MA" in cfg:
        shorts = cfg["MA"].get("short_ma_periods", [])
        longs = cfg["MA"].get("long_ma_periods", [])
        if shorts or longs:
            candidates.append(max(shorts + longs))

    # 2) RSI
    if "RSI" in cfg:
        lb_list = cfg["RSI"].get("lookback_periods", [])
        if lb_list:
            candidates.append(max(lb_list))

    # 3) OBV
    # 과거에는 'absolute_threshold_periods'가 있었다면,
    # 이제 config.indicator_config.py에서 short_ma_periods, long_ma_periods를 사용하므로 아래와 같이 변경
    if "OBV" in cfg:
        sp_list = cfg["OBV"].get("short_ma_periods", [])
        lp_list = cfg["OBV"].get("long_ma_periods", [])
        if sp_list or lp_list:
            candidates.append(max(sp_list + lp_list))

    # 4) MACD
    # (slow_period, signal_period) 합이 실제 lookback에 영향
    if "MACD" in cfg:
        slows = cfg["MACD"].get("slow_periods", [])
        signals = cfg["MACD"].get("signal_periods", [])
        if slows and signals:
            # ex) slow=30, signal=12 => 대략 42개 봉
            candidates.append(max(slows) + max(signals))

    # 5) DMI_ADX
    if "DMI_ADX" in cfg:
        lookbacks = cfg["DMI_ADX"].get("lookback_periods", [])
        if lookbacks:
            candidates.append(max(lookbacks))

    # 6) BOLL
    if "BOLL" in cfg:
        lb_boll = cfg["BOLL"].get("lookback_periods", [])
        if lb_boll:
            candidates.append(max(lb_boll))

    # 7) ICHIMOKU
    # 일목균형표는 (tenkan_period, kijun_period, senkou_span_b_period) 중 최댓값
    if "ICHIMOKU" in cfg:
        tenkans = cfg["ICHIMOKU"].get("tenkan_period", [])
        kijuns = cfg["ICHIMOKU"].get("kijun_period", [])
        spans = cfg["ICHIMOKU"].get("senkou_span_b_period", [])
        t_max = max(tenkans) if tenkans else 0
        k_max = max(kijuns) if kijuns else 0
        s_max = max(spans) if spans else 0
        candidates.append(max(t_max, k_max, s_max))

    # 8) PSAR
    # 별도 lookback 없음, 가속도(acceleration)와 max값은 기간이 아님

    # 9) SUPERTREND
    if "SUPERTREND" in cfg:
        atrs = cfg["SUPERTREND"].get("atr_period", [])
        if atrs:
            candidates.append(max(atrs))

    # 10) DONCHIAN_CHANNEL
    if "DONCHIAN_CHANNEL" in cfg:
        dc_list = cfg["DONCHIAN_CHANNEL"].get("lookback_periods", [])
        if dc_list:
            candidates.append(max(dc_list))

    # 11) STOCH
    if "STOCH" in cfg:
        k_list = cfg["STOCH"].get("k_period", [])
        d_list = cfg["STOCH"].get("d_period", [])
        if k_list and d_list:
            # 대략 max(k) + max(d) 봉
            candidates.append(max(k_list) + max(d_list))

    # 12) STOCH_RSI
    if "STOCH_RSI" in cfg:
        # config에는 "rsi_periods", "stoch_periods", "k_period", "d_period" 등 존재
        srsi_cfg = cfg["STOCH_RSI"]
        rsi_list = srsi_cfg.get("rsi_periods", [])
        stoch_list = srsi_cfg.get("stoch_periods", [])
        k_list = srsi_cfg.get("k_period", [])
        d_list = srsi_cfg.get("d_period", [])

        if rsi_list and stoch_list and k_list and d_list:
            # 예: max(rsi_list)=21, max(stoch_list)=21, max(k_list)=5, max(d_list)=5 => 52개 정도 필요
            candidates.append(max(rsi_list) + max(stoch_list) + max(k_list) + max(d_list))

    # 13) MFI
    if "MFI" in cfg:
        lb_list = cfg["MFI"].get("lookback_periods", [])
        if lb_list:
            candidates.append(max(lb_list))

    # 14) VWAP
    # 별도 기간 파라미터 없음

    if not candidates:
        return 0
    return max(candidates)


def main():
    """
    테스트용 메인 함수. indicator_config.py의 설정을 직접 불러와
    필요한 워밍업 봉 수를 출력하는 예시.
    """
    from config.indicator_config import INDICATOR_CONFIG
    needed = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[indicator_utils] 워밍업에 필요한 최대 봉 수: {needed}")


if __name__ == "__main__":
    main()
