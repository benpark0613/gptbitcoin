"""
indicator_utils.py

보조지표를 계산하기 위해 필요한 최대 윈도우(봉 개수)를 구하는 함수를 제공한다.
예:
    - MA: short_ma_periods, long_ma_periods 중 최댓값
    - RSI: lookback_periods 중 최댓값
    - OBV: short_ma_periods, long_ma_periods 중 최댓값
    - Filter: lookback_periods 중 최댓값
    - SR: lookback_periods 중 최댓값
    - CB: lookback_periods 중 최댓값
"""

from typing import Dict, Any, List

from config.indicator_config import INDICATOR_CONFIG


def get_required_warmup_bars(cfg: Dict[str, Any]) -> int:
    """
    보조지표 계산 시 필요한 최대 윈도우(봉 개수)를 찾는다.
    cfg 인자로 MA, RSI, OBV, Filter, SR, CB 등에 대한 파라미터를 전달받는다.

    Args:
        cfg (dict): 예) indicator_config.py의 INDICATOR_CONFIG

    Returns:
        int: 필요한 최대 봉 개수
    """
    candidates: List[int] = []

    # MA: 단기 및 장기 이동평균 기간
    if "MA" in cfg:
        short_list = cfg["MA"].get("short_ma_periods", [])
        long_list = cfg["MA"].get("long_ma_periods", [])
        if short_list or long_list:
            candidates.append(max(short_list + long_list))

    # RSI: lookback 기간
    if "RSI" in cfg:
        lookback_list = cfg["RSI"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # OBV: 단기 및 장기 OBV 이동평균 기간
    if "OBV" in cfg:
        sp_list = cfg["OBV"].get("short_ma_periods", [])
        lp_list = cfg["OBV"].get("long_ma_periods", [])
        if sp_list or lp_list:
            candidates.append(max(sp_list + lp_list))

    # Filter: lookback 기간
    if "Filter" in cfg:
        lookback_list = cfg["Filter"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # SR (Support/Resistance): lookback 기간
    if "SR" in cfg:
        lookback_list = cfg["SR"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

    # CB (Channel Breakout): lookback 기간
    if "CB" in cfg:
        lookback_list = cfg["CB"].get("lookback_periods", [])
        if lookback_list:
            candidates.append(max(lookback_list))

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
