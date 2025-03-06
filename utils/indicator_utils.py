# gptbitcoin/utils/indicator_utils.py
# config의 INDICATOR_CONFIG 값을 참고하여 워밍업 데이터가 몇 개 필요한지 계산하는 모듈
# 주석은 필요한 한글만, docstring은 구글 스타일

def get_required_warmup_bars(cfg: dict) -> int:
    """
    INDICATOR_CONFIG를 입력받아
    보조지표 계산 시 필요한 최대 윈도우(봉 개수)를 찾는다.

    Args:
        cfg (dict): config.config의 INDICATOR_CONFIG

    Returns:
        int: MA, RSI, OBV, Filter, Support/Resistance, Channel_Breakout 등
             모든 지표에서 요구하는 최대 윈도우(봉) 수
    """
    candidates = []

    # MA
    if "MA" in cfg:
        short_list = cfg["MA"].get("short_periods", [])
        long_list = cfg["MA"].get("long_periods", [])
        # band_filters는 'window'와 무관
        if short_list or long_list:
            candidates.append(max(short_list + long_list))

    # RSI
    if "RSI" in cfg:
        lengths = cfg["RSI"].get("lengths", [])
        if lengths:
            candidates.append(max(lengths))

    # OBV
    if "OBV" in cfg:
        sp_list = cfg["OBV"].get("short_periods", [])
        lp_list = cfg["OBV"].get("long_periods", [])
        if sp_list or lp_list:
            candidates.append(max(sp_list + lp_list))

    # Filter
    if "Filter" in cfg:
        windows = cfg["Filter"].get("windows", [])
        if windows:
            candidates.append(max(windows))

    # Support/Resistance
    if "Support_Resistance" in cfg:
        sr_windows = cfg["Support_Resistance"].get("windows", [])
        if sr_windows:
            candidates.append(max(sr_windows))

    # Channel_Breakout
    if "Channel_Breakout" in cfg:
        ch_windows = cfg["Channel_Breakout"].get("windows", [])
        if ch_windows:
            candidates.append(max(ch_windows))

    if not candidates:
        return 0

    return max(candidates)


def main():
    """
    INDICATOR_CONFIG를 가져와서
    워밍업 데이터가 몇 개 필요한지 테스트 출력
    """
    # config 불러오기
    try:
        from config.config import INDICATOR_CONFIG
    except ImportError:
        print("config.py에서 INDICATOR_CONFIG를 가져오지 못했습니다.")
        return

    needed_bars = get_required_warmup_bars(INDICATOR_CONFIG)
    print(f"[indicator_utils] 워밍업에 필요한 최소 봉 수: {needed_bars}")

if __name__ == "__main__":
    main()
