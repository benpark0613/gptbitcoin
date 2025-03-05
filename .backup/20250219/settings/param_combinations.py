# gptbitcoin/settings/param_combinations.py

"""
param_combinations.py

config.py의 INDICATOR_PARAMS 설정을 기반으로,
MA, RSI, Support/Resistance, Filter Rule, Channel Breakout, OBV 등
모든 지표의 파라미터 조합을 전수 생성해주는 모듈.

직접 실행(python param_combinations.py)하면
테스트용 디버그 함수(debug_combos)에서
조합 개수와 일부 샘플을 출력.
"""

from . import config

def generate_all_combinations():
    combos = []

    for indicator_name, param_dict in config.INDICATOR_PARAMS.items():
        # ----- MA -----
        if indicator_name == "MA":
            for sp in param_dict["short_periods"]:
                for lp in param_dict["long_periods"]:
                    if lp <= sp:
                        continue
                    for bf in param_dict["band_filter"]:
                        for dfilt in param_dict["delay_filter"]:
                            for hp in param_dict["holding_period"]:
                                combos.append({
                                    "indicator": "MA",
                                    "short_period": sp,
                                    "long_period": lp,
                                    "band_filter": bf,
                                    "delay_filter": dfilt,
                                    "holding_period": hp
                                })

        # ----- RSI -----
        elif indicator_name == "RSI":
            for length in param_dict["lengths"]:
                for ob in param_dict["overbought_values"]:
                    for osval in param_dict["oversold_values"]:
                        for bf in param_dict["band_filter"]:
                            for dfilt in param_dict["delay_filter"]:
                                for hp in param_dict["holding_period"]:
                                    combos.append({
                                        "indicator": "RSI",
                                        "length": length,
                                        "overbought": ob,
                                        "oversold": osval,
                                        "band_filter": bf,
                                        "delay_filter": dfilt,
                                        "holding_period": hp
                                    })

        # ----- Support & Resistance -----
        elif indicator_name == "Support_Resistance":
            for w in param_dict["windows"]:
                for bf in param_dict["band_filter"]:
                    for dfilt in param_dict["delay_filter"]:
                        for hp in param_dict["holding_period"]:
                            combos.append({
                                "indicator": "S&R",
                                "window": w,
                                "band_filter": bf,
                                "delay_filter": dfilt,
                                "holding_period": hp
                            })

        # ----- Filter Rule -----
        elif indicator_name == "Filter_Rule":
            for w in param_dict["windows"]:
                for xv in param_dict["x_values"]:
                    for yv in param_dict["y_values"]:
                        for bf in param_dict["band_filter"]:
                            for dfilt in param_dict["delay_filter"]:
                                for hp in param_dict["holding_period"]:
                                    combos.append({
                                        "indicator": "Filter",
                                        "window": w,
                                        "x": xv,
                                        "y": yv,
                                        "band_filter": bf,
                                        "delay_filter": dfilt,
                                        "holding_period": hp
                                    })

        # ----- Channel Breakout -----
        elif indicator_name == "Channel_Breakout":
            for w in param_dict["windows"]:
                for cv in param_dict["c_values"]:
                    for bf in param_dict["band_filter"]:
                        for dfilt in param_dict["delay_filter"]:
                            for hp in param_dict["holding_period"]:
                                combos.append({
                                    "indicator": "CB",
                                    "window": w,
                                    "c_value": cv,
                                    "band_filter": bf,
                                    "delay_filter": dfilt,
                                    "holding_period": hp
                                })

        # ----- OBV -----
        elif indicator_name == "OBV":
            for sp in param_dict["short_periods"]:
                for lp in param_dict["long_periods"]:
                    if lp <= sp:
                        continue
                    for bf in param_dict["band_filter"]:
                        for dfilt in param_dict["delay_filter"]:
                            for hp in param_dict["holding_period"]:
                                combos.append({
                                    "indicator": "OBV",
                                    "short_period": sp,
                                    "long_period": lp,
                                    "band_filter": bf,
                                    "delay_filter": dfilt,
                                    "holding_period": hp
                                })
    return combos


def debug_combos():
    """
    디버그: 실제로 generate_all_combinations()로
    몇 개의 파라미터 조합이 만들어지는지 콘솔에 표시.
    """
    all_combos = generate_all_combinations()
    total_count = len(all_combos)
    print(f"Total param combos generated: {total_count}")
    if total_count > 0:
        print("Sample combo:", all_combos[0])


if __name__ == "__main__":
    # 모듈을 직접 실행했을 때(테스트 용)
    debug_combos()
