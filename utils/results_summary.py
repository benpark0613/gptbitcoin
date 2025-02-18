# gptbitcoin/utils/results_summary.py

"""
results_summary.py

In-Sample(IS)과 Out-of-Sample(OOS) 결과를 요약하여 CSV 저장용 summary_rows(list of dict)에
추가하는 유틸 함수들.

 - append_buy_and_hold_result: Buy & Hold 결과(기존 지표+시작자금/끝자금)
 - append_combo_results       : 콤보별 IS/OOS 결과(기존 지표+시작자금/끝자금)
"""

from typing import List, Dict
from utils.param_utils import filter_params_dict


def append_buy_and_hold_result(
    summary_rows: List[dict],
    interval: str,
    metrics_bh_is: dict,
    metrics_bh_oos: dict
):
    """
    Buy & Hold 결과를 summary_rows에 한 행(row) 추가.
    컬럼:
      - timeframe: e.g. "4h(b/h)"
      - IS_return, IS_sharpe, IS_start_cap, IS_end_cap
      - OOS_return, OOS_sharpe, OOS_start_cap, OOS_end_cap
      - OOS_max_dd, OOS_calmar, OOS_candle_win_ratio, OOS_trade_count, ...
      - excluded_in_is=False, params="N/A"
    """
    # IS
    is_ret    = metrics_bh_is.get("return_pct", 0.0)
    is_sharp  = metrics_bh_is.get("sharpe", 0.0)
    is_sc     = metrics_bh_is.get("start_cap", None)
    is_ec     = metrics_bh_is.get("end_cap", None)

    # OOS
    oos_ret   = metrics_bh_oos.get("return_pct", 0.0)
    oos_sharp = metrics_bh_oos.get("sharpe", 0.0)
    oos_sc    = metrics_bh_oos.get("start_cap", None)
    oos_ec    = metrics_bh_oos.get("end_cap", None)

    # 기타 기존 성과지표들
    oos_mdd      = metrics_bh_oos.get("max_drawdown", None)
    oos_calmar   = metrics_bh_oos.get("calmar", None)
    oos_cwr      = metrics_bh_oos.get("candle_win_ratio", None)
    oos_tcount   = metrics_bh_oos.get("trade_count", None)
    oos_twrate   = metrics_bh_oos.get("trade_win_rate", None)
    oos_avgwin   = metrics_bh_oos.get("avg_win", None)
    oos_avglos   = metrics_bh_oos.get("avg_loss", None)
    oos_rr       = metrics_bh_oos.get("reward_risk", None)
    oos_mcwin    = metrics_bh_oos.get("max_consec_win", None)
    oos_mclos    = metrics_bh_oos.get("max_consec_loss", None)

    row_dict = {
        "timeframe"     : f"{interval}(b/h)",
        "indicator"     : "Buy&Hold",

        # IS
        "IS_return"     : is_ret,
        "IS_sharpe"     : is_sharp,
        "IS_start_cap"  : is_sc,    # 신규추가
        "IS_end_cap"    : is_ec,    # 신규추가

        # OOS
        "OOS_return"    : oos_ret,
        "OOS_sharpe"    : oos_sharp,
        "OOS_start_cap" : oos_sc,   # 신규추가
        "OOS_end_cap"   : oos_ec,   # 신규추가

        # 기존 OOS 지표들
        "OOS_max_dd"         : oos_mdd,
        "OOS_calmar"         : oos_calmar,
        "OOS_candle_win_ratio": oos_cwr,
        "OOS_trade_count"    : oos_tcount,
        "OOS_trade_win_rate" : oos_twrate,
        "OOS_avg_win"        : oos_avgwin,
        "OOS_avg_loss"       : oos_avglos,
        "OOS_reward_risk"    : oos_rr,
        "OOS_max_consec_win" : oos_mcwin,
        "OOS_max_consec_loss": oos_mclos,

        "excluded_in_is": False,  # B/H는 필터링X
        "params"        : "N/A"
    }

    summary_rows.append(row_dict)


def append_combo_results(
    summary_rows: List[dict],
    interval: str,
    is_results: Dict[str, dict],
    oos_results: Dict[str, dict],
    excluded_in_is: List[str]
):
    """
    콤보별 IS/OOS 결과를 summary_rows에 추가.
    - IS metrics: {return_pct, sharpe, start_cap, end_cap,...}
    - OOS metrics: {return_pct, sharpe, start_cap, end_cap,..., max_drawdown, calmar,...}
    - 기존 지표 + 신규 start_cap, end_cap 모두 보여준다.
    """

    for combo_key, val in is_results.items():
        combo_params = val["params"]
        m_is = val["metrics"]
        is_ret  = m_is.get("return_pct", 0.0)
        is_shp  = m_is.get("sharpe", 0.0)
        is_sc   = m_is.get("start_cap", None)  # 시작자금
        is_ec   = m_is.get("end_cap", None)    # 끝자금

        is_excluded = (combo_key in excluded_in_is)

        # OOS
        if combo_key in oos_results and not is_excluded:
            oos_val = oos_results[combo_key]
            m_oos   = oos_val["metrics"]

            oos_ret   = m_oos.get("return_pct", None)
            oos_sharp = m_oos.get("sharpe", None)
            oos_sc    = m_oos.get("start_cap", None)   # OOS 시작자금
            oos_ec    = m_oos.get("end_cap", None)     # OOS 끝자금

            oos_mdd   = m_oos.get("max_drawdown", None)
            oos_calmar= m_oos.get("calmar", None)
            oos_cwr   = m_oos.get("candle_win_ratio", None)
            oos_tcount= m_oos.get("trade_count", None)
            oos_twrate= m_oos.get("trade_win_rate", None)
            oos_avgwin= m_oos.get("avg_win", None)
            oos_avglos= m_oos.get("avg_loss", None)
            oos_rr    = m_oos.get("reward_risk", None)
            oos_mcwin = m_oos.get("max_consec_win", None)
            oos_mclos = m_oos.get("max_consec_loss", None)
        else:
            oos_ret   = None
            oos_sharp = None
            oos_sc    = None
            oos_ec    = None
            oos_mdd   = None
            oos_calmar= None
            oos_cwr   = None
            oos_tcount= None
            oos_twrate= None
            oos_avgwin= None
            oos_avglos= None
            oos_rr    = None
            oos_mcwin = None
            oos_mclos = None

        row_dict = {
            "timeframe" : interval,
            "indicator" : str(combo_params.get("indicator","Unknown")),

            # IS
            "IS_return"     : is_ret,
            "IS_sharpe"     : is_shp,
            "IS_start_cap"  : is_sc,  # 신규
            "IS_end_cap"    : is_ec,  # 신규

            # OOS
            "OOS_return"    : oos_ret,
            "OOS_sharpe"    : oos_sharp,
            "OOS_start_cap" : oos_sc,  # 신규
            "OOS_end_cap"   : oos_ec,  # 신규

            "OOS_max_dd"         : oos_mdd,
            "OOS_calmar"         : oos_calmar,
            "OOS_candle_win_ratio": oos_cwr,
            "OOS_trade_count"    : oos_tcount,
            "OOS_trade_win_rate" : oos_twrate,
            "OOS_avg_win"        : oos_avgwin,
            "OOS_avg_loss"       : oos_avglos,
            "OOS_reward_risk"    : oos_rr,
            "OOS_max_consec_win" : oos_mcwin,
            "OOS_max_consec_loss": oos_mclos,

            "excluded_in_is": is_excluded
        }

        # 맨 뒤 params
        filtered = filter_params_dict(combo_params)
        row_dict["params"] = str(filtered)

        summary_rows.append(row_dict)
