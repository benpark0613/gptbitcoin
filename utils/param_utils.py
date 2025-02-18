# gptbitcoin/utils/param_utils.py

"""
param_utils.py

전략 파라미터(dict) 관련 유틸 함수 모음.
"""

def filter_params_dict(params: dict) -> dict:
    """
    파라미터 dict에서 'indicator' 키 등
    특정 항목을 제외한 뒤 반환.

    예) {"indicator":"MA","short_period":5,"long_period":20} -> {"short_period":5,"long_period":20}

    Parameters
    ----------
    params : dict
        파라미터(예: combo dict)

    Returns
    -------
    dict
        indicator 키 등을 제외한 새 dict
    """
    return {k: v for k, v in params.items() if k != "indicator"}
