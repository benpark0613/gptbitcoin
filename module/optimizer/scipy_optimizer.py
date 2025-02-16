# module/optimizer/scipy_optimizer.py

import numpy as np
from scipy.optimize import minimize
from module.backtester.backtester_bt import run_backtest_bt


def create_objective_function(df, strategy_cls, start_cash=100000.0, commission=0.002, param_config=None):
    """
    주어진 df와 전략 클래스를 사용하여, 최적화할 파라미터 벡터에 대해 백테스트를 실행하고,
    Sharpe Ratio를 기반으로 objective 값을 반환하는 함수를 생성합니다.

    최적화할 파라미터는 param_config에 따라 dictionary로 변환되며,
    objective 값은 -Sharpe Ratio로 설정되어, 최적화 알고리즘은 이를 최소화합니다.

    param_config: list of tuples, e.g.
        [('bb_period', 'int'), ('bb_dev', 'float'), ('vol_fast', 'int'), ('vol_slow', 'int')]
    """
    if param_config is None:
        param_config = [
            ('bb_period', 'int'),
            ('bb_dev', 'float'),
            ('vol_fast', 'int'),
            ('vol_slow', 'int')
        ]

    def objective(param_vector):
        strategy_params = {}
        for i, (param_name, param_type) in enumerate(param_config):
            # param_vector[i]가 numpy scalar나 배열일 수 있으므로 .item() 사용
            val = param_vector[i]
            if hasattr(val, 'item'):
                val = val.item()
            if param_type == 'int':
                val = int(round(val))
            else:
                val = float(val)
            strategy_params[param_name] = val

        result = run_backtest_bt(
            df,
            strategy_cls=strategy_cls,
            strategy_params=strategy_params,
            start_cash=start_cash,
            commission=commission,
            plot=False
        )
        sharpe = result.sharpe
        if sharpe is None:
            return 1e6
        return -sharpe  # Sharpe Ratio 최대화를 위해 음수 반환

    return objective


def optimize_strategy_parameters(df, strategy_cls, param_config, bounds, initial_guess,
                                 start_cash=100000.0, commission=0.002, method='Nelder-Mead'):
    """
    논문의 Sharpe Ratio 최대화를 위해 SciPy 최적화를 수행.

    파라미터:
      - df : 백테스트할 DataFrame
      - strategy_cls : Backtrader Strategy 클래스 (예: NLS2Combined)
      - param_config : [('bb_period', 'int'), ('bb_dev', 'float'), ...]
      - bounds : 각 파라미터의 (최소, 최대) 튜플 목록
      - initial_guess : 초기 파라미터 추정값 리스트
      - start_cash, commission : 초기 자본, 거래 수수료
      - method : 최적화 알고리즘 (기본 'Nelder-Mead')

    반환:
      - 최적화 결과 객체 (scipy.optimize.OptimizeResult)
    """
    obj_func = create_objective_function(df, strategy_cls, start_cash, commission, param_config)
    result = minimize(obj_func, x0=initial_guess, bounds=bounds, method=method)
    return result
