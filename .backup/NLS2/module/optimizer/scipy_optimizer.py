from scipy.optimize import minimize
from tqdm import tqdm
from backup.NLS2.module.backtester.backtester_bt import run_backtest_bt


def create_objective_function(df, strategy_cls, start_cash=100000.0, commission=0.002, param_config=None):
    """
    주어진 df와 전략 클래스를 사용하여, 최적화할 파라미터 벡터에 대해 백테스트를 실행하고,
    Sharpe Ratio를 기반으로 objective 값을 반환하는 함수를 생성합니다.

    param_config: list of tuples, 예:
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
            return 1e6  # Sharpe 계산 불가 시 벌점
        return -sharpe  # Sharpe Ratio 최대화를 위해 음수 반환

    return objective


def optimize_strategy_parameters(df, strategy_cls, param_config, bounds, initial_guess,
                                 start_cash=100000.0, commission=0.002, method='Nelder-Mead', max_iter=100):
    """
    논문의 Sharpe Ratio 최대화를 위해 SciPy 최적화를 수행합니다.

    파라미터:
      - df : 백테스트할 DataFrame
      - strategy_cls : Backtrader Strategy 클래스 (예: NLS2Combined)
      - param_config : [('bb_period', 'int'), ('bb_dev', 'float'), ...]
      - bounds : 각 파라미터의 (최소, 최대) 튜플 목록
      - initial_guess : 초기 파라미터 추정값 리스트
      - start_cash, commission : 초기 자본, 거래 수수료
      - method : 최적화 알고리즘 (기본 'Nelder-Mead')
      - max_iter : 최대 반복 횟수 (진행바 표시를 위해 사용)

    반환:
      - 최적화 결과 객체 (scipy.optimize.OptimizeResult)
    """
    obj_func = create_objective_function(df, strategy_cls, start_cash, commission, param_config)

    # 단일 진행바 생성 (전체 최적화 과정 최대 max_iter 기준)
    pbar = tqdm(total=max_iter, desc="Optimization Progress", bar_format='{l_bar}{bar} {n_fmt}/{total_fmt}')
    iteration_count = 0  # 실제 최적화 반복 횟수를 추적할 변수

    def callback(xk):
        nonlocal iteration_count
        iteration_count += 1
        pbar.update(1)

    options = {'maxiter': max_iter}
    result = minimize(obj_func, x0=initial_guess, bounds=bounds, method=method, callback=callback, options=options)

    # 최적화가 수렴하여 실제 반복 횟수가 max_iter보다 적다면, 진행바를 100%로 업데이트
    if iteration_count < max_iter:
        pbar.update(max_iter - iteration_count)
    pbar.close()
    return result
