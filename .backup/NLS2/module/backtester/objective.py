# module/backtester/objective.py

from backup.NLS2.module.backtester.backtester_bt import run_backtest_bt

def create_objective_function(
    df,
    strategy_cls,
    start_cash=100000.0,
    commission=0.002,
    param_config=None
):
    """
    SciPy 최적화에 사용할 objective 함수를 생성.
    - param_config: [(param_name, 'int' or 'float'), ...]
      예: [
        ('bb_period', 'int'),
        ('bb_dev', 'float'),
        ('vol_fast', 'int'),
        ...
      ]
      로, param_vector를 어떤 식으로 파라미터 딕셔너리에 매핑할지 알려줌.
    """

    if param_config is None:
        # 예시: bb_period(int), bb_dev(float)만 최적화
        param_config = [
            ('bb_period', 'int'),
            ('bb_dev', 'float'),
        ]

    def objective(param_vector):
        """
        SciPy에서 minimize(...)가 호출하는 함수.
        param_vector는 [value0, value1, ...] 형태.
        이를 param_config에 따라 dict로 매핑한 뒤, 백테스트를 실행하여 Sharpe를 계산.
        Sharpe를 최대화하려면 -Sharpe를 반환(최적화 함수는 최소화 문제이기 때문).
        """
        # 1) param_vector -> strategy_params 생성
        #    param_config 순서대로 변환
        strategy_params = {}
        for (i, (pname, ptype)) in enumerate(param_config):
            val = param_vector[i]
            if ptype == 'int':
                val = int(round(val))
            strategy_params[pname] = val

        # 2) 백테스트 실행
        result = run_backtest_bt(
            df=df,
            strategy_cls=strategy_cls,
            strategy_params=strategy_params,
            start_cash=start_cash,
            commission=commission,
            plot=False
        )

        # 3) SharpeRatio => 음수화
        sharpe = result.sharpe

        # Backtrader Sharpe가 None일 수도 있으므로 방어적으로 처리
        if sharpe is None:
            # Sharpe 계산 불가 시 벌점(큰 양수 반환)
            return 1e6
        return -sharpe

    return objective
