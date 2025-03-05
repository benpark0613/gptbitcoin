# backtester_bt.py

import backtrader as bt
import pandas as pd
from backup.NLS2.module.backtester.bt_result import BacktestResult

# 기존 논문 전략
from backup.NLS2.module.strategies.nls2_combined import NLS2Combined
from backup.NLS2.module.strategies.nls2_combined_progress import NLS2CombinedProgress
# 새로 추가된 Buy & Hold 전략

def run_backtest_bt(
    df: pd.DataFrame,
    strategy_cls=None,         # 사용자가 원하는 전략 클래스를 지정할 수 있음 (None이면 기존 전략 사용)
    strategy_params=None,
    start_cash: float = 10000.0,
    commission: float = 0.002,
    plot: bool = False,
    use_progress: bool = False
) -> BacktestResult:
    """
    범용 백테스트 실행 함수.
    - strategy_cls가 None이면 기존 논문 전략(NLS2Combined 또는 NLS2CombinedProgress)을 사용하고,
      그렇지 않으면 지정된 전략 클래스를 사용 (예: BuyAndHoldStrategy).
    - Sharpe, DrawDown, TradeAnalyzer, Returns, SQN Analyzer 등을 등록하여 성과지표를 산출함.
    """

    if strategy_params is None:
        strategy_params = {}

    # 데이터 피드 생성
    data_feed = bt.feeds.PandasData(
        dataname=df,
        timeframe=bt.TimeFrame.Minutes,
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=None
    )

    cerebro = bt.Cerebro()
    cerebro.adddata(data_feed)
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=commission)

    # 전략 클래스 결정
    if strategy_cls is None:
        if use_progress:
            StrategyCls = NLS2CombinedProgress
        else:
            StrategyCls = NLS2Combined
    else:
        StrategyCls = strategy_cls

    cerebro.addstrategy(StrategyCls, **strategy_params)

    # Analyzer 등록
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days, annualize=True)
    cerebro.addanalyzer(bt.analyzers.DrawDown,   _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns', tann=365, timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

    # 백테스트 실행
    results = cerebro.run()
    strat = results[0]

    final_value = cerebro.broker.getvalue()
    net_profit = final_value - start_cash

    # Sharpe Ratio 추출
    sharpe_res = strat.analyzers.sharpe.get_analysis()
    sharpe_ratio = sharpe_res.get('sharperatio', None)

    # DrawDown 추출
    dd_res = strat.analyzers.drawdown.get_analysis()
    max_dd_pct = dd_res['max']['drawdown']

    # TradeAnalyzer 결과 추출
    ta = strat.analyzers.trades.get_analysis()

    total_trades = ta.total.get('total', 0) if 'total' in ta else 0
    won_trades   = ta.won.get('total', 0) if 'won' in ta else 0
    lost_trades  = ta.lost.get('total', 0) if 'lost' in ta else 0
    strike_rate  = (won_trades / total_trades * 100) if total_trades else 0.0

    pnl_net   = ta.pnl.net.total   if ('pnl' in ta and 'net' in ta.pnl) else None
    pnl_gross = ta.pnl.gross.total if ('pnl' in ta and 'gross' in ta.pnl) else None

    # 안전하게 이익/손실 추출
    gross_profit = 0
    gross_loss   = 0
    if 'pnl' in ta and 'gross' in ta.pnl:
        if 'won' in ta.pnl.gross:
            gross_profit = ta.pnl.gross.won
        if 'lost' in ta.pnl.gross:
            gross_loss = ta.pnl.gross.lost

    if gross_loss != 0:
        profit_factor = abs(gross_profit / gross_loss)
    else:
        profit_factor = None

    # 평균 승/패 금액
    avg_win  = ta.won.pnl.average if ('won' in ta and 'pnl' in ta.won and 'average' in ta.won.pnl) else None
    avg_loss = ta.lost.pnl.average if ('lost' in ta and 'pnl' in ta.lost and 'average' in ta.lost.pnl) else None

    # 최대 연속 승/패
    win_streak  = ta.streak.won.longest if ('streak' in ta and 'won' in ta.streak and 'longest' in ta.streak.won) else None
    lose_streak = ta.streak.lost.longest if ('streak' in ta and 'lost' in ta.streak and 'longest' in ta.streak.lost) else None

    # Returns Analyzer: 연환산 수익률(%)
    ret_res = strat.analyzers.returns.get_analysis()
    annual_return_pct = ret_res.get('rnorm100', None)

    # SQN Analyzer
    sqn_res = strat.analyzers.sqn.get_analysis()
    sqn_value = sqn_res.get('sqn', None)

    if plot:
        cerebro.plot()

    return BacktestResult(
        sharpe=sharpe_ratio,
        max_drawdown_pct=max_dd_pct,
        final_value=final_value,
        net_profit=net_profit,
        total_trades=total_trades,
        won_trades=won_trades,
        lost_trades=lost_trades,
        strike_rate=strike_rate,
        pnl_net=pnl_net,
        pnl_gross=pnl_gross,
        annual_return_pct=annual_return_pct,
        sqn=sqn_value,
        profit_factor=profit_factor,
        avg_win=avg_win,
        avg_loss=avg_loss,
        win_streak=win_streak,
        lose_streak=lose_streak
    )
