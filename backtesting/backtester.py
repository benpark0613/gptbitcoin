# backtesting/backtester.py

import pandas as pd
import numpy as np
from strategies.strategy import Strategy

class Backtester:
    def __init__(self, strategy: Strategy, data: pd.DataFrame, initial_capital=100000):
        self.strategy = strategy
        self.data = data
        self.initial_capital = initial_capital
        self.result_df = None
        self.final_portfolio_value = None

    def run_backtest(self):
        self.result_df, self.final_portfolio_value = self.strategy.simulate(self.data)
        return self.result_df, self.final_portfolio_value

    def calculate_metrics(self):
        """
        10가지 지표:
          1) final_portfolio_value
          2) Total Return
          3) CAGR
          4) Max Drawdown
          5) Sharpe Ratio
          6) Profit Factor
          7) Payoff Ratio
          8) Win Rate
          9) Number of Trades
          10) Average Holding Time
        """
        if self.result_df is None:
            raise ValueError("Backtest has not been run yet.")

        # 1) final_portfolio_value
        final_port_val = self.final_portfolio_value

        # 2) Total Return
        total_return = final_port_val/self.initial_capital -1

        # 3) CAGR
        df = self.result_df.copy()
        if len(df)==0:
            return {}
        start_date= df.index[0]
        end_date  = df.index[-1]
        days= (end_date - start_date).days
        years= days/365 if days>0 else 0
        if years>0:
            cagr= (final_port_val/self.initial_capital)**(1/years)-1
        else:
            cagr=0

        # 4) Max Drawdown
        running_max= df["portfolio_value"].cummax()
        drawdown= (df["portfolio_value"]-running_max)/running_max
        max_dd= drawdown.min()

        # 5) Sharpe Ratio
        df["strategy_return"]= df["strategy_return"].fillna(0)
        mean_ret= df["strategy_return"].mean()
        std_ret= df["strategy_return"].std()
        sharpe=0
        # 단순연간화 => 365
        if std_ret!=0:
            sharpe= (mean_ret/std_ret)*(365**0.5)

        # --- 트레이드 로그 참고 ---
        trades = self.strategy.trades  # list of dict
        n_trades= len(trades)

        # 6) Profit Factor => (이익합)/(손실합)
        #    이익 or 손실= 'pnl' 양수/음수
        profit_factor=0
        if n_trades>0:
            sum_win= 0
            sum_loss=0
            for t in trades:
                if t["pnl"]>0:
                    sum_win+= t["pnl"]
                else:
                    sum_loss+= abs(t["pnl"])
            if sum_loss>0:
                profit_factor= sum_win / sum_loss
            else:
                # 손실 전혀 없으면 profit_factor 무한대 => 임의로 큰값
                profit_factor= 999999999

        # 7) Payoff Ratio => 평균 이익 / 평균 손실
        payoff_ratio=0
        if n_trades>0:
            wins= [t["pnl"] for t in trades if t["pnl"]>0]
            losses=[abs(t["pnl"]) for t in trades if t["pnl"]<0]
            avg_win= np.mean(wins) if len(wins)>0 else 0
            avg_loss= np.mean(losses) if len(losses)>0 else 0
            if avg_loss>0:
                payoff_ratio= avg_win/avg_loss
            else:
                payoff_ratio= 999999999 if avg_win>0 else 0

        # 8) Win Rate => 승률
        win_rate=0
        if n_trades>0:
            win_count= sum([1 for t in trades if t["pnl"]>0])
            win_rate= win_count/n_trades

        # 9) Number of Trades => n_trades
        num_trades= n_trades

        # 10) Average Holding Time => trades에 holding_period(days) 기록
        avg_holding= 0
        if n_trades>0:
            hold_periods= [t["holding_period"] for t in trades]
            avg_holding= np.mean(hold_periods)

        metrics={
            "final_portfolio_value": final_port_val,
            "Total Return": total_return,
            "CAGR": cagr,
            "Max Drawdown": max_dd,
            "Sharpe Ratio": sharpe,
            "Profit Factor": profit_factor,
            "Payoff Ratio": payoff_ratio,
            "Win Rate": win_rate,
            "Number of Trades": num_trades,
            "Average Holding Time": avg_holding
        }
        return metrics
