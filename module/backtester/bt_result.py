# bt_result.py

class BacktestResult:
    """
    백테스트 결과 지표를 보관하는 클래스
    """
    def __init__(
        self,
        sharpe,
        max_drawdown_pct,
        final_value,
        net_profit,
        total_trades,
        won_trades,
        lost_trades,
        strike_rate,
        pnl_net,
        pnl_gross,

        # 새로 추가된 지표들
        annual_return_pct,
        sqn,
        profit_factor,
        avg_win,
        avg_loss,
        win_streak,
        lose_streak
    ):
        self.sharpe = sharpe
        self.max_drawdown_pct = max_drawdown_pct
        self.final_value = final_value
        self.net_profit = net_profit
        self.total_trades = total_trades
        self.won_trades = won_trades
        self.lost_trades = lost_trades
        self.strike_rate = strike_rate
        self.pnl_net = pnl_net
        self.pnl_gross = pnl_gross

        self.annual_return_pct = annual_return_pct
        self.sqn = sqn
        self.profit_factor = profit_factor
        self.avg_win = avg_win
        self.avg_loss = avg_loss
        self.win_streak = win_streak
        self.lose_streak = lose_streak

    def to_dict(self):
        return {
            "sharpe": self.sharpe,
            "max_drawdown_pct": self.max_drawdown_pct,
            "final_value": self.final_value,
            "net_profit": self.net_profit,
            "total_trades": self.total_trades,
            "won_trades": self.won_trades,
            "lost_trades": self.lost_trades,
            "strike_rate": self.strike_rate,
            "pnl_net": self.pnl_net,
            "pnl_gross": self.pnl_gross,

            "annual_return_pct": self.annual_return_pct,
            "sqn": self.sqn,
            "profit_factor": self.profit_factor,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "win_streak": self.win_streak,
            "lose_streak": self.lose_streak
        }
