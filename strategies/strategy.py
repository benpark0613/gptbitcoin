# strategies/strategy.py

import pandas as pd
import numpy as np
from strategies.signal_generator import SignalGenerator

class Strategy:
    """
    논문에서 제시된 방식대로,
    - 다양한 인디케이터/파라미터(config)를 기반으로 시그널 생성
    - time_delay, holding_period, shorting_allowed 등을 적용
    - 거래 비용(transaction_fee_rate)도 반영
    의 로직을 구현한 클래스.
    """

    def __init__(self, config, initial_capital=100000):
        """
        :param config: dict 형태. 인디케이터 및 전략 파라미터 포함
                       예) {
                         "MA": {"short_period": 12, "long_period": 26},
                         "RSI": {...},
                         "Filter": {...},
                         ...
                         "time_delay": 2,
                         "holding_period": 6,
                         "shorting_allowed": True,
                         "transaction_fee_rate": 0.0004
                       }
        :param initial_capital: 초기자금
        """
        self.config = config
        self.initial_capital = initial_capital

        # 전략 관련 파라미터
        self.shorting_allowed = self.config.get("shorting_allowed", False)
        self.time_delay = self.config.get("time_delay", 0)
        self.holding_period = self.config.get("holding_period", None)
        if isinstance(self.holding_period, str) and self.holding_period.lower() == "infinity":
            self.holding_period = 999999999

        self.transaction_fee_rate = self.config.get("transaction_fee_rate", 0.0)

        # 트레이드(체결) 정보 기록
        self.trades = []

    def _apply_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        signal_generator.py의 SignalGenerator를 사용해
        config에 명시된 단일 인디케이터에 대한 시그널을 생성.
        """
        sg = SignalGenerator(self.config)
        df_signals = sg.generate_signal(df)  # <-- 변경: generate_combined_signal -> generate_signal

        # 이제 'signal' 칼럼이 최종 매매 시그널
        # 논문대로라면 'signal' = +1/-1/0
        df_signals["raw_signal"] = df_signals["signal"]
        return df_signals

    def _calculate_raw_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        종가 기준 단순 수익률(pct_change)을 구하고,
        시그널×수익률(진입중인 경우 ± 수익)을 'raw_strategy_return' 칼럼에 계산
        """
        df["close_return"] = df["close"].pct_change().fillna(0)
        df["raw_signal"] = df["raw_signal"].fillna(0)
        df["raw_strategy_return"] = df["raw_signal"].shift(1) * df["close_return"]
        return df

    def simulate(self, df: pd.DataFrame, requested_start=None):
        """
        1) config 기반 인디케이터·파라미터로 시그널 생성
        2) 매 시점별로 time_delay, holding_period, shorting_allowed, 거래비용 등을
           고려한 포지션 진입·청산 로직 수행
        3) 포트폴리오 가치 series & 최종 평가액 산출
        """
        df_signal = self._apply_indicators(df.copy())
        df_signal = self._calculate_raw_returns(df_signal)
        df_sim, final_val = self._simulate_portfolio(df_signal)

        if requested_start is not None:
            df_sim = df_sim[df_sim.index >= requested_start]
        return df_sim, final_val

    def _simulate_portfolio(self, df: pd.DataFrame):
        """
        time_delay, holding_period, shorting_allowed,
        transaction_fee_rate를 모두 반영한 포트폴리오 시뮬레이션
        """
        portfolio_values = []
        current_value = self.initial_capital

        current_position = 0  # +1=롱, -1=숏, 0=현금
        entry_price = None
        entry_time = None
        days_in_position = 0

        # time_delay 적용을 위한 임시 변수
        pending_signal = 0
        pending_since = None

        df["strategy_return"] = 0.0
        idx_list = df.index

        # 첫 번째 bar 현재가치
        portfolio_values.append(current_value)

        for i in range(1, len(idx_list)):
            idx = idx_list[i]
            row = df.loc[idx]
            old_value = portfolio_values[-1]
            new_value = old_value

            # 포지션 보유 중이면, 보유 일수 1 증가
            if current_position != 0:
                days_in_position += 1

            # 오늘 시그널 (결합 시그널)
            todays_signal = row["raw_signal"]

            # (1) 시그널 감지
            if todays_signal != 0:
                # time_delay가 있으므로 바로 진입하지 않고 pending 상태
                pending_signal = todays_signal
                pending_since = idx
            else:
                # 시그널이 0이면, 보유 포지션이 있으면 청산
                if current_position != 0:
                    new_value = self._close_position(
                        new_capital=new_value,
                        exit_time=idx,
                        exit_price=row["close"],
                        side=current_position,
                        entry_price=entry_price,
                        entry_time=entry_time
                    )
                    current_position = 0
                    entry_price = None
                    entry_time = None
                    days_in_position = 0

                pending_signal = 0
                pending_since = None

            # (2) time_delay 로직 처리
            if pending_signal != 0 and pending_since is not None:
                delta_days = (idx - pending_since).days
                if delta_days >= self.time_delay:
                    # 기존 포지션이 있다면 우선 청산
                    if current_position != 0:
                        new_value = self._close_position(
                            new_capital=new_value,
                            exit_time=idx,
                            exit_price=row["close"],
                            side=current_position,
                            entry_price=entry_price,
                            entry_time=entry_time
                        )
                        current_position = 0
                        entry_price = None
                        entry_time = None
                        days_in_position = 0

                    # 신규 진입
                    if pending_signal == 1:
                        # 롱 포지션
                        fee = new_value * self.transaction_fee_rate
                        new_value -= fee
                        current_position = 1
                        entry_price = row["close"]
                        entry_time = idx
                        days_in_position = 1

                    elif pending_signal == -1:
                        # 숏 포지션(허용 여부 확인)
                        if self.shorting_allowed:
                            fee = new_value * self.transaction_fee_rate
                            new_value -= fee
                            current_position = -1
                            entry_price = row["close"]
                            entry_time = idx
                            days_in_position = 1
                        else:
                            # shorting 미허용이면 신호 무시
                            current_position = 0
                            entry_price = None
                            entry_time = None
                            days_in_position = 0

                    # pending 소멸
                    pending_signal = 0
                    pending_since = None

            # (3) holding_period 체크
            if current_position != 0 and self.holding_period is not None:
                if days_in_position > self.holding_period:
                    new_value = self._close_position(
                        new_capital=new_value,
                        exit_time=idx,
                        exit_price=row["close"],
                        side=current_position,
                        entry_price=entry_price,
                        entry_time=entry_time
                    )
                    current_position = 0
                    entry_price = None
                    entry_time = None
                    days_in_position = 0

            # (4) 실제 보유 포지션에 따른 수익률 반영
            actual_ret = 0.0
            if current_position == 1:
                # 롱이면 다음 시점 close_return 그대로 적용
                actual_ret = row["close_return"]
            elif current_position == -1 and self.shorting_allowed:
                # 숏이면 음의 수익률 반영
                actual_ret = -row["close_return"]

            new_value *= (1 + actual_ret)

            # 혹시나 파산(<=0) 발생 시
            if new_value <= 0:
                new_value = 0
                remain = len(idx_list) - i - 1
                portfolio_values.append(new_value)
                portfolio_values.extend([0] * remain)
                df.loc[idx_list[i:], "strategy_return"] = 0
                break

            portfolio_values.append(new_value)
            df.at[idx, "strategy_return"] = actual_ret

        df["portfolio_value"] = portfolio_values
        final_val = portfolio_values[-1]
        return df, final_val

    def _close_position(self, new_capital, exit_time, exit_price, side, entry_price, entry_time):
        """
        포지션 청산 시 거래 비용, PnL, 보유 기간 등을 계산하고
        트레이드 리스트에 기록하는 메서드
        """
        # 청산 수수료
        fee_close = new_capital * self.transaction_fee_rate
        new_cap = new_capital - fee_close

        # PnL 계산
        if side == 1:
            # 롱
            pnl_ratio = (exit_price - entry_price) / entry_price
        else:
            # 숏
            pnl_ratio = (entry_price - exit_price) / entry_price

        pnl = new_cap * pnl_ratio
        new_cap += pnl

        # 보유 일수
        holding_days = 0
        if entry_time is not None:
            holding_days = (exit_time - entry_time).days

        # 트레이드 정보 기록
        self.trades.append({
            "side": "LONG" if side == 1 else "SHORT",
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl": pnl,
            "holding_period": holding_days
        })

        return new_cap
