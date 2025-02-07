# module/mbinance/closed_positions.py

import os
import math
import csv
from datetime import datetime
from dotenv import load_dotenv
from binance.client import Client

def get_default_client():
    """
    ENV에서 BINANCE_ACCESS_KEY, BINANCE_SECRET_KEY를 로드해
    바이낸스 Futures Client를 생성 후 반환합니다.
    """
    load_dotenv()
    access_key = os.getenv("BINANCE_ACCESS_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY")
    return Client(access_key, secret_key)

def get_all_futures_trades(client, symbol):
    """
    바이낸스 선물 체결 이력(Trades)을 시간 순으로 전부 가져오는 함수.
    - GET /fapi/v1/userTrades
    """
    all_trades = []
    last_id = 0
    while True:
        trades = client.futures_account_trades(symbol=symbol, fromId=last_id, limit=1000)
        if not trades:
            break
        all_trades.extend(trades)
        last_id = trades[-1]['id'] + 1
        if len(trades) < 1000:
            break
    all_trades.sort(key=lambda x: x['time'])
    return all_trades

def get_all_futures_income(client, symbol):
    """
    바이낸스 선물 인컴(Incomes) 데이터(주로 COMMISSION, FUNDING_FEE)를 시간 순으로 조회
    - GET /fapi/v1/income
    """
    all_income = []
    start_time = 0
    while True:
        inc_list = client.futures_income_history(symbol=symbol, startTime=start_time, limit=1000)
        if not inc_list:
            break
        all_income.extend(inc_list)
        last_time = inc_list[-1]['time']
        start_time = last_time + 1
        if len(inc_list) < 1000:
            break

    all_income.sort(key=lambda x: x['time'])
    return all_income

def reconstruct_closed_positions(client, symbol):
    """
    Trades + Incomes를 종합하여 포지션을 로컬 재구성:
      - 부분 진입, 부분 청산, 반전 처리
      - 0이 되는 순간(완전 청산)에 1건 기록
      - 수수료, 펀딩 반영
    최종적으로 '닫힌 포지션'의 리스트 반환
    """
    trades = get_all_futures_trades(client, symbol)
    incomes = get_all_futures_income(client, symbol)

    commission_list = []
    funding_list = []
    for inc in incomes:
        inc_type = inc['incomeType']
        inc_time = inc['time']
        inc_amt = float(inc['income'])

        if inc_type == 'COMMISSION':
            commission_list.append({
                'time': inc_time,
                'amount': inc_amt,
                'symbol': inc['symbol'],
                'tradeId': inc.get('tradeId', None),
            })
        elif inc_type == 'FUNDING_FEE':
            funding_list.append({
                'time': inc_time,
                'amount': inc_amt,
                'symbol': inc['symbol']
            })

    commission_by_trade = {}
    for c in commission_list:
        tid = c['tradeId']
        if tid is not None:
            commission_by_trade[tid] = commission_by_trade.get(tid, 0.0) + c['amount']

    closed_positions = []
    current_side = None  # 'LONG' or 'SHORT'
    current_size = 0.0
    avg_entry_price = 0.0
    entry_time = None

    partial_closes = []
    realized_pnl_acc = 0.0
    total_commission = 0.0
    total_funding = 0.0
    max_open_interest = 0.0
    funding_idx = 0

    def apply_funding_up_to(current_ts):
        nonlocal total_funding, funding_idx
        while funding_idx < len(funding_list):
            f_item = funding_list[funding_idx]
            if f_item['symbol'] != symbol:
                funding_idx += 1
                continue
            if f_item['time'] <= current_ts:
                total_funding += f_item['amount']
                funding_idx += 1
            else:
                break

    for t in trades:
        t_time = t['time']
        t_qty = float(t['qty'])
        t_price = float(t['price'])
        is_buyer = t['buyer']
        trade_id = t['id']

        signed_qty = t_qty if is_buyer else -t_qty
        trade_commission = commission_by_trade.get(trade_id, 0.0)

        if current_side is not None:
            apply_funding_up_to(t_time)

        old_size = current_size
        new_size = old_size + signed_qty

        # (A) 포지션이 없을 때 → 새 오픈
        if abs(old_size) < 1e-12:
            current_side = 'LONG' if signed_qty > 0 else 'SHORT'
            current_size = new_size
            avg_entry_price = t_price
            entry_time = datetime.fromtimestamp(t_time / 1000.0)

            partial_closes = []
            realized_pnl_acc = 0.0
            total_commission = trade_commission
            total_funding = 0.0
            max_open_interest = abs(new_size)

        # (B) 동일 방향 → 추가 진입
        elif (old_size > 0 and signed_qty > 0) or (old_size < 0 and signed_qty < 0):
            total_cost = abs(old_size) * avg_entry_price + abs(signed_qty) * t_price
            total_size = abs(old_size) + abs(signed_qty)
            avg_entry_price = total_cost / total_size

            current_size = new_size
            total_commission += trade_commission

            if abs(current_size) > max_open_interest:
                max_open_interest = abs(current_size)

        # (C) 반대 방향 체결 → 부분/완전 청산, 반전
        else:
            if abs(new_size) < abs(old_size):
                closed_qty = abs(signed_qty)
            else:
                closed_qty = abs(old_size)

            if old_size > 0:
                partial_pnl = (t_price - avg_entry_price) * closed_qty
            else:
                partial_pnl = (avg_entry_price - t_price) * closed_qty

            realized_pnl_acc += partial_pnl
            total_commission += trade_commission
            partial_closes.append({'qty': closed_qty, 'price': t_price})
            current_size = new_size

            if abs(current_size) < 1e-12:
                close_time = datetime.fromtimestamp(t_time / 1000.0)
                apply_funding_up_to(t_time)

                total_closed_qty = sum(pc['qty'] for pc in partial_closes)
                if total_closed_qty > 0:
                    weighted_sum = sum(pc['qty'] * pc['price'] for pc in partial_closes)
                    avg_close_price = weighted_sum / total_closed_qty
                else:
                    avg_close_price = t_price

                final_pnl = realized_pnl_acc + total_funding - abs(total_commission)

                closed_positions.append({
                    'symbol': symbol,
                    'positionSide': current_side,
                    'entryPrice': avg_entry_price,
                    'avgClosePrice': avg_close_price,
                    'maxOpenInterest': max_open_interest,
                    'closedVolume': total_closed_qty,
                    'closingPnl': final_pnl,
                    'openedAt': entry_time,
                    'closedAt': close_time
                })

                # 포지션 리셋
                current_side = None
                current_size = 0.0
                avg_entry_price = 0.0
                entry_time = None
                partial_closes = []
                realized_pnl_acc = 0.0
                total_commission = 0.0
                total_funding = 0.0
                max_open_interest = 0.0

            else:
                # 반전
                if old_size * new_size < 0:
                    remain_qty = new_size
                    current_side = 'LONG' if remain_qty > 0 else 'SHORT'
                    current_size = remain_qty
                    avg_entry_price = t_price
                    entry_time = datetime.fromtimestamp(t_time / 1000.0)

                    partial_closes = []
                    realized_pnl_acc = 0.0
                    total_commission = 0.0
                    total_funding = 0.0
                    max_open_interest = abs(remain_qty)

    return closed_positions


def _calculate_metrics(positions):
    """
    오늘 청산된 포지션 리스트(positions)를 바탕으로
    - 승률(winRate)
    - 손익비(riskReward)
    - 프로핏 팩터(profitFactor)
    - 평균 손익(avgPnl)
    - 거래 빈도(tradeCount)
    """
    total_count = len(positions)
    if total_count == 0:
        return {
            'winRate': 0.0,
            'riskReward': 0.0,
            'profitFactor': 0.0,
            'avgPnl': 0.0,
            'tradeCount': 0
        }

    wins = [p for p in positions if p['closingPnl'] > 0]
    losses = [p for p in positions if p['closingPnl'] < 0]

    win_count = len(wins)
    loss_count = len(losses)

    # 승률
    win_rate = (win_count / total_count) * 100.0

    # 평균 이익, 평균 손실
    avg_profit = sum(p['closingPnl'] for p in wins) / len(wins) if wins else 0.0
    avg_loss_abs = abs(sum(p['closingPnl'] for p in losses) / len(losses)) if losses else 0.0

    # 손익비
    risk_reward = 0.0
    if avg_loss_abs != 0.0:
        risk_reward = avg_profit / avg_loss_abs

    # 프로핏 팩터
    sum_profit = sum(p['closingPnl'] for p in wins)
    sum_loss_abs = abs(sum(p['closingPnl'] for p in losses))
    profit_factor = 0.0
    if sum_loss_abs != 0.0:
        profit_factor = sum_profit / sum_loss_abs

    # 평균 손익 (모든 포지션 대상)
    total_pnl = sum(p['closingPnl'] for p in positions)
    avg_pnl = total_pnl / total_count

    # 거래 빈도
    trade_count = total_count

    return {
        'winRate': win_rate,
        'riskReward': risk_reward,
        'profitFactor': profit_factor,
        'avgPnl': avg_pnl,
        'tradeCount': trade_count
    }

def save_closed_position_csv(client, symbol, out_csv="closed_position.csv"):
    """
    1) reconstruct_closed_positions(client, symbol)에서 오늘 청산된 포지션만 골라
    2) out_csv 파일에 기록
    (승률 등 요약치는 여기 기록하지 않음)
    리턴값: 오늘 포지션 리스트
    """
    all_positions = reconstruct_closed_positions(client, symbol)
    today = datetime.now().date()
    today_closed = [p for p in all_positions if p['closedAt'].date() == today]
    today_closed.sort(key=lambda x: x['closedAt'], reverse=True)

    # CSV 필드 (지표 없음)
    fieldnames = [
        'symbol', 'positionSide', 'entryPrice', 'avgClosePrice',
        'maxOpenInterest', 'closedVolume', 'closingPnl',
        'openedAt', 'closedAt'
    ]

    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for pos in today_closed:
            writer.writerow({
                'symbol': pos['symbol'],
                'positionSide': pos['positionSide'],
                'entryPrice': round(pos['entryPrice'], 2),
                'avgClosePrice': round(pos['avgClosePrice'], 2),
                'maxOpenInterest': round(pos['maxOpenInterest'], 5),
                'closedVolume': round(pos['closedVolume'], 5),
                'closingPnl': round(pos['closingPnl'], 2),
                'openedAt': pos['openedAt'].strftime('%Y-%m-%d %H:%M:%S'),
                'closedAt': pos['closedAt'].strftime('%Y-%m-%d %H:%M:%S')
            })

    return today_closed


def save_today_trade_stats_csv(today_positions, out_csv="today_trade_stats.csv"):
    """
    1) today_positions 리스트(오늘 청산된 포지션)
    2) 승률, 손익비, 프로핏 팩터, 평균손익, 거래 빈도 계산
    3) out_csv(영어 이름) 파일에 한 줄로 기록
    """
    metrics = _calculate_metrics(today_positions)

    # CSV 필드
    fieldnames = [
        'winRate', 'riskReward', 'profitFactor', 'avgPnl', 'tradeCount'
    ]

    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        row = {
            'winRate': f"{metrics['winRate']:.2f}",
            'riskReward': f"{metrics['riskReward']:.2f}",
            'profitFactor': f"{metrics['profitFactor']:.2f}",
            'avgPnl': f"{metrics['avgPnl']:.2f}",
            'tradeCount': metrics['tradeCount']
        }
        writer.writerow(row)


def main():
    """
    모듈 단독 실행 시,
    1) client 생성
    2) 오늘 청산된 포지션 -> closed_position.csv
    3) 지표 -> today_trade_stats.csv
    """
    client = get_default_client()
    symbol = "BTCUSDT"

    closed_csv = "closed_position.csv"
    stats_csv = "today_trade_stats.csv"

    today_positions = save_closed_position_csv(client, symbol, closed_csv)
    save_today_trade_stats_csv(today_positions, stats_csv)

    print(f"[{symbol}] 오늘 청산된 포지션 {len(today_positions)}건 → {closed_csv}")
    print(f"오늘 거래 통계(today_trade_stats.csv) 생성 완료: {stats_csv}")


if __name__ == "__main__":
    main()
