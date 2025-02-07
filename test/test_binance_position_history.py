import os
import math
import csv
from dotenv import load_dotenv
from binance.client import Client
from datetime import datetime

# 1. 환경변수(ENV)에서 BINANCE_ACCESS_KEY, BINANCE_SECRET_KEY 불러오기
load_dotenv()
access_key = os.getenv("BINANCE_ACCESS_KEY")
secret_key = os.getenv("BINANCE_SECRET_KEY")

# 바이낸스 Futures 클라이언트 초기화
client = Client(access_key, secret_key)

def get_all_futures_trades(symbol):
    """
    바이낸스 선물 체결 이력(Trades)을 시간 순으로 전부 가져오는 함수.
    - GET /fapi/v1/userTrades
    - 필요하다면 날짜 범위, fromId 등을 이용해 반복 호출
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

    # 시간 순으로 정렬
    all_trades.sort(key=lambda x: x['time'])
    return all_trades

def get_all_futures_income(symbol):
    """
    바이낸스 선물 인컴(Incomes) 데이터(주로 COMMISSION, FUNDING_FEE)를
    시간 순으로 전부 가져오는 함수.
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

    # 시간 순으로 정렬
    all_income.sort(key=lambda x: x['time'])
    return all_income

def reconstruct_closed_positions(symbol):
    """
    체결(Trades) + 인컴(Incomes)을 종합하여 포지션을 로컬에서 재구성.
    - 부분 진입, 부분 청산, 포지션 반전 처리
    - 0이 되는 순간(완전 청산)에 포지션 1건 기록
    - 수수료(commission), 펀딩(funding) 반영
    - 최종적으로 닫힌 포지션의 리스트 반환
    """
    trades = get_all_futures_trades(symbol)
    incomes = get_all_futures_income(symbol)

    # 수수료 / 펀딩 나누어 담기
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

    # tradeId => 수수료 매핑 딕셔너리
    commission_by_trade = {}
    for c in commission_list:
        t_id = c['tradeId']
        if t_id is not None:
            commission_by_trade[t_id] = commission_by_trade.get(t_id, 0.0) + c['amount']

    # 포지션이 완전히 청산된(사이즈=0) 시점에 기록할 리스트
    closed_positions = []

    # 포지션 상태 변수
    current_side = None   # 'LONG' or 'SHORT'
    current_size = 0.0
    avg_entry_price = 0.0
    entry_time = None

    # 부분 청산 체결 저장(평균 청산가 계산)
    partial_closes = []
    # 누적 실현 손익, 누적 수수료, 펀딩
    realized_pnl_acc = 0.0
    total_commission = 0.0
    total_funding = 0.0
    # 포지션 최대 보유 수량
    max_open_interest = 0.0
    # 펀딩 처리 시 사용할 인덱스
    funding_idx = 0

    def apply_funding_up_to(current_ts):
        """
        current_ts 시점 이전에 발생한 FUNDING_FEE를 포지션에 누적
        """
        nonlocal total_funding, funding_idx
        while funding_idx < len(funding_list):
            f_item = funding_list[funding_idx]
            # 현재 심볼이 아닌 것은 스킵
            if f_item['symbol'] != symbol:
                funding_idx += 1
                continue
            # 펀딩 발생 시점이 현재 트레이드 시점 이전이면 누적
            if f_item['time'] <= current_ts:
                total_funding += f_item['amount']
                funding_idx += 1
            else:
                break

    # 체결 시간 순으로 처리
    for t in trades:
        t_time = t['time']
        t_qty = float(t['qty'])
        t_price = float(t['price'])
        is_buyer = t['buyer']  # True=매수, False=매도
        trade_id = t['id']

        # 롱 방향(+) / 숏 방향(-)
        signed_qty = t_qty if is_buyer else -t_qty
        # 트레이드 단위 수수료
        trade_commission = commission_by_trade.get(trade_id, 0.0)

        # 포지션 열려 있다면, 중간에 펀딩 적용
        if current_side is not None:
            apply_funding_up_to(t_time)

        old_size = current_size
        new_size = old_size + signed_qty

        # (A) 포지션이 없을 때 -> 새로 오픈
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

        # (B) 기존과 동일 방향 추가 진입
        elif (old_size > 0 and signed_qty > 0) or (old_size < 0 and signed_qty < 0):
            total_cost = abs(old_size) * avg_entry_price + abs(signed_qty) * t_price
            total_size = abs(old_size) + abs(signed_qty)
            avg_entry_price = total_cost / total_size

            current_size = new_size
            total_commission += trade_commission

            if abs(current_size) > max_open_interest:
                max_open_interest = abs(current_size)

        # (C) 반대 방향 체결 -> 부분 청산, 완전 청산, 반전
        else:
            # 부분 청산 or 완전 청산
            if abs(new_size) < abs(old_size):
                closed_qty = abs(signed_qty)   # 청산되는 수량
            else:
                closed_qty = abs(old_size)

            # 청산분 PnL 계산
            if old_size > 0:
                partial_pnl = (t_price - avg_entry_price) * closed_qty
            else:
                partial_pnl = (avg_entry_price - t_price) * closed_qty

            realized_pnl_acc += partial_pnl
            total_commission += trade_commission

            # 부분 청산 기록(평균 청산가)
            partial_closes.append({'qty': closed_qty, 'price': t_price})

            current_size = new_size

            # 완전히 0이 되면 포지션 종료
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
                    'positionSide': current_side,         # 'LONG' or 'SHORT'
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
                # 반전(롱 -> 숏, 숏 -> 롱)인지 여부 확인
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

def main():
    symbol = "BTCUSDT"

    # (1) 모든 청산된 포지션 재구성
    closed_positions = reconstruct_closed_positions(symbol)

    # (2) 오늘 날짜
    today_date = datetime.now().date()

    # (3) 오늘 청산된 포지션만 필터
    today_closed = [
        pos for pos in closed_positions
        if pos['closedAt'].date() == today_date
    ]

    # (4) 가장 최근에 청산된 순서대로 정렬(내림차순)
    today_closed.sort(key=lambda x: x['closedAt'], reverse=True)

    # (5) CSV 필드 설정
    fieldnames = [
        'symbol',
        'positionSide',
        'entryPrice',
        'avgClosePrice',
        'maxOpenInterest',
        'closedVolume',
        'closingPnl',
        'openedAt',
        'closedAt'
    ]

    # (6) CSV 파일에 기록하기 (빈 줄, 빈 칼럼 방지)
    filename = "closed_positions_today.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
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

    print(f"오늘({today_date}) 청산된 포지션: 총 {len(today_closed)}건")
    print(f"CSV 파일 생성: {filename}")

if __name__ == "__main__":
    main()
