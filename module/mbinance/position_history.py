# 위치: my_project/module/position_history.py

import pandas as pd


def fetch_binance_data(client, symbol="BTCUSDT", limit=1000):
    """
    바이낸스 선물 API에서 주문, 체결, Income 데이터를 가져와 DataFrame 3개를 반환한다.
    access_key와 secret_key를 파라미터로 받아서 Client를 생성한다.
    """
    orders = client.futures_get_all_orders(symbol=symbol, limit=limit)
    trades = client.futures_account_trades(symbol=symbol, limit=limit)
    income = client.futures_income_history(symbol=symbol, limit=limit)

    orders_df = pd.DataFrame(orders)
    trades_df = pd.DataFrame(trades)
    income_df = pd.DataFrame(income)

    # 밀리초 -> datetime 변환
    if not orders_df.empty:
        orders_df["time"] = pd.to_datetime(orders_df["time"], unit="ms")
        orders_df["updateTime"] = pd.to_datetime(orders_df["updateTime"], unit="ms")
    if not trades_df.empty:
        trades_df["time"] = pd.to_datetime(trades_df["time"], unit="ms")
    if not income_df.empty:
        income_df["time"] = pd.to_datetime(income_df["time"], unit="ms")

    orders_df.sort_values("time", inplace=True, ignore_index=True)
    trades_df.sort_values("time", inplace=True, ignore_index=True)
    income_df.sort_values("time", inplace=True, ignore_index=True)

    return orders_df, trades_df, income_df

def _close_position(symbol, open_time, close_time,
                    old_size, total_cost, total_qty,
                    is_isolated_mode, income_pnl_df):
    """
    포지션이 완전 청산될 때 포지션 레코드 생성 (롱/숏, entryPrice, avgClosePrice, closingPnL 등)
    """
    if abs(total_qty) > 0:
        entry_price = abs(total_cost / total_qty)
    else:
        entry_price = 0.0

    # 근사치로 청산 시점 주변의 REALIZED_PNL을 합산
    tolerance = pd.Timedelta(seconds=2)
    relevant_pnl_df = income_pnl_df[
        (income_pnl_df["symbol"] == symbol) &
        (income_pnl_df["time"] >= open_time - tolerance) &
        (income_pnl_df["time"] <= close_time + tolerance)
    ]
    closing_pnl = relevant_pnl_df["income"].sum() if not relevant_pnl_df.empty else 0.0

    closed_vol = abs(old_size)

    if old_size > 0:  # 롱
        avg_close_price = (closing_pnl / old_size) + entry_price if old_size != 0 else 0.0
        direction = "LONG"
    else:            # 숏
        size_abs = abs(old_size)
        avg_close_price = entry_price - (closing_pnl / size_abs) if size_abs != 0 else 0.0
        direction = "SHORT"

    mode_str = "Isolated" if is_isolated_mode else "Cross"

    return {
        "symbol": symbol,
        "mode": mode_str,
        "direction": direction,
        "entryPrice": entry_price,
        "avgClosePrice": avg_close_price,
        "closingPnL": closing_pnl,
        "maxOpenInterest": abs(old_size),
        "closedVol": closed_vol,
        "openedTime": open_time,
        "closedTime": close_time
    }

def build_position_history(client,
                           symbol="BTCUSDT", limit=1000,
                           cutoff_dt=None):
    """
    바이낸스 선물 계정에서 체결/Income 데이터를 조회하여
    (1) 포지션 히스토리 생성
    (2) UTC->KST 변환, 소수점 반올림, totalPnL 계산
    (3) cutoff_dt(조회 기준 날짜) 이후의 데이터만 필터링 등
    을 모두 수행한 뒤 최종 DataFrame을 반환한다.
    """
    orders_df, trades_df, income_df = fetch_binance_data(client,
                                                         symbol=symbol, limit=limit)

    # REALIZED_PNL만 추출
    if not income_df.empty:
        income_pnl_df = income_df[income_df["incomeType"] == "REALIZED_PNL"].copy()
        income_pnl_df["income"] = income_pnl_df["income"].astype(float, errors="ignore")
    else:
        income_pnl_df = pd.DataFrame(columns=income_df.columns)

    # 체결 정보 정리
    trades_df["qty"] = trades_df.get("qty", 0).astype(float, errors="ignore")
    trades_df["price"] = trades_df.get("price", 0).astype(float, errors="ignore")
    trades_df["side"] = trades_df.get("side", "NONE")
    trades_df["isIsolated"] = trades_df.get("isIsolated", False)
    trades_df["positionSide"] = trades_df.get("positionSide", "BOTH").fillna("BOTH")

    positions = []
    current_size = 0.0
    total_cost = 0.0
    total_qty = 0.0
    position_start_time = None
    is_isolated_mode = None

    # 포지션 추적
    for idx, row in trades_df.iterrows():
        side = row["side"]
        qty = float(row["qty"])
        price = float(row["price"])
        ttime = row["time"]
        isolated = row["isIsolated"]
        pos_side = row["positionSide"]

        # 헤지 모드는 제외
        if pos_side != "BOTH":
            continue

        if current_size == 0:
            # 새 포지션 시작
            position_start_time = ttime
            is_isolated_mode = isolated
            total_cost = price * qty
            total_qty = qty if side == "BUY" else -qty
            current_size = total_qty
        else:
            old_size = current_size
            if side == "BUY":
                current_size += qty
            else:
                current_size -= qty

            # 포지션 반전 처리
            if old_size > 0 and current_size < 0:
                positions.append(_close_position(
                    symbol, position_start_time, ttime,
                    old_size, total_cost, old_size,
                    is_isolated_mode, income_pnl_df
                ))
                # 새로운 숏 포지션 시작
                position_start_time = ttime
                is_isolated_mode = isolated
                total_cost = price * abs(current_size)
                total_qty = current_size
            elif old_size < 0 and current_size > 0:
                positions.append(_close_position(
                    symbol, position_start_time, ttime,
                    old_size, total_cost, old_size,
                    is_isolated_mode, income_pnl_df
                ))
                # 새로운 롱 포지션 시작
                position_start_time = ttime
                is_isolated_mode = isolated
                total_cost = price * abs(current_size)
                total_qty = current_size
            else:
                # 동일 방향이거나 부분 청산인 경우
                if old_size * current_size > 0:
                    total_cost += price * qty
                    total_qty = current_size
                else:
                    # 완전 청산
                    if current_size == 0:
                        positions.append(_close_position(
                            symbol, position_start_time, ttime,
                            old_size, total_cost, old_size,
                            is_isolated_mode, income_pnl_df
                        ))
                        position_start_time = None
                        total_cost = 0.0
                        total_qty = 0.0
                        is_isolated_mode = None
                    else:
                        # 부분 청산
                        portion = old_size - current_size
                        ratio = abs(portion / old_size) if old_size != 0 else 1
                        cost_reduced = total_cost * ratio
                        total_cost -= cost_reduced
                        total_qty = current_size

    # 모든 거래 처리 후, 아직 청산되지 않은 열린 포지션 기록
    if current_size != 0 and position_start_time is not None:
        entry_price = abs(total_cost / total_qty) if total_qty != 0 else 0.0
        open_record = {
            "symbol": symbol,
            "mode": "Isolated" if is_isolated_mode else "Cross",
            "direction": "LONG" if current_size > 0 else "SHORT",
            "entryPrice": round(entry_price, 2),
            "avgClosePrice": None,
            "closingPnL": None,
            "maxOpenInterest": abs(current_size),
            "closedVol": None,
            "openedTime": position_start_time,
            "closedTime": None
        }
        positions.append(open_record)

    columns = [
        "symbol", "mode", "direction",
        "entryPrice", "avgClosePrice", "closingPnL",
        "maxOpenInterest", "closedVol",
        "openedTime", "closedTime"
    ]
    history_df = pd.DataFrame(positions, columns=columns)

    # 후처리: UTC->KST 변환, 소수점 반올림, totalPnL 계산 등
    if not history_df.empty:
        if pd.api.types.is_datetime64_any_dtype(history_df["openedTime"]):
            history_df["openedTime"] = history_df["openedTime"] + pd.Timedelta(hours=9)
        if pd.api.types.is_datetime64_any_dtype(history_df["closedTime"]):
            history_df["closedTime"] = history_df["closedTime"] + pd.Timedelta(hours=9)

        history_df["entryPrice"] = history_df["entryPrice"].round(2)
        if history_df["avgClosePrice"].notnull().all():
            history_df["avgClosePrice"] = history_df["avgClosePrice"].round(2)
        if history_df["closingPnL"].notnull().all():
            history_df["closingPnL"] = history_df["closingPnL"].round(2)

        history_df.sort_values("closedTime", ascending=True, inplace=True, ignore_index=True)
        history_df["totalPnL"] = history_df["closingPnL"].fillna(0).cumsum().round(2)
        history_df.sort_values("closedTime", ascending=False, inplace=True, ignore_index=True)

        # cutoff_dt 적용 시 열린 포지션(closedTime이 NaT)도 유지하도록 조건 수정
        if cutoff_dt is not None:
            if isinstance(cutoff_dt, str):
                cutoff_dt = pd.to_datetime(cutoff_dt, format="%Y%m%d%H%M")
            filtered = history_df[
                (history_df["closedTime"].isna()) | (history_df["closedTime"] >= cutoff_dt)
            ].copy()
            filtered.sort_values("closedTime", ascending=True, inplace=True, ignore_index=True)
            filtered["totalPnL"] = filtered["closingPnL"].fillna(0).cumsum().round(2)
            filtered.sort_values("closedTime", ascending=False, inplace=True, ignore_index=True)
            return filtered
        else:
            return history_df
    else:
        return history_df
