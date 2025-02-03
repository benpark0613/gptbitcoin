import os
from dotenv import load_dotenv
import pandas as pd
from mbinance.client import Client

def fetch_binance_data(symbol="BTCUSDT", limit=1000):
    load_dotenv()
    access_key = os.getenv("BINANCE_ACCESS_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY")
    client = Client(access_key, secret_key)

    orders = client.futures_get_all_orders(symbol=symbol, limit=limit)
    trades = client.futures_account_trades(symbol=symbol, limit=limit)
    income = client.futures_income_history(symbol=symbol, limit=limit)

    orders_df = pd.DataFrame(orders)
    trades_df = pd.DataFrame(trades)
    income_df = pd.DataFrame(income)

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
    if abs(total_qty) > 0:
        entry_price = abs(total_cost / total_qty)
    else:
        entry_price = 0.0

    tolerance = pd.Timedelta(seconds=2)
    relevant_pnl_df = income_pnl_df[
        (income_pnl_df["symbol"] == symbol) &
        (income_pnl_df["time"] >= open_time - tolerance) &
        (income_pnl_df["time"] <= close_time + tolerance)
    ]
    closing_pnl = relevant_pnl_df["income"].sum() if not relevant_pnl_df.empty else 0.0

    closed_vol = abs(old_size)

    if old_size > 0:  # 롱
        if old_size != 0:
            avg_close_price = (closing_pnl / old_size) + entry_price
        else:
            avg_close_price = 0.0
        direction = "LONG"
    else:             # 숏
        size_abs = abs(old_size)
        if size_abs != 0:
            avg_close_price = entry_price - (closing_pnl / size_abs)
        else:
            avg_close_price = 0.0
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

def build_position_history(symbol="BTCUSDT", limit=1000):
    orders_df, trades_df, income_df = fetch_binance_data(symbol, limit)

    if not income_df.empty:
        income_pnl_df = income_df[income_df["incomeType"] == "REALIZED_PNL"].copy()
        income_pnl_df["income"] = income_pnl_df["income"].astype(float, errors="ignore")
    else:
        income_pnl_df = pd.DataFrame(columns=income_df.columns)

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

    for idx, row in trades_df.iterrows():
        side = row["side"]
        qty = float(row["qty"])
        price = float(row["price"])
        ttime = row["time"]
        isolated = row["isIsolated"]
        pos_side = row["positionSide"]

        if pos_side != "BOTH":
            continue

        if current_size == 0:
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

            if old_size > 0 and current_size < 0:
                positions.append(_close_position(
                    symbol, position_start_time, ttime,
                    old_size, total_cost, old_size,
                    is_isolated_mode, income_pnl_df
                ))
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
                position_start_time = ttime
                is_isolated_mode = isolated
                total_cost = price * abs(current_size)
                total_qty = current_size
            else:
                if old_size * current_size > 0:
                    # 같은 방향 유지
                    total_cost += price * qty
                    total_qty = current_size
                else:
                    if current_size == 0:
                        # 완전 청산
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

    columns = [
        "symbol","mode","direction",
        "entryPrice","avgClosePrice","closingPnL",
        "maxOpenInterest","closedVol",
        "openedTime","closedTime"
    ]
    history_df = pd.DataFrame(positions, columns=columns)
    return history_df

def main():
    # 1) 전체 히스토리 불러오기
    history_df = build_position_history(symbol="BTCUSDT", limit=200)

    # 2) 시간대 변환(UTC->KST) & 소수점 정리
    if "openedTime" in history_df.columns and pd.api.types.is_datetime64_any_dtype(history_df["openedTime"]):
        history_df["openedTime"] = history_df["openedTime"] + pd.Timedelta(hours=9)
    if "closedTime" in history_df.columns and pd.api.types.is_datetime64_any_dtype(history_df["closedTime"]):
        history_df["closedTime"] = history_df["closedTime"] + pd.Timedelta(hours=9)

    history_df["entryPrice"]    = history_df["entryPrice"].round(2)
    history_df["avgClosePrice"] = history_df["avgClosePrice"].round(2)
    history_df["closingPnL"]    = history_df["closingPnL"].round(2)

    # 먼저 전체를 오름차순 → 누적합
    history_df.sort_values("closedTime", ascending=True, inplace=True, ignore_index=True)
    history_df["totalPnL"] = history_df["closingPnL"].cumsum().round(2)

    # 다시 내림차순으로 바꿔놓기
    history_df.sort_values("closedTime", ascending=False, inplace=True, ignore_index=True)

    # 3) 사용자 지정 시각 이후 데이터만 필터 (예: 202502010100 → 2025-02-01 01:00)
    user_input_str = "202502010100"  # 원하는 시점
    filter_dt = pd.to_datetime(user_input_str, format="%Y%m%d%H%M")
    # closedTime >= filter_dt
    filtered_df = history_df[history_df["closedTime"] >= filter_dt].copy()

    # 필터링된 결과에서 다시 ‘totalPnL’을 새로 계산하고 싶다면, 아래처럼 재계산
    filtered_df.sort_values("closedTime", ascending=True, inplace=True, ignore_index=True)
    filtered_df["totalPnL"] = filtered_df["closingPnL"].cumsum().round(2)
    filtered_df.sort_values("closedTime", ascending=False, inplace=True, ignore_index=True)

    # 4) 결과 출력 및 CSV 저장
    print("=== Filtered by closedTime >= {} ===".format(filter_dt))
    print(filtered_df)

    output_file = "filtered_position_history.csv"
    filtered_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\nFiltered data saved to '{output_file}'")

if __name__ == "__main__":
    main()
