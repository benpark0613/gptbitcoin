# main.py

import os
import pandas as pd

# 필요한 모듈 임포트
from backup.multi_inticator.helper import create_binance_client
from backup.multi_inticator.data import DataManager


def load_indicator_params(path="config/parameters.json"):
    """
    인디케이터 파라미터 JSON 파일 로딩
    예) {
       "MA": {
          "short_periods": [5,10,20],
          "long_periods":  [50,100,200]
       },
       ...
    }
    """
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_strategy_params(path="config/strategy_config.json"):
    """
    전략 파라미터 JSON 파일 로딩
    예) {
       "time_delay_list": [0,1,2],
       "holding_period_list": [6,12,24],
       "shorting_allowed_list": [false, true]
    }
    """
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ─────────────────────────────────────────
# "단일 지표" 전용 config 생성 함수
# ─────────────────────────────────────────
from itertools import product

def generate_indicator_configs(ind_params):
    """
    논문 방식(각 지표별 독립 테스트)을 위해,
    한 번에 한 지표만 담긴 config들을 만들도록 수정.

    예) ind_params (예시):
    {
      "MA":{
         "short_periods":[5,10,20],
         "long_periods":[50,100]
      },
      "RSI":{
         "lengths":[7,14,30],
         "overbought_values":[70,80],
         "oversold_values":[20,30]
      },
      ...
    }
    ⇒ 반환 값:
       [{"MA":{"short_period":5,"long_period":50}}, {"MA":{"short_period":5,"long_period":100}}, ...,
        {"RSI":{"length":7,"overbought":70,"oversold":30}}, ... ]
       즉, 하나의 dict당 하나의 인디케이터만 포함.
    """

    configs = []

    # 1) MA
    if "MA" in ind_params:
        ma_params = ind_params["MA"]
        short_list = ma_params.get("short_periods", [])
        long_list  = ma_params.get("long_periods", [])
        for sp, lp in product(short_list, long_list):
            c = {
                "MA": {
                    "short_period": sp,
                    "long_period": lp,
                    "price": "close"
                }
            }
            configs.append(c)

    # 2) RSI
    if "RSI" in ind_params:
        rsi_params = ind_params["RSI"]
        lengths     = rsi_params.get("lengths", [])
        overboughts = rsi_params.get("overbought_values", [])
        oversolds   = rsi_params.get("oversold_values", [])
        for l_, ob_, os_ in product(lengths, overboughts, oversolds):
            c = {
                "RSI": {
                    "length": l_,
                    "overbought": ob_,
                    "oversold": os_
                }
            }
            configs.append(c)

    # 3) Filter
    if "Filter" in ind_params:
        f_params = ind_params["Filter"]
        x_vals   = f_params.get("x_values", [])
        y_vals   = f_params.get("y_values", [])
        windows  = f_params.get("windows", [])
        for x_, y_, w_ in product(x_vals, y_vals, windows):
            c = {
                "Filter": {
                    "x": x_,
                    "y": y_,
                    "window": w_
                }
            }
            configs.append(c)

    # 4) ChannelBreakout
    if "ChannelBreakout" in ind_params:
        cb_params = ind_params["ChannelBreakout"]
        cb_windows = cb_params.get("windows", [])
        c_vals     = cb_params.get("c_values", [])
        for w_, c_ in product(cb_windows, c_vals):
            cdict = {
                "ChannelBreakout": {
                    "window": w_,
                    "c": c_
                }
            }
            configs.append(cdict)

    # 5) OBV
    if "OBV" in ind_params:
        obv_params = ind_params["OBV"]
        sp_list = obv_params.get("short_periods", [])
        lp_list = obv_params.get("long_periods", [])
        for sp_, lp_ in product(sp_list, lp_list):
            c = {
                "OBV": {
                    "short_period": sp_,
                    "long_period": lp_
                }
            }
            configs.append(c)

    # 6) Support_Resistance
    if "Support_Resistance" in ind_params:
        sr_params = ind_params["Support_Resistance"]
        sr_windows = sr_params.get("windows", [])
        for w_ in sr_windows:
            c = {
                "Support_Resistance": {
                    "window": w_
                }
            }
            configs.append(c)

    return configs


def generate_strategy_configs(strat_params):
    """
    전략 파라미터: time_delay, holding_period, shorting_allowed 등
    예:
      {
        "time_delay_list": [0,1,2],
        "holding_period_list": [6,12],
        "shorting_allowed_list": [false]
      }
    => 모든 조합을 만들고 반환
    """
    from itertools import product
    time_delays = strat_params.get("time_delay_list", [0])
    holding_periods = strat_params.get("holding_period_list", [6])
    shorting_list = strat_params.get("shorting_allowed_list", [False])

    combos = []
    for td, hp, sa in product(time_delays, holding_periods, shorting_list):
        combos.append({
            "time_delay": td,
            "holding_period": hp,
            "shorting_allowed": sa
        })
    return combos


def combine_configs(ind_cfg, strat_cfg):
    """
    단일 지표 config + 전략 파라미터 config -> 하나의 dict로 합침
    """
    merged = {}
    for k, v in ind_cfg.items():
        merged[k] = v
    for k, v in strat_cfg.items():
        merged[k] = v
    return merged


def calc_buy_and_hold_metrics(df, initial_capital=100000):
    """
    Buy&Hold 성과 지표 계산:
      - final_portfolio_value
      - Total Return
      - CAGR
      - Max Drawdown
      - Sharpe Ratio
    """
    if len(df) < 2:
        return {
            "final_portfolio_value": initial_capital,
            "Total Return": 0,
            "CAGR": 0,
            "Max Drawdown": 0,
            "Sharpe Ratio": 0
        }

    # start / end
    start_val = df["close"].iloc[0]
    end_val   = df["close"].iloc[-1]
    total_ret = end_val / start_val - 1
    final_port_val = initial_capital * (1 + total_ret)

    start_date = df.index[0]
    end_date   = df.index[-1]
    days = (end_date - start_date).days
    years = days / 365 if days > 0 else 0
    if years > 0:
        cagr = (end_val / start_val) ** (1 / years) - 1
    else:
        cagr = 0

    series = df["close"] / start_val
    running_max = series.cummax()
    dd = (series - running_max) / running_max
    max_dd = dd.min()

    daily_ret = df["close"].pct_change().dropna()
    mean_ret = daily_ret.mean()
    std_ret  = daily_ret.std()
    sharpe = 0
    if std_ret != 0:
        sharpe = (mean_ret / std_ret) * (365**0.5)

    return {
        "final_portfolio_value": round(final_port_val, 2),
        "Total Return": round(total_ret, 2),
        "CAGR": round(cagr, 2),
        "Max Drawdown": round(max_dd, 2),
        "Sharpe Ratio": round(sharpe, 2)
    }


def update_and_load_data(symbol, intervals, start_date, end_date, origin_data_folder, warmup_period=26):
    """
    1) DataManager 이용해서 바이낸스 선물 데이터(또는 현물) 업데이트
    2) CSV를 읽어서 {interval: DataFrame} 형태로 반환
    """
    client = create_binance_client()
    manager = DataManager(
        client=client,
        symbol=symbol,
        intervals=intervals,
        start_date=start_date,
        end_date=end_date,
        save_folder=origin_data_folder,
        warmup_period=warmup_period
    )
    manager.update_all()

    data_map = {}
    for iv in intervals:
        csvp = os.path.join(origin_data_folder, f"{symbol}_{iv}.csv")
        if not os.path.exists(csvp):
            raise FileNotFoundError(f"{csvp} not found after update!")
        df_tmp = pd.read_csv(csvp, parse_dates=["open_time_dt"])
        df_tmp.set_index("open_time_dt", inplace=True)
        # 범위 필터
        df_tmp = df_tmp[df_tmp.index >= pd.to_datetime(start_date)]
        data_map[iv] = df_tmp
    return data_map


def create_cases(symbol, intervals, data_map, indicator_configs, strategy_configs, initial_capital=100000):
    """
    각 interval에 대해,
    (indicator_config × strategy_config) 조합을 만들어
    병렬 테스트용 case list를 구성
    """
    cases = []
    for iv in intervals:
        df_iv = data_map[iv]
        for i_cfg in indicator_configs:
            for s_cfg in strategy_configs:
                merged = combine_configs(i_cfg, s_cfg)
                c = {
                    "symbol": symbol,
                    "interval": iv,
                    "config": merged,
                    "data": df_iv,
                    "initial_capital": initial_capital
                }
                cases.append(c)
    return cases


def main():
    """
    1) 사용자 설정
    2) 데이터 업데이트 & 로드
    3) 전수 테스트 & Buy&Hold 결과 기록
    4) CSV 저장
    """
    import shutil

    # [1] 사용자 설정
    symbol = "BTCUSDT"
    intervals = ["4h"]
    start_date = "2024-01-01"
    end_date   = "2024-12-31"
    origin_data_folder = "data/origin_data"
    warmup_period = 26
    initial_capital = 100000
    test_result_folder = "test_result"

    # [2] 데이터 업데이트 & 로드
    data_map = update_and_load_data(
        symbol=symbol,
        intervals=intervals,
        start_date=start_date,
        end_date=end_date,
        origin_data_folder=origin_data_folder,
        warmup_period=warmup_period
    )

    # 파라미터 로딩
    indicator_params = load_indicator_params("config/parameters.json")
    strategy_params  = load_strategy_params("config/strategy_config.json")

    # (단일 지표) 인디케이터 config & 전략 config
    ind_confs  = generate_indicator_configs(indicator_params)
    strat_confs= generate_strategy_configs(strategy_params)

    print(f"Indicator configs: {len(ind_confs)}")
    print(f"Strategy configs : {len(strat_confs)}")

    # [3] interval별로 순회 → Earliest/Latest, Buy&Hold, 병렬전수테스트
    all_results = []

    for iv in intervals:
        print(f"[INFO] Processing interval: {iv}")
        df_iv = data_map[iv]
        if len(df_iv) < 2:
            print(f"[WARN] Not enough data for interval={iv}, skipping.")
            continue

        # Earliest/Latest
        earliest_date = df_iv.index.min().strftime("%Y-%m-%d %H:%M:%S")
        latest_date   = df_iv.index.max().strftime("%Y-%m-%d %H:%M:%S")

        info_cols = ["symbol","interval","config","final_portfolio_value",
                     "Total Return","CAGR","Max Drawdown","Sharpe Ratio","Number of Trades"]
        row_earliest = {c: "" for c in info_cols}
        row_latest   = {c: "" for c in info_cols}

        row_earliest["symbol"]   = "EarliestDataDate"
        row_earliest["interval"] = earliest_date
        row_latest["symbol"]     = "LatestDataDate"
        row_latest["interval"]   = latest_date

        all_results.append(row_earliest)
        all_results.append(row_latest)

        # Buy & Hold
        bh_metrics = calc_buy_and_hold_metrics(df_iv, initial_capital=initial_capital)
        bh_row = {
            "symbol": symbol,
            "interval": iv,
            "config": "BUY&HOLD",
            "final_portfolio_value": bh_metrics["final_portfolio_value"],
            "Total Return":          bh_metrics["Total Return"],
            "CAGR":                  bh_metrics["CAGR"],
            "Max Drawdown":          bh_metrics["Max Drawdown"],
            "Sharpe Ratio":          bh_metrics["Sharpe Ratio"],
            "Number of Trades":      ""
        }
        all_results.append(bh_row)

        # 병렬 전수테스트
        from backup.multi_inticator.statistics import run_multiple_tests_parallel
        cases_for_iv = create_cases(symbol, [iv], data_map, ind_confs, strat_confs, initial_capital)

        df_results_iv = run_multiple_tests_parallel(cases_for_iv)
        # (선택) 거래 건수=0 제거
        if "Number of Trades" in df_results_iv.columns:
            df_results_iv = df_results_iv[df_results_iv["Number of Trades"] != 0]

        records_iv = df_results_iv.to_dict("records")
        all_results.extend(records_iv)

    # [4] CSV 저장
    if os.path.exists(test_result_folder):
        shutil.rmtree(test_result_folder)
    os.makedirs(test_result_folder, exist_ok=True)

    col_order = ["symbol","interval","config","final_portfolio_value",
                 "Total Return","CAGR","Max Drawdown","Sharpe Ratio","Number of Trades"]
    df_final = pd.DataFrame(all_results, columns=col_order)
    out_csv = os.path.join(test_result_folder, "all_test_results.csv")
    df_final.to_csv(out_csv, index=False)
    print(f"[INFO] Saved results => {out_csv}")


if __name__ == "__main__":
    main()
