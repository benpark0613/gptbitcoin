# optimize_multi_interval.py

import os
import pandas as pd
from datetime import datetime
import shutil

# CSV 업데이트 함수, 커스텀 최적화 모듈, 전략 클래스를 임포트합니다.
from backup.NLS2.module import update_csv
from backup.NLS2.module import run_all_combinations
from backup.NLS2.module import NLS2Combined
from backup.NLS2.module import BuyAndHoldStrategy
from backup.NLS2.module.backtester import run_backtest_bt


def date_to_ms(dstr: str) -> int:
    """
    'YYYY-MM-DD' 형식의 날짜 문자열을 밀리초 단위 타임스탬프로 변환합니다.
    """
    dt = datetime.strptime(dstr, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


def clear_folder(folder_path: str):
    """
    지정된 폴더 내의 모든 파일 및 폴더를 삭제합니다.
    """
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"[ERROR] Failed to delete {file_path}: {e}")


def main():
    # 사용자 설정
    symbol = "BTCUSDT"
    intervals = ["1h"]  # 사용자가 원하는 시간프레임 배열
    csv_dir = "data_fetcher"

    # 사용자가 지정한 백테스트 기간
    start_date = "2019-10-01"
    end_date = "2020-12-31"

    start_ts = date_to_ms(start_date)
    end_ts = date_to_ms(end_date)

    # 백테스트 기본 파라미터
    start_cash = 100000.0
    commission = 0.002

    # JSON 설정 파일 경로 (보조지표 조합 정의)
    config_path = os.path.join("module", "config", "nls2_opt_config.json")

    # 결과 저장 폴더 확인 및 생성 후 내용 삭제
    result_dir = "../../results"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    else:
        clear_folder(result_dir)

    for interval in intervals:
        print(f"\n[INFO] Processing interval: {interval}")
        csv_file = f"{symbol}_{interval}.csv"
        csv_path = os.path.join(csv_dir, csv_file)

        if not os.path.exists(csv_path):
            print(f"[INFO] CSV file {csv_path} not found. Updating CSV data_fetcher...")
            update_csv(symbol, interval, start_ts, end_ts, csv_path)

        df = pd.read_csv(csv_path)
        df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)

        out_csv = os.path.join(result_dir, f"all_combos_results_{interval}.csv")

        # 모든 보조지표 조합에 대해 백테스트 실행 (병렬 처리)
        df_results = run_all_combinations(
            df=df,
            start_cash=start_cash,
            commission=commission,
            out_csv=out_csv,
            config_path=config_path,
            strategy_cls=NLS2Combined
        )
        # 결과 DataFrame에 백테스트 기간, interval, symbol 정보 추가
        df_results["data_start"] = start_date
        df_results["data_end"] = end_date
        df_results["interval"] = interval
        df_results["symbol"] = symbol

        # 바이앤홀드 전략 결과 실행 및 행 생성
        print(f"[INFO] Running Buy and Hold test for interval: {interval}")
        try:
            bh_result = run_backtest_bt(
                df,
                strategy_cls=BuyAndHoldStrategy,
                strategy_params={},
                start_cash=start_cash,
                commission=commission,
                plot=False,
                use_progress=False
            )
        except Exception as e:
            print(f"[ERROR] Buy and Hold test failed: {e}")
            bh_result = None

        if bh_result is not None:
            bh_row = {
                "data_start": start_date,
                "data_end": end_date,
                "total_trades": bh_result.total_trades,
                "sharpe": bh_result.sharpe,
                "max_drawdown_pct": bh_result.max_drawdown_pct,
                "final_value": bh_result.final_value,
                "net_profit": bh_result.net_profit,
                "won_trades": bh_result.won_trades,
                "lost_trades": bh_result.lost_trades,
                "strike_rate": bh_result.strike_rate,
                "annual_return_pct": bh_result.annual_return_pct,
                "sqn": bh_result.sqn,
                "profit_factor": bh_result.profit_factor,
                "avg_win": bh_result.avg_win,
                "avg_loss": bh_result.avg_loss,
                "win_streak": bh_result.win_streak,
                "lose_streak": bh_result.lose_streak,
                "pnl_net": bh_result.pnl_net,
                "pnl_gross": bh_result.pnl_gross,
                "interval": f"{interval}(BnH)",
                "symbol": symbol,
                "strategy_params": "BuyAndHold"
            }
            df_bh = pd.DataFrame([bh_row])
            df_final = pd.concat([df_bh, df_results], ignore_index=True)
        else:
            df_final = df_results

        columns_order = ["data_start", "data_end", "total_trades", "sharpe", "max_drawdown_pct",
                         "final_value", "net_profit", "won_trades", "lost_trades", "strike_rate",
                         "annual_return_pct", "sqn", "profit_factor", "avg_win", "avg_loss",
                         "win_streak", "lose_streak", "pnl_net", "pnl_gross", "interval", "symbol", "strategy_params"]
        df_final = df_final.reindex(columns=columns_order)
        df_final.to_csv(out_csv, index=False, encoding='utf-8-sig')
        print(f"[INFO] Results with Buy and Hold added saved to {out_csv}")


if __name__ == "__main__":
    main()
