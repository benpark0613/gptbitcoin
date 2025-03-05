from datetime import datetime
import os

import pandas as pd
import pyupbit
from dotenv import load_dotenv

from backup.NLS2.module import clear_folder


def main():
    load_dotenv()
    access = os.getenv("UPBIT_ACCESS_KEY")
    secret = os.getenv("UPBIT_SECRET_KEY")
    upbit = pyupbit.Upbit(access, secret)

    ticker = "KRW-BTC"
    rss_url = "https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US"
    sub_indicators = ["rsi", "macd", "bollinger", "ema_50", "ema_200"]

    # 업비트 관련 데이터
    my_balances = upbit.get_balances()  # 잔고 정보
    open_orders = upbit.get_order(ticker, "wait")  # 진행 중인 주문
    done_orders = upbit.get_order(ticker, "done")  # 완료된 주문

    # orderbook = pyupbit.get_orderbook(ticker)  # 호가 정보(주문장)

    # # 뉴스 및 지표 데이터
    # # (A) 구글 뉴스 최신 10개 기사
    # full_news_list = get_latest_10_articles("Bitcoin")
    # # (B) RSS로부터 최근 10개 뉴스
    # rss_news_list = get_top_10_recent_news("https://news.google.com/rss/search?q=bitcoin&hl=en&gl=US")
    #
    # fear_and_greed_index = get_upbit_fear_greed_index()

    # 차트(OHLCV) 정보
    # df_1h = pyupbit.get_ohlcv(ticker, "minute60", 700)
    # df_4h = pyupbit.get_ohlcv(ticker, "minute240", 400)
    # df_1d = pyupbit.get_ohlcv(ticker, "day", 200)
    # chart_4h = add_technical_indicators(df_4h, sub_indicators)
    # chart_4h = chart_4h.sort_index(ascending=False)
    # chart_1d = add_technical_indicators(df_1d, sub_indicators)
    # chart_1d = chart_1d.sort_index(ascending=False)

    # CSV 저장 폴더 설정
    current_time = datetime.now().strftime("%Y%m%d%H%M")  # 수정
    report_dir = "report"
    if os.path.exists(report_dir):
        clear_folder(report_dir)
    else:
        os.makedirs(report_dir)

    # 1) Balances
    file_path_my_balances = os.path.join(report_dir, f"{current_time}_my_balances.csv")
    balances_df = pd.DataFrame(my_balances)
    balances_df.to_csv(file_path_my_balances, index=False)
    print(f"my_balances saved to {file_path_my_balances}")

    # file_path_5m = os.path.join(report_dir, f"{current_time}_5m.csv")
    # df_5m.to_csv(file_path_5m, index=True, index_label="timestamp")
    # print(f"chart_5m saved to {file_path_5m}")
    #
    # file_path_15m = os.path.join(report_dir, f"{current_time}_15m.csv")
    # df_15m.to_csv(file_path_15m, index=True, index_label="timestamp")
    # print(f"chart_15m saved to {file_path_15m}")

    # file_path_1h = os.path.join(report_dir, f"{current_time}_1h.csv")
    # df_1h.to_csv(file_path_1h, index=True, index_label="timestamp")
    # print(f"chart_1h saved to {file_path_1h}")
    #
    # file_path_4h = os.path.join(report_dir, f"{current_time}_4h.csv")
    # df_4h.to_csv(file_path_4h, index=True, index_label="timestamp")
    # print(f"chart_4h saved to {file_path_4h}")
    #
    # file_path_1d = os.path.join(report_dir, f"{current_time}_1d.csv")
    # df_1d.to_csv(file_path_1d, index=True, index_label="timestamp")
    # print(f"chart_1d saved to {file_path_1d}")

    # 4) 진행 중인 주문(open_orders)
    file_path_open_orders = os.path.join(report_dir, f"{current_time}_open_orders.csv")
    if open_orders:
        pd.DataFrame(open_orders).to_csv(file_path_open_orders, index=False)
    else:
        pd.DataFrame().to_csv(file_path_open_orders, index=False)  # 빈 DataFrame
    print(f"open_orders saved to {file_path_open_orders}")

    # 5) 완료된 주문(done_orders)
    file_path_done_orders = os.path.join(report_dir, f"{current_time}_done_orders.csv")
    if done_orders:
        pd.DataFrame(done_orders).to_csv(file_path_done_orders, index=False)
    else:
        pd.DataFrame().to_csv(file_path_done_orders, index=False)
    print(f"done_orders saved to {file_path_done_orders}")

    # # 6) 호가 정보(orderbook)
    # file_path_orderbook = os.path.join(report_dir, f"{current_time}_orderbook.csv")
    # if orderbook:
    #     orderbook_df = pd.json_normalize(orderbook, record_path='orderbook_units',
    #                                      meta=['market', 'timestamp', 'total_bid_size', 'total_ask_size'])
    #
    #     if 'timestamp' in orderbook_df.columns:
    #         orderbook_df['timestamp2'] = orderbook_df['timestamp'].apply(
    #             lambda x: datetime.fromtimestamp(int(x) / 1000, tz=timezone.utc) + timedelta(hours=9)
    #         ).apply(
    #             lambda x: x.strftime('%Y-%m-%d %H:%M:%S KST')
    #         )
    #
    #     orderbook_df.to_csv(file_path_orderbook, index=False)
    # else:
    #     pd.DataFrame().to_csv(file_path_orderbook, index=False)
    # print(f"orderbook saved to {file_path_orderbook}")

    # 7) 뉴스 정보(news_list)
    # news_file = os.path.join(report_dir, f"{current_time}_news_list.csv")
    # with open(news_file, 'w', newline='', encoding='utf-8') as f:
    #     # full_news_list가 비어있지 않으면
    #     if full_news_list:
    #         fieldnames = full_news_list[0].keys()
    #         writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    #         writer.writeheader()
    #         writer.writerows(full_news_list)
    #     # full_news_list가 비어있고, rss_news_list가 있으면
    #     elif rss_news_list:
    #         fieldnames = rss_news_list[0].keys()
    #         writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    #         writer.writeheader()
    #         writer.writerows(rss_news_list)
    #     # 둘 다 없으면
    #     else:
    #         writer = csv.writer(f)
    #         writer.writerow(["No Data"])




    # # 8) 공포탐욕지수(fear_and_greed_index)
    # file_path_fng = os.path.join(report_dir, f"{current_time}_fear_and_greed.csv")
    # if fear_and_greed_index:
    #     pd.DataFrame([fear_and_greed_index]).to_csv(file_path_fng, index=False)
    # else:
    #     pd.DataFrame().to_csv(file_path_fng, index=False)
    # print(f"fear_and_greed_index saved to {file_path_fng}")


if __name__ == "__main__":
    main()