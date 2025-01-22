import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import pyupbit
import requests
import schedule
import ta
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel

# .env 파일에 저장된 환경 변수를 불러오기 (API 키 등)
load_dotenv()

# 로깅 설정 - 로그 레벨을 INFO로 설정하여 중요 정보 출력
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Upbit 객체 생성
access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")
upbit = pyupbit.Upbit(access, secret)


# OpenAI 구조화된 출력 체크용 클래스
class TradingDecision(BaseModel):
    decision: str
    percentage: int
    reason: str


# SQLite 데이터베이스 초기화 함수 - 거래 내역을 저장할 테이블을 생성
def init_db():
    conn = sqlite3.connect("../../bitcoin_trades.db")
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS trades
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp TEXT,
                  decision TEXT,
                  percentage INTEGER,
                  reason TEXT,
                  btc_balance REAL,
                  krw_balance REAL,
                  btc_avg_buy_price REAL,
                  btc_krw_price REAL,
                  reflection TEXT)"""
    )
    conn.commit()
    return conn


# 거래 기록을 DB에 저장하는 함수
def log_trade(
        conn,
        decision,
        percentage,
        reason,
        btc_balance,
        krw_balance,
        btc_avg_buy_price,
        btc_krw_price,
        reflection="",
):
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute(
        """INSERT INTO trades 
                 (timestamp, decision, percentage, reason, btc_balance, krw_balance, btc_avg_buy_price, btc_krw_price, reflection) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            timestamp,
            decision,
            percentage,
            reason,
            btc_balance,
            krw_balance,
            btc_avg_buy_price,
            btc_krw_price,
            reflection,
        ),
    )
    conn.commit()


# 최근 투자 기록 조회
def get_recent_trades(conn, days=7):
    c = conn.cursor()
    seven_days_ago = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute(
        "SELECT * FROM trades WHERE timestamp > ? ORDER BY timestamp DESC",
        (seven_days_ago,),
    )
    columns = [column[0] for column in c.description]
    return pd.DataFrame.from_records(data=c.fetchall(), columns=columns)


# 최근 투자 기록을 기반으로 퍼포먼스 계산 (초기 잔고 대비 최종 잔고)
def calculate_performance(trades_df):
    if trades_df.empty:
        return 0  # 기록이 없을 경우 0%로 설정
    # 초기 잔고 계산 (KRW + BTC * 현재 가격)
    initial_balance = (
            trades_df.iloc[-1]["krw_balance"]
            + trades_df.iloc[-1]["btc_balance"] * trades_df.iloc[-1]["btc_krw_price"]
    )
    # 최종 잔고 계산
    final_balance = (
            trades_df.iloc[0]["krw_balance"]
            + trades_df.iloc[0]["btc_balance"] * trades_df.iloc[0]["btc_krw_price"]
    )
    return (final_balance - initial_balance) / initial_balance * 100


# AI 모델을 사용하여 최근 투자 기록과 시장 데이터를 기반으로 분석 및 반성을 생성하는 함수
def generate_reflection(trades_df, current_market_data):
    performance = calculate_performance(trades_df)  # 투자 퍼포먼스 계산

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    if not client.api_key:
        logger.error("OpenAI API key is missing or invalid.")
        return None

    # OpenAI API 호출로 AI의 반성 일기 및 개선 사항 생성 요청
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "You are an AI trading assistant tasked with analyzing recent trading performance and current market conditions to generate insights and improvements for future trading decisions.",
            },
            {
                "role": "user",
                "content": f"""
                Recent trading data:
                {trades_df.to_json(orient='records')}

                Current market data:
                {current_market_data}

                Overall performance in the last 7 days: {performance:.2f}%

                Please analyze this data and provide:
                1. A brief reflection on the recent trading decisions
                2. Insights on what worked well and what didn't
                3. Suggestions for improvement in future trading decisions
                4. Any patterns or trends you notice in the market data

                Limit your response to 250 words or less.
                """,
            },
        ],
    )

    try:
        response_content = response.choices[0].message.content
        return response_content
    except (IndexError, AttributeError) as e:
        logger.error(f"Error extracting response content: {e}")
        return None


def add_indicators(df, interval):
    if interval == "minute15":
        # 15분봉: 단기 변동성 분석
        bb = ta.volatility.BollingerBands(close=df["close"], window=10, window_dev=2.0)
        df["bb_bbm"] = bb.bollinger_mavg()  # 중심선
        df["bb_bbh"] = bb.bollinger_hband()  # 상한선
        df["bb_bbl"] = bb.bollinger_lband()  # 하한선
        df["rsi"] = ta.momentum.RSIIndicator(close=df["close"], window=7).rsi()

    elif interval == "minute60":
        # 1시간봉: 중기 추세 분석
        bb = ta.volatility.BollingerBands(close=df["close"], window=20, window_dev=2.0)
        df["bb_bbm"] = bb.bollinger_mavg()
        df["bb_bbh"] = bb.bollinger_hband()
        df["bb_bbl"] = bb.bollinger_lband()
        df["rsi"] = ta.momentum.RSIIndicator(close=df["close"], window=14).rsi()

    elif interval == "minute240":
        # 4시간봉: 장기 추세 분석
        df["rsi"] = ta.momentum.RSIIndicator(close=df["close"], window=14).rsi()

    return df


# 공포 탐욕 지수 조회
def get_fear_and_greed_index():
    url = "https://api.alternative.me/fng/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["data"][0]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Fear and Greed Index: {e}")
        return None


# 뉴스 데이터 가져오기
def get_bitcoin_news():
    current_hour = datetime.now().hour  # 현재 시간의 시(hour)를 가져옵니다.
    # 8시, 16시, 24시에만 뉴스 데이터를 가져옵니다.
    if current_hour not in [8, 16, 0]:  # 0은 자정(24시)
        logger.info("Not the right time to fetch news, skipping...")
        return []  # 뉴스 가져오지 않음

    serpapi_key = os.getenv("SERPAPI_API_KEY")
    if not serpapi_key:
        logger.error("SERPAPI API key is missing.")
        return []  # 빈 리스트 반환
    url = "https://serpapi.com/search.json"
    params = {"engine": "google_news", "q": "bitcoin OR btc", "api_key": serpapi_key}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        news_results = data.get("news_results", [])
        headlines = []
        for item in news_results:
            headlines.append(
                {"title": item.get("title", ""), "date": item.get("date", "")}
            )

        return headlines[:5]
    except requests.RequestException as e:
        logger.error(f"Error fetching news: {e}")
        return []


### 메인 AI 트레이딩 로직
def ai_trading():
    global upbit
    ### 데이터 가져오기
    # 1. 현재 투자 상태 조회
    all_balances = upbit.get_balances()
    filtered_balances = [
        balance for balance in all_balances if balance["currency"] in ["BTC", "KRW"]
    ]

    # 2. 오더북(호가 데이터) 조회
    orderbook = pyupbit.get_orderbook("KRW-BTC")

    # 3. 차트 데이터 조회 및 보조지표 추가
    # 15분봉 데이터 가져오기
    df_15min = pyupbit.get_ohlcv("KRW-BTC", interval="minute15", count=60)
    df_15min = add_indicators(df_15min.dropna(), interval="minute15")

    # 1시간봉 데이터 가져오기
    df_hourly = pyupbit.get_ohlcv("KRW-BTC", interval="minute60", count=40)
    df_hourly = add_indicators(df_hourly.dropna(), interval="minute60")

    # 4시간봉 데이터 가져오기
    df_4hour = pyupbit.get_ohlcv("KRW-BTC", interval="minute240", count=20)
    df_4hour = add_indicators(df_4hour.dropna(), interval="minute240")

    # 4. 공포 탐욕 지수 가져오기
    fear_greed_index = get_fear_and_greed_index()

    # 5. 뉴스 헤드라인 가져오기
    news_headlines = get_bitcoin_news()

    ### AI에게 데이터 제공하고 판단 받기
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    if not client.api_key:
        logger.error("OpenAI API key is missing or invalid.")
        return None
    try:
        # 데이터베이스 연결
        with sqlite3.connect("../../bitcoin_trades.db") as conn:
            # 최근 거래 내역 가져오기
            recent_trades = get_recent_trades(conn)

            # 현재 시장 데이터 수집 (기존 코드에서 가져온 데이터 사용)
            current_market_data = {
                "fear_greed_index": fear_greed_index,
                "news_headlines": news_headlines,
                "orderbook": orderbook,
                "15min_ohlcv": df_15min.to_dict(),
                "hourly_ohlcv": df_hourly.to_dict(),
                "4hour_ohlcv": df_4hour.to_dict(),
            }

            # 반성 및 개선 내용 생성
            reflection = generate_reflection(recent_trades, current_market_data)

            # API 호출 간 대기 시간 추가
            time.sleep(30)  # 10초 대기 (필요에 따라 증가 가능)

            # AI 모델에 반성 내용 제공
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": f"""
                        You are an expert in Bitcoin investing. This analysis is performed every 4 hours. Analyze the provided data and determine whether to buy, sell, or hold at the current moment. 

                        Consider the following in your analysis:
                            - The strategy focuses solely on spot trading and excludes leveraged or futures trading
                            - The analysis should emphasize chart patterns, candlestick analysis, and trading volume, while using technical indicators like MACD, RSI, or Bollinger Bands sparingly and only when necessary to support the analysis.
                            - **Stop-loss** should be set around 3-5% loss, and you should act quickly if losses are anticipated.
                            - **Take-profit** should be executed when the target price or market trend changes. Realize profits by cashing out a portion of the gains to manage risk.
                            - Maintain **mental discipline** by avoiding emotional decisions and continue gaining experience despite any losses. Focus on **consistent win rates** especially when starting with limited capital.
                            - Operate with **position management** using **partial purchases** to spread risk, especially near key support or resistance levels. Sell portions of the position when the price movement stops to secure profits.
                            - Adapt the strategy to changing market conditions. Avoid sticking to a particular market direction (up or down). In overbought or oversold conditions, consider **reversal signals** as potential trading opportunities.

                        The following materials will be provided for your analysis:
                            - Technical indicators and market data
                            - Recent news headlines and their potential impact on Bitcoin price
                            - The Fear and Greed Index and its implications
                            - Overall market sentiment
                            - Recent trading performance and reflection

                        Recent trading reflection:
                        {reflection}

                        Based on your analysis, make a decision and provide your reasoning.

                        Response format:
                        1. Decision (buy, sell, or hold)
                        2. If the decision is 'buy', provide a percentage (1-100) of available KRW to use for buying.
                        If the decision is 'sell', provide a percentage (1-100) of held BTC to sell.
                        If the decision is 'hold', set the percentage to 0.
                        3. Reason for your decision

                        Ensure that the percentage is an integer between 1 and 100 for buy/sell decisions, and exactly 0 for hold decisions.
                        Your percentage should reflect the strength of your conviction in the decision based on the analyzed data.""",
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"""Current investment status: {json.dumps(filtered_balances)}
                                    Orderbook: {json.dumps(orderbook)}                                
                                    15-minute OHLCV with indicators (last 60 intervals): {df_15min.to_json()}
                                    Hourly OHLCV with indicators (last 40 intervals): {df_hourly.to_json()}
                                    4-hour OHLCV with indicators (last 20 intervals): {df_4hour.to_json()}
                                    Recent news headlines: {json.dumps(news_headlines)}
                                    Fear and Greed Index: {json.dumps(fear_greed_index)}""",
                            },
                        ],
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "trading_decision",
                        "strict": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "decision": {
                                    "type": "string",
                                    "enum": ["buy", "sell", "hold"],
                                },
                                "percentage": {"type": "integer"},
                                "reason": {"type": "string"},
                            },
                            "required": ["decision", "percentage", "reason"],
                            "additionalProperties": False,
                        },
                    },
                },
            )

            # Pydantic을 사용하여 AI의 트레이딩 결정 구조를 정의
            try:
                result = TradingDecision.model_validate_json(
                    response.choices[0].message.content
                )
            except Exception as e:
                logger.error(f"Error parsing AI response: {e}")
                return

            logger.info(f"AI Decision: {result.decision.upper()}")
            logger.info(f"Decision Reason: {result.reason}")

            order_executed = False

            if result.decision == "buy":
                my_krw = upbit.get_balance("KRW")
                if my_krw is None:
                    logger.error("Failed to retrieve KRW balance.")
                    return
                buy_amount = my_krw * (result.percentage / 100) * 0.9995  # 수수료 고려
                if buy_amount > 5000:
                    logger.info(
                        f"Buy Order Executed: {result.percentage}% of available KRW"
                    )
                    try:
                        order = upbit.buy_market_order("KRW-BTC", buy_amount)
                        if order:
                            logger.info(f"Buy order executed successfully: {order}")
                            order_executed = True
                        else:
                            logger.error("Buy order failed.")
                    except Exception as e:
                        logger.error(f"Error executing buy order: {e}")
                else:
                    logger.warning(
                        "Buy Order Failed: Insufficient KRW (less than 5000 KRW)"
                    )
            elif result.decision == "sell":
                my_btc = upbit.get_balance("KRW-BTC")
                if my_btc is None:
                    logger.error("Failed to retrieve KRW balance.")
                    return
                sell_amount = my_btc * (result.percentage / 100)
                current_price = pyupbit.get_current_price("KRW-BTC")
                if sell_amount * current_price > 5000:
                    logger.info(
                        f"Sell Order Executed: {result.percentage}% of held BTC"
                    )
                    try:
                        order = upbit.sell_market_order("KRW-BTC", sell_amount)
                        if order:
                            order_executed = True
                        else:
                            logger.error("Buy order failed.")
                    except Exception as e:
                        logger.error(f"Error executing sell order: {e}")
                else:
                    logger.warning(
                        "Sell Order Failed: Insufficient BTC (less than 5000 KRW worth)"
                    )

            # 거래 실행 여부와 관계없이 현재 잔고 조회
            time.sleep(2)  # API 호출 제한을 고려하여 잠시 대기
            balances = upbit.get_balances()
            btc_balance = next(
                (
                    float(balance["balance"])
                    for balance in balances
                    if balance["currency"] == "BTC"
                ),
                0,
            )
            krw_balance = next(
                (
                    float(balance["balance"])
                    for balance in balances
                    if balance["currency"] == "KRW"
                ),
                0,
            )
            btc_avg_buy_price = next(
                (
                    float(balance["avg_buy_price"])
                    for balance in balances
                    if balance["currency"] == "BTC"
                ),
                0,
            )
            current_btc_price = pyupbit.get_current_price("KRW-BTC")

            # 거래 기록을 DB에 저장하기
            log_trade(
                conn,
                result.decision,
                result.percentage if order_executed else 0,
                result.reason,
                btc_balance,
                krw_balance,
                btc_avg_buy_price,
                current_btc_price,
                reflection,
            )
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        return


if __name__ == "__main__":
    # 데이터베이스 초기화
    init_db()

    # 중복 실행 방지를 위한 변수
    trading_in_progress = False


    # 트레이딩 작업을 수행하는 함수
    def job():
        global trading_in_progress
        if trading_in_progress:
            logger.warning("Trading job is already in progress, skipping this run.")
            return
        try:
            trading_in_progress = True
            ai_trading()
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            trading_in_progress = False


    ## 테스트용 바로 실행
    job()

    ## 매일 특정 시간에 실행
    # schedule.every().day.at("04:00").do(job)
    # schedule.every().day.at("08:00").do(job)
    # schedule.every().day.at("12:00").do(job)
    # schedule.every().day.at("16:00").do(job)
    # schedule.every().day.at("20:00").do(job)
    # schedule.every().day.at("00:00").do(job)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
