import logging
import time
import random
import re
import csv
import json
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ✅ 상대 시간 변환 함수 ('3시간 전' → datetime 변환)
def parse_relative_time_kor(time_str):
    pattern = re.compile(r"(\d+)(일|시간|분)\s*전")
    match = pattern.match(time_str.strip())
    if not match:
        return None
    value, unit = match.groups()
    value = int(value)

    now = datetime.now()
    if unit == '일':
        return now - timedelta(days=value)
    elif unit == '시간':
        return now - timedelta(hours=value)
    elif unit == '분':
        return now - timedelta(minutes=value)
    return None


# ✅ 랜덤 User-Agent 목록 (탐지 방지)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.77 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
]


# ✅ 구글 뉴스 URL 생성 함수
def generate_url(query, start=0, date_filter="w"):
    base_url = "https://www.google.com/search"
    params = {
        "q": query,
        "gl": "us",
        "tbm": "nws",
        "start": start
    }
    if date_filter:
        params["tbs"] = f"qdr:{date_filter}"  # 최근 1주일 뉴스 필터링
    query_string = "&".join([f"{key}={value}" for key, value in params.items()])
    return f"{base_url}?{query_string}"


# ✅ WebDriver 실행 함수 (탐지 방지 옵션 적용)
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    return webdriver.Chrome(options=chrome_options)


# ✅ 구글 뉴스 스크래핑 함수 (각 키워드별 최대 5개 기사 수집)
def scrape_news(queries, max_articles_per_query=5, date_filter="w"):
    driver = get_driver()
    all_results = []

    try:
        for query in queries:
            collected_results = []
            start = 0

            while len(collected_results) < max_articles_per_query:
                url = generate_url(query, start, date_filter)
                driver.get(url)

                # ✅ 스크롤 동작 추가 (봇 탐지 방지)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3, 7))  # 랜덤 대기 시간

                articles = driver.find_elements(By.CSS_SELECTOR, "div.SoaBEf")
                if not articles:
                    break

                for el in articles:
                    try:
                        title = el.find_element(By.CSS_SELECTOR, "div.MBeuO").text.strip()
                        snippet = el.find_element(By.CSS_SELECTOR, ".GI74Re").text.strip()
                        date_str = el.find_element(By.CSS_SELECTOR, ".LfVVr").text.strip()
                        source = el.find_element(By.CSS_SELECTOR, ".NUnG9d span").text.strip()
                        parsed_time = parse_relative_time_kor(date_str)

                        collected_results.append({
                            "keyword": query,
                            "title": title,
                            "snippet": snippet,
                            "date": date_str,
                            "source": source,
                            "parsed_date": parsed_time
                        })
                    except Exception as e:
                        logger.warning(f"기사 파싱 중 오류 발생: {e}")

                start += 10

            # ✅ 최신순 정렬 후 키워드별 상위 max_articles_per_query개만 유지
            collected_results.sort(key=lambda x: x["parsed_date"] or datetime.min, reverse=True)
            all_results.extend(collected_results[:max_articles_per_query])

    finally:
        driver.quit()

    return all_results


# ✅ 테스트 실행 함수 (다중 키워드 뉴스 수집 후 CSV 저장)
def test_news_scraping():
    queries = [
        "Bitcoin price prediction", "Bitcoin volatility", "Bitcoin whale activity",
        "Bitcoin institutional adoption", "SEC Bitcoin ETF decision", "Bitcoin regulation",
        "Bitcoin mining difficulty", "Bitcoin network congestion", "US inflation CPI data",
        "Federal Reserve interest rates"
    ]

    logger.info(f"[TEST] {len(queries)}개 키워드에 대해 뉴스 수집 시작.")

    news_data = scrape_news(queries, max_articles_per_query=5, date_filter="w")
    logger.info(f"총 수집된 뉴스 기사: {len(news_data)}개")

    # ✅ CSV 저장
    csv_filename = "bitcoin_news.csv"
    try:
        with open(csv_filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["keyword", "title", "snippet", "date", "source", "parsed_date"])
            for article in news_data:
                writer.writerow([
                    article["keyword"],
                    article["title"],
                    article["snippet"],
                    article["date"],
                    article["source"],
                    str(article["parsed_date"]) if article["parsed_date"] else ""
                ])
        logger.info(f"뉴스 데이터 CSV 저장 완료: {csv_filename}")
    except Exception as e:
        logger.error(f"CSV 저장 중 에러: {e}")

    # ✅ 콘솔 출력
    for idx, article in enumerate(news_data, start=1):
        print(f"\n[{idx}] {article['keyword']} - {article['title']} ({article['date']})")
        print(json.dumps(article, indent=4, ensure_ascii=False, default=str))


# ✅ 실행
if __name__ == "__main__":
    test_news_scraping()
